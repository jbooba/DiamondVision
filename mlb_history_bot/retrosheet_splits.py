from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Iterator
from zipfile import ZipFile

import pandas as pd

from .config import Settings
from .metrics import MetricCatalog
from .models import EvidenceSnippet
from .query_utils import extract_first_n_games, extract_referenced_season, extract_season_span
from .storage import (
    clear_retrosheet_team_split_games,
    get_connection,
    initialize_database,
    set_metadata_value,
    table_exists,
    upsert_retrosheet_team_split_games,
)


BEST_HINTS = {"best", "leader", "leaders"}
WORST_HINTS = {"worst"}
HIGH_HINTS = {"highest", "most", "top"}
LOW_HINTS = {"lowest", "least", "bottom"}

PLAY_USECOLS = [
    "gid",
    "batteam",
    "walk",
    "iw",
    "hbp",
    "sf",
    "k",
    "pa",
    "ab",
    "single",
    "double",
    "triple",
    "hr",
    "rbi",
    "br1_pre",
    "br2_pre",
    "br3_pre",
    "date",
    "gametype",
]


@dataclass(slots=True)
class SituationalSplitSpec:
    key: str
    label: str
    aliases: tuple[str, ...]
    matcher: Callable[[pd.DataFrame], pd.Series]


@dataclass(slots=True)
class SplitMetricSpec:
    metric_name: str
    aliases: tuple[str, ...]
    label: str
    higher_is_better: bool
    sql_expression: str
    decimal_places: int


@dataclass(slots=True)
class TeamSplitHistoryQuery:
    split: SituationalSplitSpec
    metric: SplitMetricSpec
    first_n_games: int
    sort_desc: bool
    descriptor: str
    start_season: int | None
    end_season: int | None
    scope_label: str


TRACKED_SPLITS: tuple[SituationalSplitSpec, ...] = (
    SituationalSplitSpec(
        key="risp",
        label="with RISP",
        aliases=(
            " with risp ",
            " runners in scoring position ",
            " runner in scoring position ",
            " with runners in scoring position ",
            " with runner in scoring position ",
        ),
        matcher=lambda frame: _has_runner(frame["br2_pre"]) | _has_runner(frame["br3_pre"]),
    ),
    SituationalSplitSpec(
        key="men_on",
        label="with runners on",
        aliases=(" with runners on ", " with men on ", " runners on base ", " men on base "),
        matcher=lambda frame: _has_runner(frame["br1_pre"]) | _has_runner(frame["br2_pre"]) | _has_runner(frame["br3_pre"]),
    ),
    SituationalSplitSpec(
        key="bases_empty",
        label="with the bases empty",
        aliases=(" with bases empty ", " bases empty "),
        matcher=lambda frame: ~(_has_runner(frame["br1_pre"]) | _has_runner(frame["br2_pre"]) | _has_runner(frame["br3_pre"])),
    ),
    SituationalSplitSpec(
        key="bases_loaded",
        label="with the bases loaded",
        aliases=(" with bases loaded ", " bases loaded "),
        matcher=lambda frame: _has_runner(frame["br1_pre"]) & _has_runner(frame["br2_pre"]) & _has_runner(frame["br3_pre"]),
    ),
)

SUPPORTED_SPLIT_METRICS: tuple[SplitMetricSpec, ...] = (
    SplitMetricSpec(
        metric_name="BA",
        aliases=("batting average", " ba ", " avg ", "average"),
        label="batting average",
        higher_is_better=True,
        sql_expression="CAST(sum_h AS REAL) / NULLIF(sum_ab, 0)",
        decimal_places=3,
    ),
    SplitMetricSpec(
        metric_name="OBP",
        aliases=("obp", "on-base percentage"),
        label="on-base percentage",
        higher_is_better=True,
        sql_expression="CAST(sum_h + sum_bb + sum_hbp AS REAL) / NULLIF(sum_ab + sum_bb + sum_hbp + sum_sf, 0)",
        decimal_places=3,
    ),
    SplitMetricSpec(
        metric_name="SLG",
        aliases=("slg", "slugging percentage"),
        label="slugging percentage",
        higher_is_better=True,
        sql_expression="CAST((sum_h - sum_2b - sum_3b - sum_hr) + (2 * sum_2b) + (3 * sum_3b) + (4 * sum_hr) AS REAL) / NULLIF(sum_ab, 0)",
        decimal_places=3,
    ),
    SplitMetricSpec(
        metric_name="OPS",
        aliases=("ops", "on-base plus slugging"),
        label="OPS",
        higher_is_better=True,
        sql_expression="""
            (
                CAST(sum_h + sum_bb + sum_hbp AS REAL) / NULLIF(sum_ab + sum_bb + sum_hbp + sum_sf, 0)
            ) + (
                CAST((sum_h - sum_2b - sum_3b - sum_hr) + (2 * sum_2b) + (3 * sum_3b) + (4 * sum_hr) AS REAL) / NULLIF(sum_ab, 0)
            )
        """,
        decimal_places=3,
    ),
)


class RetrosheetSituationalResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.catalog = MetricCatalog.load(settings.project_root)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_team_split_history_query(question, self.catalog)
        if query is None or not table_exists(connection, "retrosheet_team_split_games"):
            return None
        rows = fetch_team_split_window_rankings(connection, query)
        if not rows:
            return None
        return EvidenceSnippet(
            source="Retrosheet Situational Splits",
            title=f"{query.scope_label} {query.metric.label} {query.split.label} through first {query.first_n_games} games",
            citation="Retrosheet plays.csv aggregated to team-game situational split rows",
            summary=build_team_split_window_summary(query, rows),
            payload={
                "analysis_type": "team_split_window_ranking",
                "metric": query.metric.metric_name,
                "metric_label": query.metric.label,
                "split_key": query.split.key,
                "split_label": query.split.label,
                "first_n_games": query.first_n_games,
                "start_season": query.start_season,
                "end_season": query.end_season,
                "scope_label": query.scope_label,
                "descriptor": query.descriptor,
                "leaders": rows,
            },
        )


def parse_team_split_history_query(question: str, catalog: MetricCatalog) -> TeamSplitHistoryQuery | None:
    lowered = f" {question.lower()} "
    if "team" not in lowered:
        return None
    first_n_games = extract_first_n_games(question)
    if first_n_games is None:
        return None
    split = find_situational_split(lowered)
    if split is None:
        return None
    wants_high = any(hint in lowered for hint in HIGH_HINTS)
    wants_low = any(hint in lowered for hint in LOW_HINTS)
    wants_best = any(hint in lowered for hint in BEST_HINTS)
    wants_worst = any(hint in lowered for hint in WORST_HINTS)
    if not wants_high and not wants_low and not wants_best and not wants_worst:
        return None
    metric = find_split_metric(lowered, catalog)
    if metric is None:
        return None
    if wants_high:
        sort_desc = True
        descriptor = "highest"
    elif wants_low:
        sort_desc = False
        descriptor = "lowest"
    elif wants_worst:
        sort_desc = not metric.higher_is_better
        descriptor = "worst"
    else:
        sort_desc = metric.higher_is_better
        descriptor = "best"
    current_season = date.today().year
    span = extract_season_span(question, current_season)
    referenced_season = extract_referenced_season(question, current_season)
    if span is not None:
        start_season = span.start_season
        end_season = span.end_season
        scope_label = span.label
    elif referenced_season is not None:
        start_season = referenced_season
        end_season = referenced_season
        scope_label = str(referenced_season)
    else:
        start_season = None
        end_season = None
        scope_label = "Retrosheet history"
    return TeamSplitHistoryQuery(
        split=split,
        metric=metric,
        first_n_games=first_n_games,
        sort_desc=sort_desc,
        descriptor=descriptor,
        start_season=start_season,
        end_season=end_season,
        scope_label=scope_label,
    )


def find_situational_split(lowered_question: str) -> SituationalSplitSpec | None:
    best_match: tuple[int, SituationalSplitSpec] | None = None
    for split in TRACKED_SPLITS:
        score = 0
        for alias in split.aliases:
            if alias in lowered_question:
                score = max(score, len(alias.strip()))
        if score and (best_match is None or score > best_match[0]):
            best_match = (score, split)
    return best_match[1] if best_match else None


def find_split_metric(lowered_question: str, catalog: MetricCatalog) -> SplitMetricSpec | None:
    exact_metric_names = {metric.name for metric in catalog.search(lowered_question, limit=5)}
    best_match: tuple[int, SplitMetricSpec] | None = None
    for metric in SUPPORTED_SPLIT_METRICS:
        score = 0
        if metric.metric_name in exact_metric_names:
            score += 20
        for alias in metric.aliases:
            alias_text = alias if alias.startswith(" ") else f" {alias} "
            if alias_text in lowered_question:
                score = max(score, len(alias.strip()))
        if score and (best_match is None or score > best_match[0]):
            best_match = (score, metric)
    return best_match[1] if best_match else None


def fetch_team_split_window_rankings(connection, query: TeamSplitHistoryQuery) -> list[dict[str, Any]]:
    order_direction = "DESC" if query.sort_desc else "ASC"
    season_filter = ""
    parameters: list[Any] = [query.split.key]
    if query.start_season is not None and query.end_season is not None:
        season_filter = "AND season BETWEEN ? AND ?"
        parameters.extend([query.start_season, query.end_season])
    parameters.extend([query.first_n_games, query.first_n_games])
    rows = connection.execute(
        f"""
        WITH ordered_games AS (
            SELECT
                season,
                game_date,
                gid,
                team,
                plate_appearances,
                at_bats,
                hits,
                doubles,
                triples,
                home_runs,
                walks,
                hit_by_pitch,
                sacrifice_flies,
                strikeouts,
                runs_batted_in,
                ROW_NUMBER() OVER (
                    PARTITION BY season, team
                    ORDER BY game_date, gid
                ) AS game_number
            FROM retrosheet_team_split_games
            WHERE split_key = ?
              {season_filter}
        ),
        aggregates AS (
            SELECT
                season,
                team,
                COUNT(*) AS games_played,
                SUM(plate_appearances) AS sum_pa,
                SUM(at_bats) AS sum_ab,
                SUM(hits) AS sum_h,
                SUM(doubles) AS sum_2b,
                SUM(triples) AS sum_3b,
                SUM(home_runs) AS sum_hr,
                SUM(walks) AS sum_bb,
                SUM(hit_by_pitch) AS sum_hbp,
                SUM(sacrifice_flies) AS sum_sf,
                SUM(strikeouts) AS sum_so,
                SUM(runs_batted_in) AS sum_rbi
            FROM ordered_games
            WHERE game_number <= ?
            GROUP BY season, team
            HAVING COUNT(*) = ?
        ),
        names AS (
            SELECT CAST(yearid AS TEXT) AS season, teamidretro AS team, MIN(name) AS team_name
            FROM lahman_teams
            GROUP BY CAST(yearid AS TEXT), teamidretro
        )
        SELECT
            aggregates.season,
            aggregates.team,
            COALESCE(names.team_name, aggregates.team) AS team_name,
            aggregates.games_played,
            aggregates.sum_pa,
            aggregates.sum_ab,
            aggregates.sum_h,
            aggregates.sum_2b,
            aggregates.sum_3b,
            aggregates.sum_hr,
            aggregates.sum_bb,
            aggregates.sum_hbp,
            aggregates.sum_sf,
            aggregates.sum_so,
            aggregates.sum_rbi,
            {query.metric.sql_expression} AS metric_value
        FROM aggregates
        LEFT JOIN names
            ON names.season = CAST(aggregates.season AS TEXT)
           AND names.team = aggregates.team
        WHERE ({query.metric.sql_expression}) IS NOT NULL
        ORDER BY metric_value {order_direction}, aggregates.season ASC, team_name ASC
        LIMIT 5
        """,
        tuple(parameters),
    ).fetchall()
    return [
        {
            "season": int(row["season"]),
            "team": str(row["team"]),
            "team_name": str(row["team_name"]),
            "games_played": int(row["games_played"]),
            "plate_appearances": int(row["sum_pa"]),
            "at_bats": int(row["sum_ab"]),
            "hits": int(row["sum_h"]),
            "walks": int(row["sum_bb"]),
            "hit_by_pitch": int(row["sum_hbp"]),
            "sacrifice_flies": int(row["sum_sf"]),
            "strikeouts": int(row["sum_so"]),
            "runs_batted_in": int(row["sum_rbi"]),
            "metric_value": float(row["metric_value"]),
        }
        for row in rows
    ]


def build_team_split_window_summary(query: TeamSplitHistoryQuery, rows: list[dict[str, Any]]) -> str:
    lead = rows[0]
    summary = (
        f"Across {query.scope_label}, the {query.descriptor} team {query.metric.label} {query.split.label} "
        f"through the first {query.first_n_games} games of a season was the {lead['season']} {lead['team_name']} at "
        f"{format_metric_value(lead['metric_value'], query.metric.decimal_places)}."
    )
    next_rows = rows[1:4]
    if next_rows:
        trailing = "; ".join(
            f"{row['season']} {row['team_name']} {format_metric_value(row['metric_value'], query.metric.decimal_places)}"
            for row in next_rows
        )
        summary = f"{summary} Next on the list: {trailing}."
    summary = f"{summary} Games with zero split chances still count toward the first-{query.first_n_games}-games window."
    return summary


def format_metric_value(value: float, decimal_places: int) -> str:
    return f"{value:.{decimal_places}f}"


def sync_retrosheet_team_splits(
    settings: Settings,
    *,
    retrosheet_dir: Path | None = None,
    chunk_size: int = 250_000,
) -> list[str]:
    source_dir = retrosheet_dir or (settings.raw_data_dir / "retrosheet")
    if not source_dir.exists():
        return [f"Retrosheet directory not found at {source_dir}."]

    connection = get_connection(settings.database_path)
    initialize_database(connection)
    clear_retrosheet_team_split_games(connection)
    total_rows = 0
    total_chunks = 0
    total_regular_plays = 0
    source_description = ""
    try:
        with open_retrosheet_plays_stream(source_dir) as handle:
            source_description = handle.name if hasattr(handle, "name") else "plays.csv stream"
            reader = pd.read_csv(
                handle,
                usecols=PLAY_USECOLS,
                dtype=str,
                chunksize=chunk_size,
                low_memory=False,
            )
            for chunk in reader:
                aggregated_rows, regular_plays = aggregate_situational_chunk(chunk)
                if not aggregated_rows:
                    continue
                total_rows += upsert_retrosheet_team_split_games(connection, aggregated_rows)
                total_chunks += 1
                total_regular_plays += regular_plays
        set_metadata_value(connection, "retrosheet_team_split_games_last_sync", pd.Timestamp.utcnow().isoformat())
    finally:
        connection.close()

    if total_rows == 0:
        return [f"No regular-season Retrosheet split rows were built from {source_dir}."]
    return [
        (
            "Built Retrosheet situational split game rows "
            f"from {source_description} ({total_chunks} chunk(s), {total_regular_plays:,} regular-season plays, {total_rows:,} upsert row operations)."
        )
    ]


def aggregate_situational_chunk(chunk: pd.DataFrame) -> tuple[list[dict[str, Any]], int]:
    if chunk.empty:
        return [], 0
    frame = chunk.copy()
    frame["gametype"] = frame["gametype"].fillna("").str.lower()
    frame = frame[frame["gametype"] == "regular"]
    if frame.empty:
        return [], 0

    for column in ("date", "gid", "batteam", "br1_pre", "br2_pre", "br3_pre"):
        frame[column] = frame[column].fillna("")
    frame["season"] = pd.to_numeric(frame["date"].str[:4], errors="coerce").fillna(0).astype(int)
    game_dates = pd.to_datetime(frame["date"], format="%Y%m%d", errors="coerce")
    frame = frame[game_dates.notna()].copy()
    if frame.empty:
        return [], 0
    frame["game_date"] = game_dates[game_dates.notna()].dt.strftime("%Y-%m-%d")

    for column in ("pa", "ab", "single", "double", "triple", "hr", "walk", "iw", "hbp", "sf", "k", "rbi"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0).astype(int)
    frame["hits"] = frame["single"] + frame["double"] + frame["triple"] + frame["hr"]
    frame["walks_total"] = frame["walk"] + frame["iw"]

    base_games = frame[["season", "game_date", "gid", "batteam"]].drop_duplicates()
    rows: list[dict[str, Any]] = []
    for split in TRACKED_SPLITS:
        rows.extend(_zero_rows(base_games, split.key))
        matched = frame[split.matcher(frame)].copy()
        if matched.empty:
            continue
        grouped = (
            matched.groupby(["season", "game_date", "gid", "batteam"], sort=False)
            .agg(
                plate_appearances=("pa", "sum"),
                at_bats=("ab", "sum"),
                hits=("hits", "sum"),
                doubles=("double", "sum"),
                triples=("triple", "sum"),
                home_runs=("hr", "sum"),
                walks=("walks_total", "sum"),
                hit_by_pitch=("hbp", "sum"),
                sacrifice_flies=("sf", "sum"),
                strikeouts=("k", "sum"),
                runs_batted_in=("rbi", "sum"),
            )
            .reset_index()
        )
        for row in grouped.itertuples(index=False):
            rows.append(
                {
                    "season": int(row.season),
                    "game_date": str(row.game_date),
                    "gid": str(row.gid),
                    "team": str(row.batteam),
                    "split_key": split.key,
                    "plate_appearances": int(row.plate_appearances),
                    "at_bats": int(row.at_bats),
                    "hits": int(row.hits),
                    "doubles": int(row.doubles),
                    "triples": int(row.triples),
                    "home_runs": int(row.home_runs),
                    "walks": int(row.walks),
                    "hit_by_pitch": int(row.hit_by_pitch),
                    "sacrifice_flies": int(row.sacrifice_flies),
                    "strikeouts": int(row.strikeouts),
                    "runs_batted_in": int(row.runs_batted_in),
                }
            )
    return rows, int(len(frame))


def _zero_rows(base_games: pd.DataFrame, split_key: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in base_games.itertuples(index=False):
        rows.append(
            {
                "season": int(row.season),
                "game_date": str(row.game_date),
                "gid": str(row.gid),
                "team": str(row.batteam),
                "split_key": split_key,
                "plate_appearances": 0,
                "at_bats": 0,
                "hits": 0,
                "doubles": 0,
                "triples": 0,
                "home_runs": 0,
                "walks": 0,
                "hit_by_pitch": 0,
                "sacrifice_flies": 0,
                "strikeouts": 0,
                "runs_batted_in": 0,
            }
        )
    return rows


def _has_runner(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().ne("")


@contextmanager
def open_retrosheet_plays_stream(retrosheet_dir: Path) -> Iterator[Any]:
    extracted_path = retrosheet_dir / "plays.csv"
    if extracted_path.exists():
        with extracted_path.open("rb") as handle:
            yield handle
        return

    zip_path = retrosheet_dir / "csvdownloads.zip"
    if not zip_path.exists():
        raise FileNotFoundError(f"Could not find plays.csv or {zip_path}")
    with ZipFile(zip_path) as archive:
        with archive.open("plays.csv") as handle:
            yield handle
