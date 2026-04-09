from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .models import EvidenceSnippet
from .query_intent import detect_ranking_intent, looks_like_leaderboard_question
from .query_utils import extract_referenced_season, extract_season_span, question_requests_current_scope
from .statcast_relationships import extract_batter_name, safe_float
from .statcast_sync import iter_sync_chunks, resolve_statcast_sync_windows
from .pybaseball_adapter import load_statcast_range
from .live import LiveStatsClient
from .storage import table_exists


HIT_EVENTS = {"single", "double", "triple", "home_run"}
WALK_EVENTS = {"walk", "intent_walk"}
STRIKEOUT_EVENTS = {"strikeout", "strikeout_double_play"}
SACRIFICE_FLY_EVENTS = {"sac_fly", "sac_fly_double_play"}
SACRIFICE_BUNT_EVENTS = {"sac_bunt", "sac_bunt_double_play"}
NON_AT_BAT_EVENTS = WALK_EVENTS | {"hit_by_pitch"} | SACRIFICE_FLY_EVENTS | SACRIFICE_BUNT_EVENTS | {
    "catcher_interf",
    "other_out",
}
HISTORICAL_SCOPE_HINTS = {"historically", "all time", "all-time", "ever", "career"}
OFFENSIVE_SUMMARY_HINTS = ("offensive", "offense", "offensively", "at the plate", "hitting")


@dataclass(frozen=True, slots=True)
class SituationalSplitSpec:
    key: str
    label: str
    aliases: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PlayerSituationalMetricSpec:
    key: str
    label: str
    aliases: tuple[str, ...]
    higher_is_better: bool
    kind: str
    min_sample_size: int = 0
    sample_basis: str | None = None


@dataclass(slots=True)
class PlayerSituationalQuery:
    split: SituationalSplitSpec
    metric: PlayerSituationalMetricSpec
    descriptor: str
    sort_desc: bool
    start_season: int
    end_season: int
    scope_label: str
    mode: str


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
    ),
    SituationalSplitSpec(
        key="men_on",
        label="with runners on",
        aliases=(" with runners on ", " with men on ", " runners on base ", " men on base "),
    ),
    SituationalSplitSpec(
        key="bases_empty",
        label="with the bases empty",
        aliases=(" with bases empty ", " bases empty "),
    ),
    SituationalSplitSpec(
        key="bases_loaded",
        label="with the bases loaded",
        aliases=(" with bases loaded ", " bases loaded "),
    ),
)

SUPPORTED_METRICS: tuple[PlayerSituationalMetricSpec, ...] = (
    PlayerSituationalMetricSpec("ops", "OPS", ("ops", "on-base plus slugging", "on base plus slugging"), True, "rate", 25, "plate_appearances"),
    PlayerSituationalMetricSpec("obp", "OBP", ("obp", "on-base percentage", "on base percentage"), True, "rate", 25, "plate_appearances"),
    PlayerSituationalMetricSpec("slg", "SLG", ("slg", "slugging percentage", "slugging"), True, "rate", 25, "at_bats"),
    PlayerSituationalMetricSpec("ba", "BA", ("batting average", " ba ", " avg ", "average"), True, "rate", 25, "at_bats"),
    PlayerSituationalMetricSpec("home_runs", "HR", ("home runs", "home run", "homers", "homer", " hr "), True, "count"),
    PlayerSituationalMetricSpec("hits", "Hits", ("base hits", "base hit", " hits "), True, "count"),
    PlayerSituationalMetricSpec("walks", "BB", ("walks", "walk", " bb "), True, "count"),
    PlayerSituationalMetricSpec("strikeouts", "SO", ("strikeouts", "strikeout", " struck out ", " so ", " ks "), False, "count"),
    PlayerSituationalMetricSpec("plate_appearances", "PA", ("plate appearances", "plate appearance", " pa "), True, "count"),
    PlayerSituationalMetricSpec("at_bats", "AB", ("at-bats", "at bats", " at bat ", " ab "), True, "count"),
)


class PlayerSituationalLeaderboardResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        current_season = self.settings.live_season or date.today().year
        query = parse_player_situational_query(question, current_season)
        if query is None:
            return None
        if query.start_season < 2015:
            return build_player_situational_gap_snippet(query, "Public Statcast split leaderboards only exist from 2015 forward, so pre-Statcast seasons need a different source family.")

        rows = run_player_situational_query(connection, self.live_client, query)
        if not rows:
            return build_player_situational_gap_snippet(query, "I recognized the split leaderboard, but I did not find enough terminal Statcast events to ground it yet.")
        summary = build_player_situational_summary(query, rows)
        mode = "live" if query.mode == "live" else "historical"
        return EvidenceSnippet(
            source="Player Situational Leaderboards",
            title=f"{query.scope_label} {query.metric.label} {query.split.label} leaderboard",
            citation="Local Statcast terminal-event warehouse aggregated to player split leaderboards",
            summary=summary,
            payload={
                "analysis_type": "player_situational_leaderboard",
                "mode": mode,
                "start_season": query.start_season,
                "end_season": query.end_season,
                "scope_label": query.scope_label,
                "split_key": query.split.key,
                "split_label": query.split.label,
                "metric": query.metric.label,
                "descriptor": query.descriptor,
                "leaders": rows,
            },
        )


def parse_player_situational_query(question: str, current_season: int) -> PlayerSituationalQuery | None:
    lowered = f" {question.lower()} "
    split = find_split(lowered)
    if split is None:
        return None
    if "team" in lowered or "roster" in lowered:
        return None
    if not looks_like_leaderboard_question(lowered):
        return None
    metric = find_metric(lowered)
    if metric is None:
        return None
    ranking = detect_ranking_intent(lowered, higher_is_better=metric.higher_is_better, require_hint=True)
    if ranking is None:
        return None
    season_span = extract_season_span(question, current_season)
    referenced_season = extract_referenced_season(question, current_season)
    if season_span is not None:
        start_season = season_span.start_season
        end_season = season_span.end_season
        scope_label = season_span.label
        mode = "historical"
    elif referenced_season is not None:
        start_season = referenced_season
        end_season = referenced_season
        scope_label = str(referenced_season)
        mode = "live" if referenced_season == current_season else "historical"
    elif any(token in lowered for token in HISTORICAL_SCOPE_HINTS) or "statcast era" in lowered:
        start_season = 2015
        end_season = current_season
        scope_label = "Statcast era"
        mode = "historical"
    elif question_requests_current_scope(question):
        start_season = current_season
        end_season = current_season
        scope_label = str(current_season)
        mode = "live"
    else:
        start_season = current_season
        end_season = current_season
        scope_label = str(current_season)
        mode = "live"
    return PlayerSituationalQuery(
        split=split,
        metric=metric,
        descriptor=ranking.descriptor,
        sort_desc=ranking.sort_desc,
        start_season=start_season,
        end_season=end_season,
        scope_label=scope_label,
        mode=mode,
    )


def find_split(lowered_question: str) -> SituationalSplitSpec | None:
    best: tuple[int, SituationalSplitSpec] | None = None
    for split in TRACKED_SPLITS:
        score = 0
        for alias in split.aliases:
            if alias in lowered_question:
                score = max(score, len(alias.strip()))
        if score and (best is None or score > best[0]):
            best = (score, split)
    return best[1] if best else None


def find_metric(lowered_question: str) -> PlayerSituationalMetricSpec | None:
    if any(token in lowered_question for token in OFFENSIVE_SUMMARY_HINTS):
        return SUPPORTED_METRICS[0]
    best: tuple[int, PlayerSituationalMetricSpec] | None = None
    for metric in SUPPORTED_METRICS:
        score = 0
        for alias in metric.aliases:
            if alias in lowered_question:
                score = max(score, len(alias.strip()))
        if score and (best is None or score > best[0]):
            best = (score, metric)
    return best[1] if best else None


def run_player_situational_query(connection, live_client: LiveStatsClient, query: PlayerSituationalQuery) -> list[dict[str, Any]]:
    if table_exists(connection, "statcast_events"):
        local_rows = fetch_player_situational_rows_from_events(connection, query)
        if local_rows:
            return local_rows
    if query.start_season != query.end_season or query.split.key != "risp":
        return []
    return fetch_player_situational_rows_from_live_feed(live_client, query)


def fetch_player_situational_rows_from_events(connection, query: PlayerSituationalQuery) -> list[dict[str, Any]]:
    split_clause = "has_risp = 1" if query.split.key == "risp" else None
    if split_clause is None:
        return []
    order_direction = "DESC" if query.sort_desc else "ASC"
    rows = connection.execute(
        f"""
        WITH aggregates AS (
            SELECT
                batter_id AS player_id,
                MIN(batter_name) AS player_name,
                MIN(batting_team) AS team,
                COUNT(*) AS plate_appearances,
                SUM(is_ab) AS at_bats,
                SUM(is_hit) AS hits,
                SUM(CASE WHEN event = 'double' THEN 1 ELSE 0 END) AS doubles,
                SUM(CASE WHEN event = 'triple' THEN 1 ELSE 0 END) AS triples,
                SUM(is_home_run) AS home_runs,
                SUM(CASE WHEN event IN ('walk', 'intent_walk') THEN 1 ELSE 0 END) AS walks,
                SUM(CASE WHEN event = 'hit_by_pitch' THEN 1 ELSE 0 END) AS hit_by_pitch,
                SUM(CASE WHEN event IN ('sac_fly', 'sac_fly_double_play') THEN 1 ELSE 0 END) AS sacrifice_flies,
                SUM(is_strikeout) AS strikeouts
            FROM statcast_events
            WHERE season BETWEEN ? AND ?
              AND event <> ''
              AND {split_clause}
            GROUP BY batter_id
        )
        SELECT
            player_id,
            player_name,
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
            {metric_expression(query.metric.key)} AS metric_value
        FROM aggregates
        WHERE {metric_expression(query.metric.key)} IS NOT NULL
          AND (? = '' OR COALESCE(player_name, '') <> '')
        ORDER BY metric_value {order_direction}, plate_appearances DESC, hits DESC, player_name ASC
        LIMIT 8
        """,
        (query.start_season, query.end_season, ""),
    ).fetchall()
    leaders = []
    for row in rows:
        sample_value = int(row[query.metric.sample_basis] or 0) if query.metric.sample_basis else 0
        if query.metric.sample_basis and query.metric.min_sample_size > 0 and sample_value < query.metric.min_sample_size:
            continue
        leaders.append(
            {
                "player_id": int(row["player_id"]),
                "player_name": str(row["player_name"] or row["player_id"]),
                "team": str(row["team"] or ""),
                "plate_appearances": int(row["plate_appearances"] or 0),
                "at_bats": int(row["at_bats"] or 0),
                "hits": int(row["hits"] or 0),
                "home_runs": int(row["home_runs"] or 0),
                "walks": int(row["walks"] or 0),
                "strikeouts": int(row["strikeouts"] or 0),
                "metric_value": float(row["metric_value"]),
            }
        )
    return leaders[:5]


def fetch_player_situational_rows_from_live_feed(live_client: LiveStatsClient, query: PlayerSituationalQuery) -> list[dict[str, Any]]:
    aggregates: dict[int, dict[str, Any]] = {}
    windows = resolve_statcast_sync_windows(
        live_client.settings,
        start_season=query.start_season,
        end_season=query.end_season,
    )
    for window in windows:
        for chunk_start, chunk_end in iter_sync_chunks(window.start_date, window.end_date, 21):
            rows = load_statcast_range(chunk_start.isoformat(), chunk_end.isoformat())
            for row in rows:
                aggregate_player_split_row(row, query, aggregates)
    if not aggregates:
        return []

    leaders: list[dict[str, Any]] = []
    for player_id, aggregate in aggregates.items():
        metric_value = compute_metric_value(query.metric.key, aggregate)
        if metric_value is None:
            continue
        if query.metric.sample_basis and query.metric.min_sample_size > 0:
            sample_value = int(aggregate.get(query.metric.sample_basis) or 0)
            if sample_value < query.metric.min_sample_size:
                continue
        leaders.append(
            {
                "player_id": player_id,
                "player_name": str(aggregate.get("player_name") or player_id),
                "team": str(aggregate.get("team") or ""),
                "plate_appearances": int(aggregate.get("plate_appearances") or 0),
                "at_bats": int(aggregate.get("at_bats") or 0),
                "hits": int(aggregate.get("hits") or 0),
                "home_runs": int(aggregate.get("home_runs") or 0),
                "walks": int(aggregate.get("walks") or 0),
                "strikeouts": int(aggregate.get("strikeouts") or 0),
                "metric_value": float(metric_value),
            }
        )
    leaders.sort(
        key=lambda row: (
            row["metric_value"],
            row["plate_appearances"],
            row["hits"],
            row["player_name"],
        ),
        reverse=query.sort_desc,
    )
    leaders = leaders[:8]
    fill_missing_player_names(live_client, leaders)
    return leaders[:5]


def metric_expression(metric_key: str) -> str:
    if metric_key == "ba":
        return "CAST(hits AS REAL) / NULLIF(at_bats, 0)"
    if metric_key == "obp":
        return "CAST(hits + walks + hit_by_pitch AS REAL) / NULLIF(at_bats + walks + hit_by_pitch + sacrifice_flies, 0)"
    if metric_key == "slg":
        return "CAST((hits - doubles - triples - home_runs) + (2 * doubles) + (3 * triples) + (4 * home_runs) AS REAL) / NULLIF(at_bats, 0)"
    if metric_key == "ops":
        return """
            (
                CAST(hits + walks + hit_by_pitch AS REAL) / NULLIF(at_bats + walks + hit_by_pitch + sacrifice_flies, 0)
            ) + (
                CAST((hits - doubles - triples - home_runs) + (2 * doubles) + (3 * triples) + (4 * home_runs) AS REAL) / NULLIF(at_bats, 0)
            )
        """
    if metric_key == "home_runs":
        return "CAST(home_runs AS REAL)"
    if metric_key == "hits":
        return "CAST(hits AS REAL)"
    if metric_key == "walks":
        return "CAST(walks AS REAL)"
    if metric_key == "strikeouts":
        return "CAST(strikeouts AS REAL)"
    if metric_key == "plate_appearances":
        return "CAST(plate_appearances AS REAL)"
    if metric_key == "at_bats":
        return "CAST(at_bats AS REAL)"
    return "NULL"


def aggregate_player_split_row(row: dict[str, Any], query: PlayerSituationalQuery, aggregates: dict[int, dict[str, Any]]) -> None:
    if str(row.get("game_type") or "R") not in {"R", ""}:
        return
    event_name = str(row.get("events") or "").strip().lower()
    if not event_name:
        return
    if not split_matches(row, query.split.key):
        return
    batter_id = safe_int(row.get("batter"))
    if batter_id is None:
        return
    aggregate = aggregates.setdefault(
        batter_id,
        {
            "player_name": extract_batter_name(row),
            "team": batting_team_code(row),
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
        },
    )
    if not aggregate.get("player_name"):
        aggregate["player_name"] = extract_batter_name(row)
    if not aggregate.get("team"):
        aggregate["team"] = batting_team_code(row)

    aggregate["plate_appearances"] = int(aggregate["plate_appearances"]) + 1
    if event_name not in NON_AT_BAT_EVENTS:
        aggregate["at_bats"] = int(aggregate["at_bats"]) + 1
    if event_name in HIT_EVENTS:
        aggregate["hits"] = int(aggregate["hits"]) + 1
    if event_name == "double":
        aggregate["doubles"] = int(aggregate["doubles"]) + 1
    if event_name == "triple":
        aggregate["triples"] = int(aggregate["triples"]) + 1
    if event_name == "home_run":
        aggregate["home_runs"] = int(aggregate["home_runs"]) + 1
    if event_name in WALK_EVENTS:
        aggregate["walks"] = int(aggregate["walks"]) + 1
    if event_name == "hit_by_pitch":
        aggregate["hit_by_pitch"] = int(aggregate["hit_by_pitch"]) + 1
    if event_name in SACRIFICE_FLY_EVENTS:
        aggregate["sacrifice_flies"] = int(aggregate["sacrifice_flies"]) + 1
    if event_name in STRIKEOUT_EVENTS:
        aggregate["strikeouts"] = int(aggregate["strikeouts"]) + 1


def split_matches(row: dict[str, Any], split_key: str) -> bool:
    on_1b = has_runner(row.get("on_1b"))
    on_2b = has_runner(row.get("on_2b"))
    on_3b = has_runner(row.get("on_3b"))
    if split_key == "risp":
        return on_2b or on_3b
    if split_key == "men_on":
        return on_1b or on_2b or on_3b
    if split_key == "bases_empty":
        return not (on_1b or on_2b or on_3b)
    if split_key == "bases_loaded":
        return on_1b and on_2b and on_3b
    return False


def has_runner(value: Any) -> bool:
    if value in (None, "", "nan"):
        return False
    numeric = safe_float(value)
    return numeric is not None and numeric > 0


def batting_team_code(row: dict[str, Any]) -> str:
    half = str(row.get("inning_topbot") or "").casefold()
    if half == "top":
        return str(row.get("away_team") or "").strip()
    if half == "bottom":
        return str(row.get("home_team") or "").strip()
    return ""


def compute_metric_value(metric_key: str, aggregate: dict[str, Any]) -> float | None:
    at_bats = int(aggregate.get("at_bats") or 0)
    hits = int(aggregate.get("hits") or 0)
    doubles = int(aggregate.get("doubles") or 0)
    triples = int(aggregate.get("triples") or 0)
    home_runs = int(aggregate.get("home_runs") or 0)
    walks = int(aggregate.get("walks") or 0)
    hit_by_pitch = int(aggregate.get("hit_by_pitch") or 0)
    sacrifice_flies = int(aggregate.get("sacrifice_flies") or 0)
    plate_appearances = int(aggregate.get("plate_appearances") or 0)
    strikeouts = int(aggregate.get("strikeouts") or 0)
    if metric_key == "ba":
        return (hits / at_bats) if at_bats else None
    if metric_key == "obp":
        denom = at_bats + walks + hit_by_pitch + sacrifice_flies
        return ((hits + walks + hit_by_pitch) / denom) if denom else None
    if metric_key == "slg":
        total_bases = (hits - doubles - triples - home_runs) + (2 * doubles) + (3 * triples) + (4 * home_runs)
        return (total_bases / at_bats) if at_bats else None
    if metric_key == "ops":
        obp = compute_metric_value("obp", aggregate)
        slg = compute_metric_value("slg", aggregate)
        return (obp + slg) if obp is not None and slg is not None else None
    if metric_key == "home_runs":
        return float(home_runs)
    if metric_key == "hits":
        return float(hits)
    if metric_key == "walks":
        return float(walks)
    if metric_key == "strikeouts":
        return float(strikeouts)
    if metric_key == "plate_appearances":
        return float(plate_appearances)
    if metric_key == "at_bats":
        return float(at_bats)
    return None


def fill_missing_player_names(live_client: LiveStatsClient, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        player_name = str(row.get("player_name") or "").strip()
        if player_name and player_name != str(row.get("player_id")):
            continue
        details = live_client.person_details(int(row.get("player_id") or 0))
        if details and details.get("fullName"):
            row["player_name"] = str(details["fullName"]).strip()


def build_player_situational_summary(query: PlayerSituationalQuery, rows: list[dict[str, Any]]) -> str:
    lead = rows[0]
    metric_value = format_metric_value(query.metric, float(lead["metric_value"]))
    summary = (
        f"Across tracked Statcast terminal events in {query.scope_label}, the {query.descriptor} "
        f"{query.metric.label} {query.split.label} belongs to {lead['player_name']} ({lead['team']}) at {metric_value}. "
        f"That line comes from {lead['hits']} hits in {lead['at_bats']} at-bats across {lead['plate_appearances']} plate appearances."
    )
    trailing = rows[1:4]
    if trailing:
        summary = (
            f"{summary} Next on the board: "
            + "; ".join(
                f"{row['player_name']} ({row['team']}) {format_metric_value(query.metric, float(row['metric_value']))}"
                for row in trailing
            )
            + "."
        )
    return summary


def build_player_situational_gap_snippet(query: PlayerSituationalQuery, reason: str) -> EvidenceSnippet:
    return EvidenceSnippet(
        source="Player Situational Leaderboards",
        title=f"{query.metric.label} {query.split.label} source gap",
        citation="Raw Statcast split leaderboard planner",
        summary=(
            f"I understand this as a player split leaderboard for {query.metric.label} {query.split.label} in {query.scope_label}. "
            f"{reason}"
        ),
        payload={
            "analysis_type": "contextual_source_gap",
            "metric": query.metric.label,
            "context": query.split.label,
        },
    )


def format_metric_value(metric: PlayerSituationalMetricSpec, value: float) -> str:
    if metric.kind == "rate":
        return f"{value:.3f}"
    return str(int(round(value)))


def safe_int(value: Any) -> int | None:
    numeric = safe_float(value)
    return int(numeric) if numeric is not None else None
