from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .config import Settings
from .metrics import MetricCatalog
from .models import EvidenceSnippet
from .query_intent import detect_ranking_intent
from .query_utils import extract_first_n_games
from .storage import table_exists


@dataclass(slots=True)
class StatcastTeamMetricSpec:
    metric_name: str
    aliases: tuple[str, ...]
    label: str
    sql_expression: str
    decimal_places: int


SUPPORTED_STATCAST_TEAM_METRICS: tuple[StatcastTeamMetricSpec, ...] = (
    StatcastTeamMetricSpec(
        metric_name="xBA",
        aliases=("expected batting average", " xba "),
        label="xBA",
        sql_expression="CAST(xba_numerator AS REAL) / NULLIF(at_bats, 0)",
        decimal_places=3,
    ),
    StatcastTeamMetricSpec(
        metric_name="xwOBA",
        aliases=("expected woba", " xwoba "),
        label="xwOBA",
        sql_expression="CAST(xwoba_numerator AS REAL) / NULLIF(xwoba_denom, 0)",
        decimal_places=3,
    ),
    StatcastTeamMetricSpec(
        metric_name="xSLG",
        aliases=("expected slugging", " xslg "),
        label="xSLG",
        sql_expression="CAST(xslg_numerator AS REAL) / NULLIF(at_bats, 0)",
        decimal_places=3,
    ),
    StatcastTeamMetricSpec(
        metric_name="Hard-Hit Rate",
        aliases=("hard-hit rate", "hard hit rate"),
        label="Hard-Hit Rate",
        sql_expression="CAST(hard_hit_bbe AS REAL) / NULLIF(batted_ball_events, 0)",
        decimal_places=3,
    ),
    StatcastTeamMetricSpec(
        metric_name="Barrel Rate",
        aliases=("barrel rate",),
        label="Barrel Rate",
        sql_expression="CAST(barrel_bbe AS REAL) / NULLIF(batted_ball_events, 0)",
        decimal_places=3,
    ),
)


@dataclass(slots=True)
class StatcastTeamWindowQuery:
    metric: StatcastTeamMetricSpec
    first_n_games: int
    sort_desc: bool
    descriptor: str
    season: int | None


class StatcastTeamHistoryResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.catalog = MetricCatalog.load(settings.project_root)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_statcast_team_window_query(question, self.catalog)
        if query is None or not table_exists(connection, "statcast_team_games"):
            return None
        rows = fetch_statcast_team_window_rankings(connection, query)
        if not rows:
            return None
        summary = build_statcast_team_window_summary(query, rows)
        title = f"{query.metric.label} through first {query.first_n_games} games"
        if query.season is not None:
            title = f"{query.season} {title}"
        return EvidenceSnippet(
            source="Statcast Team Windows",
            title=title,
            citation="Public Statcast plate appearance data aggregated to team-game level",
            summary=summary,
            payload={
                "analysis_type": "statcast_team_window_ranking",
                "metric": query.metric.metric_name,
                "metric_label": query.metric.label,
                "first_n_games": query.first_n_games,
                "season": query.season,
                "descriptor": query.descriptor,
                "leaders": rows,
            },
        )


def parse_statcast_team_window_query(question: str, catalog: MetricCatalog) -> StatcastTeamWindowQuery | None:
    lowered = f" {question.lower()} "
    first_n_games = extract_first_n_games(question)
    if first_n_games is None or "team" not in lowered:
        return None
    metric = find_statcast_team_metric(lowered, catalog)
    if metric is None:
        return None
    ranking_intent = detect_ranking_intent(lowered, higher_is_better=True, require_hint=True)
    if ranking_intent is None:
        return None
    return StatcastTeamWindowQuery(
        metric=metric,
        first_n_games=first_n_games,
        sort_desc=ranking_intent.sort_desc,
        descriptor=ranking_intent.descriptor,
        season=extract_year(question),
    )


def find_statcast_team_metric(lowered_question: str, catalog: MetricCatalog) -> StatcastTeamMetricSpec | None:
    exact_metric_names = {metric.name for metric in catalog.search(lowered_question, limit=5)}
    best_match: tuple[int, StatcastTeamMetricSpec] | None = None
    for metric in SUPPORTED_STATCAST_TEAM_METRICS:
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


def fetch_statcast_team_window_rankings(connection, query: StatcastTeamWindowQuery) -> list[dict[str, Any]]:
    order_direction = "DESC" if query.sort_desc else "ASC"
    season_filter = "WHERE season = ?" if query.season is not None else ""
    parameters: list[Any] = [query.season] if query.season is not None else []
    parameters.extend([query.first_n_games, query.first_n_games])
    rows = connection.execute(
        f"""
        WITH ordered_games AS (
            SELECT
                season,
                game_date,
                game_pk,
                team,
                team_name,
                at_bats,
                batted_ball_events,
                xba_numerator,
                xwoba_numerator,
                xwoba_denom,
                xslg_numerator,
                hard_hit_bbe,
                barrel_bbe,
                ROW_NUMBER() OVER (
                    PARTITION BY season, team
                    ORDER BY game_date, game_pk
                ) AS game_number
            FROM statcast_team_games
            {season_filter}
        ),
        aggregates AS (
            SELECT
                season,
                team,
                MIN(team_name) AS team_name,
                COUNT(*) AS games_played,
                SUM(at_bats) AS at_bats,
                SUM(batted_ball_events) AS batted_ball_events,
                SUM(xba_numerator) AS xba_numerator,
                SUM(xwoba_numerator) AS xwoba_numerator,
                SUM(xwoba_denom) AS xwoba_denom,
                SUM(xslg_numerator) AS xslg_numerator,
                SUM(hard_hit_bbe) AS hard_hit_bbe,
                SUM(barrel_bbe) AS barrel_bbe
            FROM ordered_games
            WHERE game_number <= ?
            GROUP BY season, team
            HAVING COUNT(*) = ?
        )
        SELECT
            season,
            team,
            team_name,
            {query.metric.sql_expression} AS metric_value,
            games_played
        FROM aggregates
        WHERE ({query.metric.sql_expression}) IS NOT NULL
        ORDER BY metric_value {order_direction}, season ASC, team_name ASC
        LIMIT 5
        """,
        tuple(parameters),
    ).fetchall()
    return [
        {
            "season": int(row["season"]),
            "team": str(row["team"]),
            "team_name": str(row["team_name"]),
            "metric_value": float(row["metric_value"]),
            "games_played": int(row["games_played"]),
        }
        for row in rows
    ]


def build_statcast_team_window_summary(query: StatcastTeamWindowQuery, rows: list[dict[str, Any]]) -> str:
    lead = rows[0]
    if query.season is not None:
        prefix = (
            f"In synced Statcast data for {query.season}, the {query.descriptor} team {query.metric.label} through the first "
            f"{query.first_n_games} games was {lead['team_name']} at "
        )
    else:
        prefix = (
            f"Across synced Statcast seasons, the {query.descriptor} team {query.metric.label} through the first "
            f"{query.first_n_games} games of a season was the {lead['season']} {lead['team_name']} at "
        )
    summary = f"{prefix}{format_metric_value(lead['metric_value'], query.metric.decimal_places)}."
    next_rows = rows[1:4]
    if next_rows:
        trailing = "; ".join(
            f"{row['season']} {row['team_name']} {format_metric_value(row['metric_value'], query.metric.decimal_places)}"
            for row in next_rows
        )
        summary = f"{summary} Next on the list: {trailing}."
    if query.season is None:
        summary = f"{summary} Coverage depends on which Statcast seasons have been synced locally."
    return summary


def format_metric_value(value: float, decimal_places: int) -> str:
    return f"{value:.{decimal_places}f}"


def extract_year(question: str) -> int | None:
    match = re.search(r"\b(20\d{2})\b", question)
    return int(match.group(1)) if match else None
