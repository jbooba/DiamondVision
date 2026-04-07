from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import Settings
from .metrics import MetricCatalog, MetricDefinition
from .models import EvidenceSnippet
from .query_intent import detect_ranking_intent
from .query_utils import extract_first_n_games
from .storage import table_exists


@dataclass(slots=True)
class TeamWindowMetricSpec:
    metric_name: str
    aliases: tuple[str, ...]
    label: str
    higher_is_better: bool
    sql_expression: str
    decimal_places: int


SUPPORTED_TEAM_WINDOW_METRICS: tuple[TeamWindowMetricSpec, ...] = (
    TeamWindowMetricSpec(
        metric_name="BA",
        aliases=("batting average", " ba ", " avg ", "average"),
        label="batting average",
        higher_is_better=True,
        sql_expression="CAST(sum_h AS REAL) / NULLIF(sum_ab, 0)",
        decimal_places=3,
    ),
    TeamWindowMetricSpec(
        metric_name="OBP",
        aliases=("obp", "on-base percentage"),
        label="on-base percentage",
        higher_is_better=True,
        sql_expression="CAST(sum_h + sum_bb + sum_hbp AS REAL) / NULLIF(sum_ab + sum_bb + sum_hbp + sum_sf, 0)",
        decimal_places=3,
    ),
    TeamWindowMetricSpec(
        metric_name="SLG",
        aliases=("slg", "slugging percentage"),
        label="slugging percentage",
        higher_is_better=True,
        sql_expression="CAST((sum_h - sum_2b - sum_3b - sum_hr) + (2 * sum_2b) + (3 * sum_3b) + (4 * sum_hr) AS REAL) / NULLIF(sum_ab, 0)",
        decimal_places=3,
    ),
    TeamWindowMetricSpec(
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
    TeamWindowMetricSpec(
        metric_name="ERA",
        aliases=("era", "earned run average"),
        label="ERA",
        higher_is_better=False,
        sql_expression="27.0 * CAST(sum_er AS REAL) / NULLIF(sum_ipouts, 0)",
        decimal_places=2,
    ),
    TeamWindowMetricSpec(
        metric_name="WHIP",
        aliases=("whip",),
        label="WHIP",
        higher_is_better=False,
        sql_expression="3.0 * CAST(sum_ph + sum_pbb AS REAL) / NULLIF(sum_ipouts, 0)",
        decimal_places=2,
    ),
    TeamWindowMetricSpec(
        metric_name="Runs/Game",
        aliases=("runs per game",),
        label="runs per game",
        higher_is_better=True,
        sql_expression="CAST(sum_runs AS REAL) / NULLIF(games_played, 0)",
        decimal_places=2,
    ),
    TeamWindowMetricSpec(
        metric_name="HR/Game",
        aliases=("home runs per game", "hr per game"),
        label="home runs per game",
        higher_is_better=True,
        sql_expression="CAST(sum_hr AS REAL) / NULLIF(games_played, 0)",
        decimal_places=2,
    ),
)


@dataclass(slots=True)
class TeamHistoryRankingQuery:
    metric: TeamWindowMetricSpec
    first_n_games: int
    sort_desc: bool
    descriptor: str


class TeamHistoryRankingResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.catalog = MetricCatalog.load(settings.project_root)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_team_history_ranking_query(question, self.catalog)
        if query is None:
            return None
        if not table_exists(connection, "retrosheet_teamstats") or not table_exists(connection, "lahman_teams"):
            return None
        rows = fetch_team_window_rankings(connection, query)
        if not rows:
            return None
        summary = build_team_window_summary(query, rows)
        return EvidenceSnippet(
            source="Team History Rankings",
            title=f"{query.metric.label} through first {query.first_n_games} games",
            citation="Retrosheet team game logs aggregated by team-season",
            summary=summary,
            payload={
                "analysis_type": "team_window_ranking",
                "metric": query.metric.metric_name,
                "metric_label": query.metric.label,
                "first_n_games": query.first_n_games,
                "descriptor": query.descriptor,
                "sort_desc": query.sort_desc,
                "leaders": rows,
            },
        )


def parse_team_history_ranking_query(question: str, catalog: MetricCatalog) -> TeamHistoryRankingQuery | None:
    lowered = f" {question.lower()} "
    first_n_games = extract_first_n_games(question)
    if first_n_games is None or "team" not in lowered:
        return None
    metric = find_team_window_metric(lowered, catalog)
    if metric is None:
        return None
    ranking_intent = detect_ranking_intent(lowered, higher_is_better=metric.higher_is_better, require_hint=True)
    if ranking_intent is None:
        return None
    return TeamHistoryRankingQuery(
        metric=metric,
        first_n_games=first_n_games,
        sort_desc=ranking_intent.sort_desc,
        descriptor=ranking_intent.descriptor,
    )


def find_team_window_metric(lowered_question: str, catalog: MetricCatalog) -> TeamWindowMetricSpec | None:
    exact_metric_names = {metric.name for metric in catalog.search(lowered_question, limit=5)}
    best_match: tuple[int, TeamWindowMetricSpec] | None = None
    for metric in SUPPORTED_TEAM_WINDOW_METRICS:
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


def fetch_team_window_rankings(connection, query: TeamHistoryRankingQuery) -> list[dict[str, Any]]:
    order_direction = "DESC" if query.sort_desc else "ASC"
    rows = connection.execute(
        f"""
        WITH ordered_games AS (
            SELECT
                team,
                substr(date, 1, 4) AS season,
                gid,
                CAST(COALESCE(b_ab, '0') AS INTEGER) AS b_ab,
                CAST(COALESCE(b_h, '0') AS INTEGER) AS b_h,
                CAST(COALESCE(b_d, '0') AS INTEGER) AS b_2b,
                CAST(COALESCE(b_t, '0') AS INTEGER) AS b_3b,
                CAST(COALESCE(b_hr, '0') AS INTEGER) AS b_hr,
                CAST(COALESCE(b_w, '0') AS INTEGER) AS b_bb,
                CAST(COALESCE(b_hbp, '0') AS INTEGER) AS b_hbp,
                CAST(COALESCE(b_sf, '0') AS INTEGER) AS b_sf,
                CAST(COALESCE(b_r, '0') AS INTEGER) AS b_r,
                CAST(COALESCE(p_h, '0') AS INTEGER) AS p_h,
                CAST(COALESCE(p_w, '0') AS INTEGER) AS p_bb,
                CAST(COALESCE(p_er, '0') AS INTEGER) AS p_er,
                CAST(COALESCE(p_ipouts, '0') AS INTEGER) AS p_ipouts,
                ROW_NUMBER() OVER (
                    PARTITION BY team, substr(date, 1, 4)
                    ORDER BY date, CAST(COALESCE(number, '0') AS INTEGER), gid
                ) AS game_number
            FROM retrosheet_teamstats
            WHERE stattype = 'value' AND gametype = 'regular'
        ),
        aggregates AS (
            SELECT
                team,
                season,
                COUNT(*) AS games_played,
                SUM(b_ab) AS sum_ab,
                SUM(b_h) AS sum_h,
                SUM(b_2b) AS sum_2b,
                SUM(b_3b) AS sum_3b,
                SUM(b_hr) AS sum_hr,
                SUM(b_bb) AS sum_bb,
                SUM(b_hbp) AS sum_hbp,
                SUM(b_sf) AS sum_sf,
                SUM(b_r) AS sum_runs,
                SUM(p_h) AS sum_ph,
                SUM(p_bb) AS sum_pbb,
                SUM(p_er) AS sum_er,
                SUM(p_ipouts) AS sum_ipouts
            FROM ordered_games
            WHERE game_number <= ?
            GROUP BY team, season
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
            {query.metric.sql_expression} AS metric_value,
            aggregates.games_played
        FROM aggregates
        LEFT JOIN names
            ON names.season = aggregates.season
           AND names.team = aggregates.team
        WHERE ({query.metric.sql_expression}) IS NOT NULL
        ORDER BY metric_value {order_direction}, aggregates.season ASC, team_name ASC
        LIMIT 5
        """,
        (query.first_n_games, query.first_n_games),
    ).fetchall()
    return [
        {
            "season": str(row["season"]),
            "team": str(row["team"]),
            "team_name": str(row["team_name"]),
            "metric_value": float(row["metric_value"]),
            "games_played": int(row["games_played"]),
        }
        for row in rows
    ]


def build_team_window_summary(query: TeamHistoryRankingQuery, rows: list[dict[str, Any]]) -> str:
    lead = rows[0]
    summary = (
        f"Across imported Retrosheet history, the {query.descriptor} team {query.metric.label} through the first "
        f"{query.first_n_games} games of a season was the {lead['season']} {lead['team_name']} at "
        f"{format_metric_value(lead['metric_value'], query.metric.decimal_places)}."
    )
    next_rows = rows[1:4]
    if next_rows:
        trailing = "; ".join(
            f"{row['season']} {row['team_name']} {format_metric_value(row['metric_value'], query.metric.decimal_places)}"
            for row in next_rows
        )
        summary = f"{summary} Next on the list: {trailing}."
    return summary


def format_metric_value(value: float, decimal_places: int) -> str:
    if decimal_places == 3:
        return f"{value:.3f}"
    return f"{value:.{decimal_places}f}"
