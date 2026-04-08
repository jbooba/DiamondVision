from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .cohort_timeline import ResolvedCohort, parse_cohort_filter, resolve_cohort_filter
from .config import Settings
from .models import EvidenceSnippet
from .query_intent import looks_like_leaderboard_question
from .query_utils import extract_minimum_qualifier
from .season_metric_leaderboards import SeasonMetricSpec, find_season_metric
from .storage import table_exists
from .team_season_leaders import build_person_name


TEAM_SPAN_PATTERN = re.compile(
    r"\b(?:for|on)\s+the\s+(?P<label>most|fewest|least|lowest|highest)\s+teams\b",
    re.IGNORECASE,
)
TEAM_COUNT_COLUMN_MAP: dict[tuple[str, str], tuple[str, str, str]] = {
    ("hitter", "hits"): ("lahman_batting", "h", "H"),
    ("hitter", "home_runs"): ("lahman_batting", "hr", "HR"),
    ("hitter", "runs"): ("lahman_batting", "r", "R"),
    ("hitter", "rbi"): ("lahman_batting", "rbi", "RBI"),
    ("hitter", "walks"): ("lahman_batting", "bb", "BB"),
    ("hitter", "strikeouts"): ("lahman_batting", "so", "SO"),
    ("hitter", "doubles"): ("lahman_batting", "c_2b", "2B"),
    ("hitter", "triples"): ("lahman_batting", "c_3b", "3B"),
    ("pitcher", "wins"): ("lahman_pitching", "w", "W"),
    ("pitcher", "losses"): ("lahman_pitching", "l", "L"),
    ("pitcher", "saves"): ("lahman_pitching", "sv", "SV"),
    ("pitcher", "strikeouts"): ("lahman_pitching", "so", "SO"),
}


@dataclass(slots=True)
class PlayerTeamSpanQuery:
    metric: SeasonMetricSpec
    descriptor: str
    sort_desc: bool
    table_name: str
    value_column: str
    value_label: str
    minimum_total: int | None
    cohort: ResolvedCohort | None


class PlayerTeamRelationshipResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_player_team_span_query(connection, question)
        if query is None:
            return None
        rows = fetch_player_team_span_rows(connection, query)
        if not rows:
            return None
        leader = rows[0]
        summary = build_player_team_span_summary(query, leader, rows[1:4])
        return EvidenceSnippet(
            source="Player-Team Relationships",
            title=f"{query.metric.label} across teams leaderboard",
            citation=build_player_team_span_citation(query),
            summary=summary,
            payload={
                "analysis_type": "player_team_span_leaderboard",
                "mode": "historical",
                "metric": query.metric.label,
                "metric_total_label": query.value_label,
                "rows": rows[:12],
                "cohort_label": query.cohort.label if query.cohort else "",
            },
        )


def parse_player_team_span_query(connection, question: str) -> PlayerTeamSpanQuery | None:
    lowered = f" {question.lower()} "
    team_span_match = TEAM_SPAN_PATTERN.search(lowered)
    if team_span_match is None or not looks_like_leaderboard_question(lowered):
        return None
    metric = find_season_metric(lowered)
    if metric is None or metric.source_family != "historical" or metric.entity_scope != "player":
        return None
    column_metadata = TEAM_COUNT_COLUMN_MAP.get((metric.role, metric.key))
    if column_metadata is None:
        return None
    label = team_span_match.group("label").lower()
    descriptor = "fewest" if label in {"fewest", "least", "lowest"} else "most"
    sort_desc = descriptor == "most"
    cohort = None
    cohort_filter = parse_cohort_filter(question)
    if cohort_filter is not None:
        cohort = resolve_cohort_filter(connection, cohort_filter)
        if cohort is None:
            return None
    minimum_total = extract_minimum_qualifier(question, metric.aliases)
    table_name, value_column, value_label = column_metadata
    return PlayerTeamSpanQuery(
        metric=metric,
        descriptor=descriptor,
        sort_desc=sort_desc,
        table_name=table_name,
        value_column=value_column,
        value_label=value_label,
        minimum_total=minimum_total,
        cohort=cohort,
    )


def fetch_player_team_span_rows(connection, query: PlayerTeamSpanQuery) -> list[dict[str, Any]]:
    if not (table_exists(connection, query.table_name) and table_exists(connection, "lahman_people")):
        return []
    value_expr = f"SUM(CAST(COALESCE(stats.{query.value_column}, '0') AS INTEGER))"
    where_clause, parameters = build_cohort_where_clause(query.cohort)
    rows = connection.execute(
        f"""
        WITH player_team_totals AS (
            SELECT
                stats.playerid,
                ppl.namefirst,
                ppl.namelast,
                stats.teamid,
                MIN(CAST(stats.yearid AS INTEGER)) AS first_team_season,
                MAX(CAST(stats.yearid AS INTEGER)) AS last_team_season,
                {value_expr} AS metric_total
            FROM {query.table_name} AS stats
            JOIN lahman_people AS ppl
              ON ppl.playerid = stats.playerid
            WHERE {where_clause}
            GROUP BY stats.playerid, ppl.namefirst, ppl.namelast, stats.teamid
        )
        SELECT
            playerid,
            namefirst,
            namelast,
            COUNT(*) AS team_count,
            SUM(metric_total) AS metric_total,
            MIN(first_team_season) AS first_season,
            MAX(last_team_season) AS last_season,
            GROUP_CONCAT(teamid, ', ') AS teams
        FROM player_team_totals
        WHERE metric_total > 0
        GROUP BY playerid, namefirst, namelast
        """,
        parameters,
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        metric_total = int(row["metric_total"] or 0)
        if query.minimum_total is not None and metric_total < query.minimum_total:
            continue
        candidates.append(
            {
                "player_name": build_person_name(row["namefirst"], row["namelast"], row["playerid"]),
                "team_count": int(row["team_count"] or 0),
                "metric_total": metric_total,
                "first_season": int(row["first_season"] or 0),
                "last_season": int(row["last_season"] or 0),
                "teams": str(row["teams"] or ""),
            }
        )
    candidates.sort(
        key=lambda row: (
            -int(row["team_count"]) if query.sort_desc else int(row["team_count"]),
            -int(row["metric_total"]) if query.sort_desc else int(row["metric_total"]),
            str(row["player_name"]),
        )
    )
    for index, row in enumerate(candidates, start=1):
        row["rank"] = index
    return candidates


def build_cohort_where_clause(cohort: ResolvedCohort | None) -> tuple[str, tuple[Any, ...]]:
    clauses = ["1=1"]
    parameters: list[Any] = []
    if cohort is None:
        return " AND ".join(clauses), tuple(parameters)
    if cohort.player_ids:
        placeholders = ", ".join("?" for _ in cohort.player_ids)
        clauses.append(f"stats.playerid IN ({placeholders})")
        parameters.extend(sorted(cohort.player_ids))
    elif cohort.player_names:
        placeholders = ", ".join("?" for _ in cohort.player_names)
        clauses.append(
            "lower(trim(coalesce(ppl.namefirst, '') || ' ' || coalesce(ppl.namelast, ''))) "
            f"IN ({placeholders})"
        )
        parameters.extend(sorted(cohort.player_names))
    return " AND ".join(clauses), tuple(parameters)


def build_player_team_span_summary(
    query: PlayerTeamSpanQuery,
    leader: dict[str, Any],
    runners_up: list[dict[str, Any]],
) -> str:
    cohort_text = f" among {query.cohort.label}" if query.cohort else ""
    summary = (
        f"Across MLB history{cohort_text}, the player who recorded {query.metric.label} for the {query.descriptor} teams "
        f"is {leader['player_name']} with {leader['team_count']} teams and {leader['metric_total']} {query.value_label}."
    )
    if runners_up:
        follow_text = "; ".join(
            f"{row['player_name']} ({row['team_count']} teams, {row['metric_total']} {query.value_label})"
            for row in runners_up
        )
        summary += f" Next on the list: {follow_text}."
    if query.minimum_total is not None:
        summary += f" Minimum total applied: {query.minimum_total} {query.value_label}."
    return summary


def build_player_team_span_citation(query: PlayerTeamSpanQuery) -> str:
    prefix = f"{query.cohort.label} " if query.cohort else ""
    return f"{prefix}{query.metric.label} by team from Lahman batting/pitching tables"
