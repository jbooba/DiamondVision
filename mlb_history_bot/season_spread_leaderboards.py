from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from .config import Settings
from .metrics import MetricCatalog
from .models import EvidenceSnippet
from .query_intent import detect_ranking_intent
from .season_metric_leaderboards import (
    SeasonMetricQuery,
    fetch_historical_fielder_rows,
    fetch_historical_hitter_rows,
    fetch_historical_pitcher_rows,
    fetch_historical_team_rows,
    fetch_statcast_season_rows,
    parse_season_metric_query,
)
from .team_evaluator import safe_float


SPREAD_HINT_WORDS = (" delta ", " difference ", " gap ", " spread ")
SEASON_EXTREMA_HINT_WORDS = (
    " highest ",
    " lowest ",
    " best ",
    " worst ",
    " season ",
)


@dataclass(slots=True)
class SeasonSpreadQuery:
    base_query: SeasonMetricQuery
    descriptor: str
    sort_desc: bool


class SeasonSpreadLeaderboardResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.catalog = MetricCatalog.load(settings.project_root)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_season_spread_query(connection, self.settings, self.catalog, question)
        if query is None:
            return None
        rows = fetch_season_spread_rows(connection, query)
        if not rows:
            return None
        leader = rows[0]
        return EvidenceSnippet(
            source="Season Spread Leaderboards",
            title=f"{query.base_query.scope_label} {query.base_query.metric.label} season spread leaderboard",
            citation="Computed from full local season-by-season rows, then reduced to each entity's highest/lowest season span",
            summary=build_season_spread_summary(query, leader, rows[1:4]),
            payload={
                "analysis_type": "season_metric_spread_leaderboard",
                "mode": "historical" if query.base_query.metric.source_family == "historical" else "hybrid",
                "metric": query.base_query.metric.label,
                "entity_scope": query.base_query.entity_scope,
                "role": query.base_query.role,
                "source_family": query.base_query.metric.source_family,
                "scope_label": query.base_query.scope_label,
                "complete": True,
                "total_row_count": len(rows),
                "rows": rows[:25],
            },
        )


def parse_season_spread_query(
    connection,
    settings: Settings,
    catalog: MetricCatalog,
    question: str,
) -> SeasonSpreadQuery | None:
    lowered = f" {question.lower()} "
    if not any(token in lowered for token in SPREAD_HINT_WORDS):
        return None
    if sum(token in lowered for token in SEASON_EXTREMA_HINT_WORDS) < 2:
        return None
    if " season " not in lowered:
        return None
    base_query = parse_season_metric_query(connection, settings, catalog, question)
    if base_query is None:
        return None
    if base_query.metric.source_family not in {"historical", "statcast"}:
        return None
    ranking_intent = detect_ranking_intent(lowered, higher_is_better=True, fallback_label="highest")
    descriptor = "highest"
    sort_desc = True
    if ranking_intent is not None:
        descriptor = ranking_intent.descriptor
        sort_desc = ranking_intent.sort_desc
    season_query = replace(base_query, aggregate_range=False)
    return SeasonSpreadQuery(base_query=season_query, descriptor=descriptor, sort_desc=sort_desc)


def fetch_season_spread_rows(connection, query: SeasonSpreadQuery) -> list[dict[str, Any]]:
    season_rows = fetch_season_level_rows(connection, query.base_query)
    grouped: dict[str, dict[str, Any]] = {}
    for row in season_rows:
        metric_value = safe_float(row.get("metric_value"))
        if metric_value is None:
            continue
        entity_key = build_entity_key(query.base_query, row)
        if not entity_key:
            continue
        bucket = grouped.setdefault(
            entity_key,
            {
                "player_name": row.get("player_name"),
                "team_name": row.get("team_name"),
                "team": row.get("team"),
                "high_row": row,
                "low_row": row,
                "season_count": 0,
            },
        )
        bucket["season_count"] += 1
        if metric_value > safe_float(bucket["high_row"].get("metric_value")):
            bucket["high_row"] = row
        if metric_value < safe_float(bucket["low_row"].get("metric_value")):
            bucket["low_row"] = row
    normalized: list[dict[str, Any]] = []
    for bucket in grouped.values():
        if int(bucket["season_count"]) < 2:
            continue
        high_row = bucket["high_row"]
        low_row = bucket["low_row"]
        high_value = safe_float(high_row.get("metric_value"))
        low_value = safe_float(low_row.get("metric_value"))
        if high_value is None or low_value is None:
            continue
        normalized.append(
            {
                "player_name": bucket.get("player_name"),
                "team_name": str(bucket.get("team_name") or bucket.get("team") or ""),
                "team": bucket.get("team"),
                "metric_value": float(high_value - low_value),
                "spread_value": float(high_value - low_value),
                "high_metric_value": float(high_value),
                "low_metric_value": float(low_value),
                "high_scope_label": str(high_row.get("scope_label") or high_row.get("season") or ""),
                "low_scope_label": str(low_row.get("scope_label") or low_row.get("season") or ""),
                "scope_start_season": min(
                    int(high_row.get("scope_start_season") or high_row.get("season") or 0),
                    int(low_row.get("scope_start_season") or low_row.get("season") or 0),
                ),
                "scope_end_season": max(
                    int(high_row.get("scope_end_season") or high_row.get("season") or 0),
                    int(low_row.get("scope_end_season") or low_row.get("season") or 0),
                ),
                "season_count": int(bucket["season_count"]),
                "sample_size": float(bucket["season_count"]),
            }
        )
    normalized.sort(
        key=lambda row: (
            -row["spread_value"] if query.sort_desc else row["spread_value"],
            str(row.get("player_name") or row.get("team_name") or ""),
        )
    )
    for index, row in enumerate(normalized, start=1):
        row["rank"] = index
    return normalized


def fetch_season_level_rows(connection, query: SeasonMetricQuery) -> list[dict[str, Any]]:
    if query.metric.source_family == "historical":
        if query.entity_scope == "team":
            return fetch_historical_team_rows(connection, query)
        if query.role == "pitcher":
            return fetch_historical_pitcher_rows(connection, query)
        if query.role == "fielder":
            return fetch_historical_fielder_rows(connection, query)
        return fetch_historical_hitter_rows(connection, query)
    return fetch_statcast_season_rows(connection, query)


def build_entity_key(query: SeasonMetricQuery, row: dict[str, Any]) -> str:
    if query.entity_scope == "team":
        return str(row.get("team") or row.get("team_name") or "").strip()
    return str(row.get("player_name") or "").strip().casefold()


def build_season_spread_summary(
    query: SeasonSpreadQuery,
    leader: dict[str, Any],
    others: list[dict[str, Any]],
) -> str:
    subject_label = str(leader.get("player_name") or leader.get("team_name") or "Unknown")
    spread_text = f"{leader['spread_value']:{query.base_query.metric.formatter}}"
    high_text = f"{leader['high_metric_value']:{query.base_query.metric.formatter}}"
    low_text = f"{leader['low_metric_value']:{query.base_query.metric.formatter}}"
    role_label = query.base_query.role if query.base_query.entity_scope == "player" else "team"
    summary = (
        f"For {query.base_query.scope_label}, the {query.descriptor} spread between highest and lowest {query.base_query.metric.label} seasons "
        f"belongs to {subject_label}: {spread_text}. "
        f"High season: {high_text} in {leader['high_scope_label']}. "
        f"Low season: {low_text} in {leader['low_scope_label']}. "
        f"That covers {leader['season_count']} qualifying seasons for this {role_label}."
    )
    if others:
        runner_text = "; ".join(
            f"{str(row.get('player_name') or row.get('team_name') or 'Unknown')} {row['spread_value']:{query.base_query.metric.formatter}}"
            for row in others
        )
        summary += f" Next on the board: {runner_text}."
    return summary
