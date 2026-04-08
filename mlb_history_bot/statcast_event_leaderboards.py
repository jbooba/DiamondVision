from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .metrics import MetricCatalog
from .models import EvidenceSnippet
from .pybaseball_adapter import load_statcast_range
from .query_intent import detect_ranking_intent, mentions_current_scope
from .query_utils import extract_date_window, extract_referenced_season, question_mentions_explicit_year
from .statcast_sync import TEAM_NAMES, iter_sync_chunks, resolve_statcast_sync_windows
from .storage import table_exists


RISP_HINTS = (
    " with risp",
    " runners in scoring position",
    " runner in scoring position",
    " with runners in scoring position",
)
BATTER_NAME_PATTERN = re.compile(
    r"^(?P<name>.+?)\s+(?:homers|hits a home run|hits a grand slam|singles|doubles|triples|walks|grounds out|flies out|lines out|pops out|strikes out|reaches)",
    re.IGNORECASE,
)
PARK_PATTERN = re.compile(r"\b(?:at|in)\s+([A-Z][A-Za-z0-9.'& -]{2,50}?)(?=(?:\s+(?:in|on|through|during|with|to|from|for|of|era)\b|[?.!,]|$))")


@dataclass(slots=True)
class StatcastEventMetricSpec:
    key: str
    label: str
    aliases: tuple[str, ...]
    formatter: str


@dataclass(slots=True)
class StatcastEventQuery:
    metric: StatcastEventMetricSpec
    descriptor: str
    sort_desc: bool
    start_season: int | None
    end_season: int | None
    start_date: date | None
    end_date: date | None
    scope_label: str
    wants_player_aggregation: bool
    event_filter: str | None
    split_key: str | None
    park_phrase: str | None
    direction_filter: str | None


SUPPORTED_EVENT_METRICS: tuple[StatcastEventMetricSpec, ...] = (
    StatcastEventMetricSpec(
        key="launch_speed",
        label="exit velocity",
        aliases=("exit velocity", "launch speed", " ev ", "highest ev"),
        formatter=".1f",
    ),
    StatcastEventMetricSpec(
        key="bat_speed",
        label="bat speed",
        aliases=("bat speed",),
        formatter=".1f",
    ),
)


class StatcastEventResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.catalog = MetricCatalog.load(settings.project_root)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        current_season = self.settings.live_season or date.today().year
        query = parse_statcast_event_query(question, current_season)
        if query is None:
            return None
        mode = (
            "live"
            if query.start_date is not None
            or (query.start_season == current_season and query.end_season == current_season and query.scope_label != "Statcast era")
            else "historical"
        )
        park_filter = None
        if query.park_phrase:
            park_filter = resolve_park_home_team(connection, query.park_phrase)
            if park_filter is None:
                return EvidenceSnippet(
                    source="Statcast Event Planner",
                    title=f"{query.metric.label} park filter gap",
                    citation="Lahman team park history plus Statcast event planner",
                    summary=(
                        f"I understand this as a Statcast event leaderboard filtered to {query.park_phrase}, "
                        "but that park name did not resolve cleanly to a tracked home-team filter. "
                        "The leaderboard planner should not answer a park-filtered question without matching the park first."
                    ),
                    payload={
                        "analysis_type": "contextual_source_gap",
                        "metric": query.metric.label,
                        "context": query.park_phrase,
                    },
                )
        rows = scan_statcast_events(self.settings, query, park_filter)
        if not rows:
            return None
        summary = build_statcast_event_summary(query, rows, park_filter)
        return EvidenceSnippet(
            source="Statcast Event Leaderboards",
            title=f"{query.scope_label} {query.metric.label} leaderboard",
            citation="pybaseball raw Statcast event feed filtered by query context",
            summary=summary,
            payload={
                "analysis_type": "statcast_event_leaderboard",
                "mode": mode,
                "metric": query.metric.label,
                "scope_label": query.scope_label,
                "descriptor": query.descriptor,
                "leaders": rows,
                "aggregate_mode": "player" if query.wants_player_aggregation else "events",
                "split_key": query.split_key,
                "direction_filter": query.direction_filter,
                "park_filter": park_filter,
            },
        )


def parse_statcast_event_query(question: str, current_season: int) -> StatcastEventQuery | None:
    lowered = f" {question.lower()} "
    metric = find_statcast_event_metric(lowered)
    if metric is None:
        return None
    ranking_intent = detect_ranking_intent(lowered, higher_is_better=True, require_hint=True)
    if ranking_intent is None:
        return None
    descriptor = ranking_intent.descriptor
    sort_desc = ranking_intent.sort_desc
    date_window = extract_date_window(question, current_season)
    start_season, end_season, start_date, end_date, scope_label = resolve_query_scope(question, current_season, date_window)
    event_filter = "home_run" if any(token in lowered for token in ("home run", "home runs", "homer", "homers")) else None
    split_key = "risp" if any(hint in lowered for hint in RISP_HINTS) else None
    direction_filter = extract_direction_filter(lowered)
    park_phrase = extract_park_phrase(question)
    wants_player_aggregation = metric.key == "bat_speed" or lowered.startswith((" who ", " which player "))
    return StatcastEventQuery(
        metric=metric,
        descriptor=descriptor,
        sort_desc=sort_desc,
        start_season=start_season,
        end_season=end_season,
        start_date=start_date,
        end_date=end_date,
        scope_label=scope_label,
        wants_player_aggregation=wants_player_aggregation,
        event_filter=event_filter,
        split_key=split_key,
        park_phrase=park_phrase,
        direction_filter=direction_filter,
    )


def find_statcast_event_metric(lowered_question: str) -> StatcastEventMetricSpec | None:
    best_match: tuple[int, StatcastEventMetricSpec] | None = None
    for metric in SUPPORTED_EVENT_METRICS:
        for alias in metric.aliases:
            alias_text = alias if alias.startswith(" ") else f" {alias} "
            if alias_text not in lowered_question:
                continue
            score = len(alias.strip())
            if best_match is None or score > best_match[0]:
                best_match = (score, metric)
    return best_match[1] if best_match else None


def resolve_query_scope(
    question: str,
    current_season: int,
    date_window,
) -> tuple[int | None, int | None, date | None, date | None, str]:
    lowered = question.lower()
    if "statcast era" in lowered:
        return 2015, current_season, None, None, "Statcast era"
    if date_window is not None:
        return None, None, date_window.start_date, date_window.end_date, date_window.label
    referenced_season = extract_referenced_season(question, current_season)
    if referenced_season is not None:
        season = referenced_season
        return season, season, None, None, str(season)
    if mentions_current_scope(lowered) or not question_mentions_explicit_year(question):
        return current_season, current_season, None, None, str(current_season)
    return current_season, current_season, None, None, str(current_season)


def extract_direction_filter(lowered_question: str) -> str | None:
    if "to right field" in lowered_question or "right field" in lowered_question:
        return "right field"
    if "to left field" in lowered_question or "left field" in lowered_question:
        return "left field"
    if "to center field" in lowered_question or "center field" in lowered_question:
        return "center field"
    return None


def extract_park_phrase(question: str) -> str | None:
    match = PARK_PATTERN.search(question)
    if match is None:
        return None
    value = match.group(1).strip()
    if value.lower() in {"mlb", "baseball", "statcast era"}:
        return None
    return value


def resolve_park_home_team(connection, park_phrase: str) -> str | None:
    lowered = park_phrase.casefold()
    rows = connection.execute(
        """
        SELECT name, park
        FROM lahman_teams
        WHERE lower(COALESCE(park, '')) = ?
           OR lower(COALESCE(park, '')) LIKE ?
        ORDER BY CAST(yearid AS INTEGER) DESC
        LIMIT 10
        """,
        (lowered, f"%{lowered}%"),
    ).fetchall()
    if rows:
        team_name = str(rows[0]["name"])
        return team_name_to_statcast_code(team_name)
    rows = connection.execute(
        """
        SELECT teams.name
        FROM lahman_parks AS parks
        JOIN lahman_homegames AS homegames
          ON homegames.parkkey = parks.parkkey
        JOIN lahman_teams AS teams
          ON teams.yearid = homegames.yearkey
         AND (
             teams.teamid = homegames.teamkey
             OR teams.teamidretro = homegames.teamkey
             OR teams.teamidbr = homegames.teamkey
         )
        WHERE lower(COALESCE(parks.parkname, '')) = ?
           OR lower(COALESCE(parks.parkalias, '')) LIKE ?
        ORDER BY CAST(homegames.yearkey AS INTEGER) DESC
        LIMIT 10
        """,
        (lowered, f"%{lowered}%"),
    ).fetchall()
    if not rows:
        return None
    return team_name_to_statcast_code(str(rows[0]["name"]))


def team_name_to_statcast_code(team_name: str) -> str | None:
    inverse: dict[str, str] = {}
    for code, name in TEAM_NAMES.items():
        previous = inverse.get(name)
        if previous is None or len(code) < len(previous):
            inverse[name] = code
    return inverse.get(team_name)


def scan_statcast_events(settings: Settings, query: StatcastEventQuery, park_filter: str | None) -> list[dict[str, Any]]:
    local_rows = scan_local_statcast_events(settings, query, park_filter)
    if local_rows is not None:
        return local_rows
    rows: list[dict[str, Any]] = []
    windows = resolve_event_windows(settings, query)
    for window in windows:
        for chunk_start, chunk_end in iter_sync_chunks(window.start_date, window.end_date, 21):
            chunk_rows = load_statcast_range(chunk_start.isoformat(), chunk_end.isoformat())
            for item in chunk_rows:
                event_row = event_to_result_row(item, query, park_filter)
                if event_row is not None:
                    rows.append(event_row)
    if not rows:
        return []
    if query.wants_player_aggregation:
        rows = aggregate_player_rows(rows, query.metric.key)
    rows.sort(key=lambda row: sortable_metric_value(row["metric_value"]), reverse=query.sort_desc)
    return rows[:5]


def scan_local_statcast_events(settings: Settings, query: StatcastEventQuery, park_filter: str | None) -> list[dict[str, Any]] | None:
    import sqlite3

    connection = sqlite3.connect(settings.database_path)
    connection.row_factory = sqlite3.Row
    try:
        if not table_exists(connection, "statcast_events"):
            return None
        where_clause, params = build_local_event_filters(query, park_filter)
        rows = connection.execute(
            f"""
            SELECT
                batter_name AS player_name,
                game_date,
                away_team || ' @ ' || home_team AS team_matchup,
                event,
                home_team,
                away_team,
                hit_distance,
                launch_angle,
                bat_speed,
                launch_speed AS exit_velocity,
                CASE
                    WHEN ? = 'launch_speed' THEN launch_speed
                    WHEN ? = 'bat_speed' THEN bat_speed
                    ELSE NULL
                END AS metric_value
            FROM statcast_events
            WHERE {where_clause}
            """,
            (query.metric.key, query.metric.key, *params),
        ).fetchall()
    finally:
        connection.close()
    filtered = [dict(row) for row in rows if row["metric_value"] is not None]
    if not filtered:
        return []
    if query.wants_player_aggregation:
        filtered = aggregate_player_rows(filtered, query.metric.key)
    filtered.sort(key=lambda row: sortable_metric_value(row["metric_value"]), reverse=query.sort_desc)
    return filtered[:5]


def build_local_event_filters(query: StatcastEventQuery, park_filter: str | None) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if query.start_date is not None and query.end_date is not None:
        clauses.append("game_date >= ? AND game_date <= ?")
        params.extend([query.start_date.isoformat(), query.end_date.isoformat()])
    else:
        if query.start_season is not None:
            clauses.append("season >= ?")
            params.append(query.start_season)
        if query.end_season is not None:
            clauses.append("season <= ?")
            params.append(query.end_season)
    if query.event_filter:
        clauses.append("event = ?")
        params.append(query.event_filter)
    if query.split_key == "risp":
        clauses.append("has_risp = 1")
    if query.direction_filter:
        clauses.append("field_direction = ?")
        params.append(query.direction_filter)
    if park_filter:
        clauses.append("home_team = ?")
        params.append(park_filter)
    metric_column = "launch_speed" if query.metric.key == "launch_speed" else "bat_speed"
    clauses.append(f"{metric_column} IS NOT NULL")
    return (" AND ".join(clauses) if clauses else "1=1"), params


def resolve_event_windows(settings: Settings, query: StatcastEventQuery):
    if query.start_date is not None and query.end_date is not None:
        return [type("Window", (), {"start_date": query.start_date, "end_date": query.end_date})]
    return resolve_statcast_sync_windows(
        settings,
        start_season=query.start_season,
        end_season=query.end_season,
    )


def event_to_result_row(item: dict[str, Any], query: StatcastEventQuery, park_filter: str | None) -> dict[str, Any] | None:
    if query.event_filter and str(item.get("events") or "") != query.event_filter:
        return None
    if str(item.get("game_type") or "R") not in {"R", ""}:
        return None
    if park_filter and str(item.get("home_team") or "") != park_filter:
        return None
    if query.direction_filter:
        description = str(item.get("des") or item.get("description") or "").lower()
        if query.direction_filter not in description:
            return None
    if query.split_key == "risp" and not has_risp(item):
        return None
    metric_value = safe_float(item.get(query.metric.key))
    if metric_value is None:
        return None
    batter_name = extract_batter_name(item)
    if not batter_name:
        return None
    description = str(item.get("des") or item.get("description") or "").strip()
    return {
        "player_name": batter_name,
        "metric_value": metric_value,
        "game_date": str(item.get("game_date") or ""),
        "team_matchup": f"{item.get('away_team') or ''} @ {item.get('home_team') or ''}".strip(),
        "event": str(item.get("events") or ""),
        "description": description,
        "home_team": str(item.get("home_team") or ""),
        "away_team": str(item.get("away_team") or ""),
        "hit_distance": safe_float(item.get("hit_distance_sc")),
        "launch_angle": safe_float(item.get("launch_angle")),
        "bat_speed": safe_float(item.get("bat_speed")),
        "exit_velocity": safe_float(item.get("launch_speed")),
    }


def aggregate_player_rows(rows: list[dict[str, Any]], metric_key: str) -> list[dict[str, Any]]:
    best_by_player: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row["player_name"])
        current = best_by_player.get(name)
        if current is None or sortable_metric_value(row["metric_value"]) > sortable_metric_value(current["metric_value"]):
            best_by_player[name] = row
    return list(best_by_player.values())


def build_statcast_event_summary(
    query: StatcastEventQuery,
    rows: list[dict[str, Any]],
    park_filter: str | None,
) -> str:
    lead = rows[0]
    if query.wants_player_aggregation:
        summary = (
            f"Across {query.scope_label} tracked Statcast events"
            f"{' with RISP' if query.split_key == 'risp' else ''}, "
            f"{lead['player_name']} has the {query.descriptor} recorded {query.metric.label} at "
            f"{format_metric_value(lead['metric_value'])}."
        )
    else:
        summary = (
            f"Across {query.scope_label} Statcast events, the {query.descriptor} {query.metric.label} match I found "
            f"was {lead['player_name']} on {lead['game_date']} at {format_metric_value(lead['metric_value'])}."
        )
    if query.event_filter == "home_run":
        summary = f"{summary} The event was a home run."
    if query.direction_filter:
        summary = f"{summary} Filter: {query.direction_filter}."
    if park_filter:
        summary = f"{summary} Park filter resolved to home-team code {park_filter}."
    trailing = rows[1:4]
    if trailing:
        next_rows = "; ".join(
            f"{row['player_name']} {format_metric_value(row['metric_value'])} ({row['game_date']})"
            for row in trailing
        )
        summary = f"{summary} Next on the board: {next_rows}."
    if query.metric.key == "bat_speed":
        summary = f"{summary} This treats the leaderboard as peak single-event bat speed, not season-average bat speed."
    return summary


def extract_batter_name(item: dict[str, Any]) -> str | None:
    description = str(item.get("des") or item.get("description") or "").strip()
    if not description:
        return None
    match = BATTER_NAME_PATTERN.match(description)
    if match is not None:
        return match.group("name").strip()
    return None


def has_risp(item: dict[str, Any]) -> bool:
    return has_base_runner(item.get("on_2b")) or has_base_runner(item.get("on_3b"))


def has_base_runner(value: Any) -> bool:
    if value in (None, "", 0, "0"):
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    return True


def safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric):
        return None
    return numeric


def sortable_metric_value(value: Any) -> float:
    numeric = safe_float(value)
    return numeric if numeric is not None else float("-inf")


def format_metric_value(value: Any) -> str:
    numeric = safe_float(value)
    if numeric is None:
        return "n/a"
    return f"{numeric:.1f}"
