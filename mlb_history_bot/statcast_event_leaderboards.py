from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .models import EvidenceSnippet
from .pybaseball_adapter import load_statcast_range
from .query_intent import detect_ranking_intent, mentions_current_scope
from .query_utils import extract_date_window, extract_referenced_season, question_mentions_explicit_year
from .statcast_sync import TEAM_NAMES, iter_sync_chunks, resolve_statcast_sync_windows
from .storage import table_exists


HIT_EVENTS = ("single", "double", "triple", "home_run")
XBH_EVENTS = ("double", "triple", "home_run")
WALK_EVENTS = ("walk", "intent_walk")
STRIKEOUT_EVENTS = ("strikeout", "strikeout_double_play")
RISP_HINTS = (
    " with risp",
    " runners in scoring position",
    " runner in scoring position",
    " with runners in scoring position",
)
PARK_PATTERN = re.compile(r"\b(?:at|in)\s+([A-Z][A-Za-z0-9.'& -]{2,50}?)(?=(?:\s+(?:in|on|through|during|with|to|from|for|of|era)\b|[?.!,]|$))")
PITCH_TYPE_MAP = {
    "fastball": ("4-Seam Fastball", "Sinker", "Cutter", "2-Seam Fastball", "Fastball"),
    "changeup": ("Changeup", "Split-Finger", "Forkball", "Vulcan Change"),
    "curveball": ("Curveball", "Knuckle Curve", "Slow Curve", "Eephus"),
    "slider": ("Slider", "Sweeper", "Slurve"),
}


@dataclass(slots=True)
class StatcastEventMetricSpec:
    key: str
    label: str
    aliases: tuple[str, ...]
    column: str
    raw_key: str
    formatter: str
    unit: str = ""
    higher_is_better: bool = True


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
    aggregation_mode: str
    minimum_events: int | None
    event_filter: tuple[str, ...] | None
    event_label: str | None
    split_key: str | None
    park_phrase: str | None
    direction_filter: str | None
    pitch_family: str | None
    horizontal_location: str | None
    vertical_location: str | None


SUPPORTED_EVENT_METRICS: tuple[StatcastEventMetricSpec, ...] = (
    StatcastEventMetricSpec(
        key="launch_speed",
        label="exit velocity",
        aliases=("exit velocity", "launch speed", "ev"),
        column="launch_speed",
        raw_key="launch_speed",
        formatter=".1f",
        unit="mph",
    ),
    StatcastEventMetricSpec(
        key="hit_distance",
        label="home run distance",
        aliases=("home run distance", "hr distance", "distance"),
        column="hit_distance",
        raw_key="hit_distance_sc",
        formatter=".1f",
        unit="ft",
    ),
    StatcastEventMetricSpec(
        key="launch_angle",
        label="launch angle",
        aliases=("launch angle",),
        column="launch_angle",
        raw_key="launch_angle",
        formatter=".1f",
        unit="deg",
    ),
    StatcastEventMetricSpec(
        key="release_speed",
        label="pitch velocity",
        aliases=("pitch velocity", "pitch speed", "release speed", "velocity"),
        column="release_speed",
        raw_key="release_speed",
        formatter=".1f",
        unit="mph",
    ),
    StatcastEventMetricSpec(
        key="release_spin_rate",
        label="spin rate",
        aliases=("spin rate", "spin"),
        column="release_spin_rate",
        raw_key="release_spin_rate",
        formatter=".1f",
        unit="rpm",
    ),
    StatcastEventMetricSpec(
        key="bat_speed",
        label="bat speed",
        aliases=("bat speed",),
        column="bat_speed",
        raw_key="bat_speed",
        formatter=".1f",
        unit="mph",
    ),
    StatcastEventMetricSpec(
        key="estimated_ba",
        label="xBA",
        aliases=("xba", "expected batting average", "estimated batting average"),
        column="estimated_ba",
        raw_key="estimated_ba_using_speedangle",
        formatter=".3f",
    ),
    StatcastEventMetricSpec(
        key="estimated_woba",
        label="xwOBA",
        aliases=("xwoba", "expected woba", "estimated woba"),
        column="estimated_woba",
        raw_key="estimated_woba_using_speedangle",
        formatter=".3f",
    ),
    StatcastEventMetricSpec(
        key="estimated_slg",
        label="xSLG",
        aliases=("xslg", "expected slugging", "estimated slugging"),
        column="estimated_slg",
        raw_key="estimated_slg_using_speedangle",
        formatter=".3f",
    ),
    StatcastEventMetricSpec(
        key="runs_batted_in",
        label="RBI",
        aliases=("runs batted in", "rbi", "rbis"),
        column="runs_batted_in",
        raw_key="rbi",
        formatter=".0f",
    ),
)


class StatcastEventResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        current_season = self.settings.live_season or date.today().year
        query = parse_statcast_event_query(question, current_season)
        if query is None:
            return None
        park_filter = None
        if query.park_phrase:
            park_filter = resolve_park_home_team(connection, query.park_phrase)
            if park_filter is None:
                return EvidenceSnippet(
                    source="Statcast Event Planner",
                    title=f"{query.metric.label} park filter gap",
                    citation="Lahman park history plus Statcast event planner",
                    summary=(
                        f"I understand this as a filtered Statcast event-set leaderboard for {query.park_phrase}, "
                        "but that park name did not resolve cleanly to a tracked home-team filter."
                    ),
                    payload={
                        "analysis_type": "contextual_source_gap",
                        "metric": query.metric.label,
                        "context": query.park_phrase,
                    },
                )
        rows = scan_statcast_events(connection, self.settings, query, park_filter)
        if not rows:
            return EvidenceSnippet(
                source="Statcast Event Planner",
                title=f"{query.metric.label} event-set gap",
                citation="local compact Statcast event index plus raw Statcast fallback",
                summary=(
                    f"I understand this as a filtered Statcast event-set leaderboard for {query.metric.label} "
                    f"across {query.scope_label}, but the requested event set did not return any tracked rows "
                    "from the local event index or the bounded raw fallback path."
                ),
                payload={
                    "analysis_type": "contextual_source_gap",
                    "metric": query.metric.label,
                    "scope_label": query.scope_label,
                    "event_label": query.event_label,
                },
            )
        mode = determine_event_mode(query, current_season)
        return EvidenceSnippet(
            source="Statcast Event Leaderboards",
            title=f"{query.scope_label} {query.metric.label} leaderboard",
            citation="local compact Statcast event index with optional raw Statcast fallback",
            summary=build_statcast_event_summary(query, rows, park_filter),
            payload={
                "analysis_type": "statcast_event_leaderboard",
                "mode": mode,
                "metric": query.metric.label,
                "scope_label": query.scope_label,
                "aggregate_mode": query.aggregation_mode,
                "event_label": query.event_label,
                "leaders": rows,
                "minimum_events": query.minimum_events,
                "direction_filter": query.direction_filter,
                "park_filter": park_filter,
                "pitch_family": query.pitch_family,
                "horizontal_location": query.horizontal_location,
                "vertical_location": query.vertical_location,
            },
        )


def parse_statcast_event_query(question: str, current_season: int) -> StatcastEventQuery | None:
    lowered = question.lower()
    normalized = normalize_match_text(lowered)
    metric = find_statcast_event_metric(normalized)
    if metric is None:
        return None
    ranking_intent = detect_ranking_intent(
        f" {lowered} ",
        higher_is_better=metric.higher_is_better,
        require_hint=True,
    )
    if ranking_intent is None:
        return None
    event_filter, event_label = detect_event_filter(lowered)
    if event_label is None:
        return None
    split_key = "risp" if any(hint in lowered for hint in RISP_HINTS) else None
    park_phrase = extract_park_phrase(question)
    direction_filter = extract_direction_filter(lowered)
    pitch_family = detect_pitch_family(lowered)
    horizontal_location, vertical_location = extract_location_filters(lowered)
    minimum_events = extract_minimum_events(lowered)
    aggregation_mode = detect_aggregation_mode(lowered, event_label)
    start_season, end_season, start_date, end_date, scope_label = resolve_query_scope(question, current_season)
    return StatcastEventQuery(
        metric=metric,
        descriptor=ranking_intent.descriptor,
        sort_desc=ranking_intent.sort_desc,
        start_season=start_season,
        end_season=end_season,
        start_date=start_date,
        end_date=end_date,
        scope_label=scope_label,
        aggregation_mode=aggregation_mode,
        minimum_events=minimum_events,
        event_filter=event_filter,
        event_label=event_label,
        split_key=split_key,
        park_phrase=park_phrase,
        direction_filter=direction_filter,
        pitch_family=pitch_family,
        horizontal_location=horizontal_location,
        vertical_location=vertical_location,
    )


def normalize_match_text(text: str) -> str:
    return " " + re.sub(r"[^a-z0-9+]+", " ", text.casefold()).strip() + " "


def find_statcast_event_metric(normalized_question: str) -> StatcastEventMetricSpec | None:
    best_match: tuple[int, StatcastEventMetricSpec] | None = None
    for metric in SUPPORTED_EVENT_METRICS:
        for alias in metric.aliases:
            needle = " " + re.sub(r"[^a-z0-9+]+", " ", alias.casefold()).strip() + " "
            if needle not in normalized_question:
                continue
            score = len(needle)
            if best_match is None or score > best_match[0]:
                best_match = (score, metric)
    return best_match[1] if best_match else None


def detect_event_filter(lowered_question: str) -> tuple[tuple[str, ...] | None, str | None]:
    if re.search(r"\b(?:home\s*-?\s*runs?|homeruns?|homers?)\b", lowered_question):
        return ("home_run",), "home runs"
    if "extra-base hits" in lowered_question or "extra base hits" in lowered_question or "xbh" in lowered_question:
        return XBH_EVENTS, "extra-base hits"
    if "base hits" in lowered_question or re.search(r"\bhits?\b", lowered_question):
        return HIT_EVENTS, "hits"
    if re.search(r"\bwalks?\b", lowered_question):
        return WALK_EVENTS, "walks"
    if re.search(r"\bstrikeouts?\b", lowered_question):
        return STRIKEOUT_EVENTS, "strikeouts"
    if re.search(r"\bpitches?\b", lowered_question):
        return None, "pitches"
    return None, None


def detect_aggregation_mode(lowered_question: str, event_label: str) -> str:
    if lowered_question.startswith(("show me", "list", "find")):
        return "events"
    if event_label == "pitches":
        return "events"
    if any(token in lowered_question for token in ("average", "avg", "mean", "career")):
        return "player_avg"
    if re.search(r"\b(?:which player|who has|who had|player with)\b", lowered_question):
        if re.search(r"\b(?:lowest|fewest|shortest|smallest|worst)\b", lowered_question):
            return "player_min"
        return "player_max"
    return "events"


def extract_minimum_events(lowered_question: str) -> int | None:
    patterns = (
        r"\bat least\s+(\d+)\b",
        r"\bminimum(?:\s+of)?\s+(\d+)\b",
    )
    for pattern in patterns:
        match = re.search(pattern, lowered_question)
        if match is not None:
            return int(match.group(1))
    return None


def resolve_query_scope(
    question: str,
    current_season: int,
) -> tuple[int | None, int | None, date | None, date | None, str]:
    lowered = question.lower()
    date_window = extract_date_window(question, current_season)
    if date_window is not None:
        return None, None, date_window.start_date, date_window.end_date, date_window.label
    if "statcast era" in lowered:
        return 2015, current_season, None, None, "Statcast era"
    if "mlb history" in lowered or "history" in lowered or "career" in lowered:
        return 2015, current_season, None, None, "MLB history (Statcast era)"
    referenced_season = extract_referenced_season(question, current_season)
    if referenced_season is not None:
        return referenced_season, referenced_season, None, None, str(referenced_season)
    if mentions_current_scope(lowered) or not question_mentions_explicit_year(question):
        return current_season, current_season, None, None, str(current_season)
    return current_season, current_season, None, None, str(current_season)


def determine_event_mode(query: StatcastEventQuery, current_season: int) -> str:
    if query.scope_label in {"today", "yesterday", "this week", "last week"}:
        return "live"
    if query.start_season == current_season and query.end_season == current_season and query.scope_label not in {"Statcast era", "MLB history (Statcast era)"}:
        return "live"
    return "historical"


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


def detect_pitch_family(lowered_question: str) -> str | None:
    for family in PITCH_TYPE_MAP:
        if family in lowered_question:
            return family
    return None


def extract_location_filters(lowered_question: str) -> tuple[str | None, str | None]:
    horizontal = None
    vertical = None
    if any(token in lowered_question for token in ("middle-middle", "middle middle", "middle/middle")):
        return "middle", "middle"
    if any(token in lowered_question for token in ("in on the hands", "inner-half", "inner half", "up-and-in", "down-and-in")) or re.search(r"\binside\b", lowered_question):
        horizontal = "inside"
    elif any(token in lowered_question for token in ("outer-half", "outer half", "up-and-away", "down-and-away")) or re.search(r"\b(?:outside|away)\b", lowered_question):
        horizontal = "outside"
    elif any(token in lowered_question for token in ("center cut", "down the middle", "heart of the plate")) or re.search(r"\bmiddle\b", lowered_question):
        horizontal = "middle"
    if any(token in lowered_question for token in ("up in the zone", "elevated", "upper third", "top of the zone", "up-and-in", "up-and-away")) or re.search(r"\b(?:high|up)\b", lowered_question):
        vertical = "high"
    elif any(token in lowered_question for token in ("bottom of the zone", "lower third", "down-and-in", "down-and-away")) or re.search(r"\b(?:low|down)\b", lowered_question):
        vertical = "low"
    elif any(token in lowered_question for token in ("belt high", "mid-zone")) or re.search(r"\bmiddle\b", lowered_question):
        vertical = "middle"
    return horizontal, vertical


def scan_statcast_events(connection, settings: Settings, query: StatcastEventQuery, park_filter: str | None) -> list[dict[str, Any]]:
    local_rows = scan_local_statcast_events(connection, query, park_filter)
    if local_rows:
        return local_rows
    if local_rows == [] and not should_raw_fallback(query):
        return []
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
    if query.aggregation_mode == "events":
        rows.sort(key=lambda row: sortable_metric_value(row.get("metric_value")), reverse=query.sort_desc)
        rows = rows[:20]
    else:
        rows = aggregate_player_rows(rows, query)
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def should_raw_fallback(query: StatcastEventQuery) -> bool:
    if query.start_date is not None and query.end_date is not None:
        return (query.end_date - query.start_date).days <= 7
    if query.start_season is not None and query.end_season is not None:
        return query.start_season == query.end_season
    return False


def scan_local_statcast_events(connection, query: StatcastEventQuery, park_filter: str | None) -> list[dict[str, Any]] | None:
    if not table_exists(connection, "statcast_events"):
        return None
    metric_column = query.metric.column
    where_clause, params = build_local_event_filters(query, park_filter, metric_column)
    if query.aggregation_mode == "events":
        rows = connection.execute(
            f"""
            SELECT
                batter_name AS player_name,
                pitcher_name,
                game_date,
                away_team || ' @ ' || home_team AS team_matchup,
                event,
                pitch_name,
                pitch_family,
                count_key,
                field_direction,
                horizontal_location,
                vertical_location,
                launch_speed AS exit_velocity,
                hit_distance,
                launch_angle,
                release_speed,
                release_spin_rate,
                bat_speed,
                estimated_ba,
                estimated_woba,
                estimated_slg,
                runs_batted_in,
                {metric_column} AS metric_value
            FROM statcast_events
            WHERE {where_clause}
            ORDER BY {metric_column} {"DESC" if query.sort_desc else "ASC"}, game_date DESC
            LIMIT 20
            """,
            params,
        ).fetchall()
        result = [dict(row) for row in rows]
        for index, row in enumerate(result, start=1):
            row["rank"] = index
        return result
    aggregate_rows = connection.execute(
        f"""
        SELECT
            batter_name AS player_name,
            COUNT(*) AS event_count,
            AVG({metric_column}) AS avg_metric_value,
            MAX({metric_column}) AS max_metric_value,
            MIN({metric_column}) AS min_metric_value,
            MAX(game_date) AS latest_date,
            SUM(runs_batted_in) AS runs_batted_in,
            SUM(is_home_run) AS home_runs
        FROM statcast_events
        WHERE {where_clause}
        GROUP BY batter_id, batter_name
        HAVING COUNT(*) >= ?
        ORDER BY {aggregate_metric_expression(query)} {"DESC" if query.sort_desc else "ASC"}, COUNT(*) DESC, player_name ASC
        LIMIT 20
        """,
        [*params, query.minimum_events or 1],
    ).fetchall()
    result = []
    for index, row in enumerate(aggregate_rows, start=1):
        item = dict(row)
        item["metric_value"] = select_aggregate_metric_value(item, query.aggregation_mode)
        item["rank"] = index
        result.append(item)
    return result


def build_local_event_filters(query: StatcastEventQuery, park_filter: str | None, metric_column: str) -> tuple[str, list[Any]]:
    clauses: list[str] = [f"{metric_column} IS NOT NULL"]
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
        placeholders = ", ".join("?" for _ in query.event_filter)
        clauses.append(f"event IN ({placeholders})")
        params.extend(query.event_filter)
    if query.split_key == "risp":
        clauses.append("has_risp = 1")
    if query.direction_filter:
        clauses.append("field_direction = ?")
        params.append(query.direction_filter)
    if query.pitch_family:
        clauses.append("pitch_family = ?")
        params.append(query.pitch_family)
    if query.horizontal_location:
        clauses.append("horizontal_location = ?")
        params.append(query.horizontal_location)
    if query.vertical_location:
        clauses.append("vertical_location = ?")
        params.append(query.vertical_location)
    if park_filter:
        clauses.append("home_team = ?")
        params.append(park_filter)
    return " AND ".join(clauses), params


def aggregate_metric_expression(query: StatcastEventQuery) -> str:
    if query.aggregation_mode == "player_avg":
        return f"AVG({query.metric.column})"
    if query.aggregation_mode == "player_min":
        return f"MIN({query.metric.column})"
    return f"MAX({query.metric.column})"


def select_aggregate_metric_value(row: dict[str, Any], aggregation_mode: str) -> float | None:
    if aggregation_mode == "player_avg":
        return safe_float(row.get("avg_metric_value"))
    if aggregation_mode == "player_min":
        return safe_float(row.get("min_metric_value"))
    return safe_float(row.get("max_metric_value"))


def resolve_event_windows(settings: Settings, query: StatcastEventQuery):
    if query.start_date is not None and query.end_date is not None:
        return [type("Window", (), {"start_date": query.start_date, "end_date": query.end_date})]
    return resolve_statcast_sync_windows(settings, start_season=query.start_season, end_season=query.end_season)


def event_to_result_row(item: dict[str, Any], query: StatcastEventQuery, park_filter: str | None) -> dict[str, Any] | None:
    if str(item.get("game_type") or "R") not in {"R", ""}:
        return None
    event_name = str(item.get("events") or "").strip().lower()
    if query.event_filter and event_name not in query.event_filter:
        return None
    if park_filter and str(item.get("home_team") or "") != park_filter:
        return None
    pitch_name = str(item.get("pitch_name") or "").strip()
    if query.pitch_family and not pitch_matches_family(pitch_name, query.pitch_family):
        return None
    if query.split_key == "risp" and not has_risp(item):
        return None
    field_direction = extract_field_direction(str(item.get("des") or item.get("description") or ""))
    if query.direction_filter and field_direction != query.direction_filter:
        return None
    if not pitch_location_matches(item, query.horizontal_location, query.vertical_location):
        return None
    metric_value = safe_float(item.get(query.metric.raw_key))
    if metric_value is None:
        return None
    return {
        "player_name": extract_batter_name(item),
        "pitcher_name": format_pitcher_name(str(item.get("player_name") or "")),
        "game_date": str(item.get("game_date") or ""),
        "team_matchup": f"{item.get('away_team') or ''} @ {item.get('home_team') or ''}".strip(),
        "event": event_name,
        "pitch_name": pitch_name,
        "pitch_family": query.pitch_family or detect_pitch_family(pitch_name.casefold() if pitch_name else ""),
        "count_key": count_key_from_row(item),
        "field_direction": field_direction,
        "horizontal_location": classify_horizontal_location(item),
        "vertical_location": classify_vertical_location(item),
        "exit_velocity": safe_float(item.get("launch_speed")),
        "hit_distance": safe_float(item.get("hit_distance_sc")),
        "launch_angle": safe_float(item.get("launch_angle")),
        "release_speed": safe_float(item.get("release_speed")),
        "release_spin_rate": safe_float(item.get("release_spin_rate")),
        "bat_speed": safe_float(item.get("bat_speed")),
        "estimated_ba": safe_float(item.get("estimated_ba_using_speedangle")),
        "estimated_woba": safe_float(item.get("estimated_woba_using_speedangle")),
        "estimated_slg": safe_float(item.get("estimated_slg_using_speedangle")),
        "runs_batted_in": safe_int(item.get("rbi")) or 0,
        "metric_value": metric_value,
    }


def aggregate_player_rows(rows: list[dict[str, Any]], query: StatcastEventQuery) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        player_name = str(row.get("player_name") or "").strip()
        if not player_name:
            continue
        current = grouped.setdefault(
            player_name,
            {
                "player_name": player_name,
                "event_count": 0,
                "avg_metric_value": 0.0,
                "max_metric_value": None,
                "min_metric_value": None,
                "latest_date": "",
                "runs_batted_in": 0,
                "home_runs": 0,
            },
        )
        metric_value = safe_float(row.get("metric_value"))
        if metric_value is None:
            continue
        current["event_count"] += 1
        current["avg_metric_value"] += metric_value
        current["max_metric_value"] = metric_value if current["max_metric_value"] is None else max(current["max_metric_value"], metric_value)
        current["min_metric_value"] = metric_value if current["min_metric_value"] is None else min(current["min_metric_value"], metric_value)
        current["runs_batted_in"] += safe_int(row.get("runs_batted_in")) or 0
        if row.get("event") == "home_run":
            current["home_runs"] += 1
        game_date = str(row.get("game_date") or "")
        if game_date > str(current["latest_date"] or ""):
            current["latest_date"] = game_date
    ranked: list[dict[str, Any]] = []
    minimum_events = query.minimum_events or 1
    for item in grouped.values():
        event_count = int(item["event_count"])
        if event_count < minimum_events:
            continue
        item["avg_metric_value"] = item["avg_metric_value"] / event_count if event_count else None
        item["metric_value"] = select_aggregate_metric_value(item, query.aggregation_mode)
        ranked.append(item)
    ranked.sort(
        key=lambda row: (
            -sortable_metric_value(row.get("metric_value")) if query.sort_desc else sortable_metric_value(row.get("metric_value")),
            -(safe_int(row.get("event_count")) or 0),
            str(row.get("player_name") or ""),
        )
    )
    ranked = ranked[:20]
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    return ranked


def build_statcast_event_summary(query: StatcastEventQuery, rows: list[dict[str, Any]], park_filter: str | None) -> str:
    lead = rows[0]
    metric_value = format_metric_value(query.metric, lead.get("metric_value"))
    filter_bits: list[str] = []
    if query.event_label:
        filter_bits.append(query.event_label)
    if query.pitch_family:
        filter_bits.append(f"{query.pitch_family}s")
    if query.split_key == "risp":
        filter_bits.append("with RISP")
    location_label = format_location_label(query.horizontal_location, query.vertical_location)
    if location_label:
        filter_bits.append(location_label)
    if query.direction_filter:
        filter_bits.append(query.direction_filter)
    filter_label = " ".join(filter_bits).strip() or "events"
    if query.aggregation_mode == "events":
        summary = (
            f"Across {query.scope_label}, the {query.descriptor} {query.metric.label} {filter_label} I found "
            f"were led by {lead.get('player_name')} at {metric_value}"
        )
        if lead.get("game_date"):
            summary = f"{summary} on {lead['game_date']}"
        summary = f"{summary}."
    else:
        qualifier_text = f" with at least {query.minimum_events} {query.event_label}" if query.minimum_events else ""
        summary = (
            f"Across {query.scope_label}, the {query.descriptor} player by "
            f"{aggregation_label(query.aggregation_mode)} {query.metric.label} on {filter_label}{qualifier_text} "
            f"is {lead.get('player_name')} at {metric_value} across {lead.get('event_count')} event(s)."
        )
    trailing = rows[1:4]
    if trailing:
        if query.aggregation_mode == "events":
            board = "; ".join(
                f"{row.get('player_name')} {format_metric_value(query.metric, row.get('metric_value'))} ({row.get('game_date')})"
                for row in trailing
            )
        else:
            board = "; ".join(
                f"{row.get('player_name')} {format_metric_value(query.metric, row.get('metric_value'))} ({row.get('event_count')} event(s))"
                for row in trailing
            )
        summary = f"{summary} Next on the board: {board}."
    if park_filter:
        summary = f"{summary} Park filter resolved to home-team code {park_filter}."
    return summary


def aggregation_label(mode: str) -> str:
    if mode == "player_avg":
        return "average"
    if mode == "player_min":
        return "lowest single-event"
    return "highest single-event"


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


def pitch_matches_family(pitch_name: str, family: str) -> bool:
    options = PITCH_TYPE_MAP.get(family, ())
    lowered_pitch = pitch_name.casefold()
    return any(option.casefold() in lowered_pitch for option in options)


def pitch_location_matches(row: dict[str, Any], horizontal_location: str | None, vertical_location: str | None) -> bool:
    if horizontal_location is None and vertical_location is None:
        return True
    plate_x = safe_float(row.get("plate_x"))
    plate_z = safe_float(row.get("plate_z"))
    stand = str(row.get("stand") or "").upper()
    if horizontal_location:
        if plate_x is None or stand not in {"R", "L"}:
            return False
        if horizontal_location == "inside" and not ((stand == "R" and plate_x <= -0.35) or (stand == "L" and plate_x >= 0.35)):
            return False
        if horizontal_location == "outside" and not ((stand == "R" and plate_x >= 0.35) or (stand == "L" and plate_x <= -0.35)):
            return False
        if horizontal_location == "middle" and not (abs(plate_x) < 0.35):
            return False
    if vertical_location:
        if plate_z is None:
            return False
        sz_top = safe_float(row.get("sz_top"))
        sz_bot = safe_float(row.get("sz_bot"))
        if sz_top is not None and sz_bot is not None and sz_top > sz_bot:
            zone_height = sz_top - sz_bot
            upper_cut = sz_bot + zone_height * 0.67
            lower_cut = sz_bot + zone_height * 0.33
            if vertical_location == "high" and not (plate_z >= upper_cut):
                return False
            if vertical_location == "low" and not (plate_z <= lower_cut):
                return False
            if vertical_location == "middle" and not (lower_cut < plate_z < upper_cut):
                return False
        else:
            if vertical_location == "high" and not (plate_z >= 3.0):
                return False
            if vertical_location == "low" and not (plate_z <= 2.0):
                return False
            if vertical_location == "middle" and not (2.0 < plate_z < 3.0):
                return False
    return True


def extract_field_direction(value: str) -> str:
    lowered = str(value or "").casefold()
    if "right-center" in lowered or "right center" in lowered:
        return "right center"
    if "left-center" in lowered or "left center" in lowered:
        return "left center"
    if "to right field" in lowered or "right field" in lowered:
        return "right field"
    if "to left field" in lowered or "left field" in lowered:
        return "left field"
    if "to center field" in lowered or "center field" in lowered:
        return "center field"
    return ""


def extract_batter_name(item: dict[str, Any]) -> str:
    description = str(item.get("des") or item.get("description") or "").strip()
    match = re.match(
        r"(.+?)\s+(?:singles|doubles|triples|homers|hits a home run|hits a grand slam|walks|grounds out|flies out|lines out|pops out|strikes out|reaches)\b",
        description,
        re.IGNORECASE,
    )
    if match is None:
        return ""
    return " ".join(part.capitalize() for part in match.group(1).split())


def format_pitcher_name(value: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned or "," not in cleaned:
        return cleaned
    last_name, first_name = [part.strip() for part in cleaned.split(",", 1)]
    return f"{first_name} {last_name}".strip()


def count_key_from_row(row: dict[str, Any]) -> str:
    balls = safe_int(row.get("balls"))
    strikes = safe_int(row.get("strikes"))
    if balls is None or strikes is None:
        return ""
    return f"{balls}-{strikes}"


def classify_horizontal_location(row: dict[str, Any]) -> str:
    plate_x = safe_float(row.get("plate_x"))
    stand = str(row.get("stand") or "").upper()
    if plate_x is None or stand not in {"R", "L"}:
        return ""
    if (stand == "R" and plate_x <= -0.35) or (stand == "L" and plate_x >= 0.35):
        return "inside"
    if (stand == "R" and plate_x >= 0.35) or (stand == "L" and plate_x <= -0.35):
        return "outside"
    return "middle"


def classify_vertical_location(row: dict[str, Any]) -> str:
    plate_z = safe_float(row.get("plate_z"))
    if plate_z is None:
        return ""
    sz_top = safe_float(row.get("sz_top"))
    sz_bot = safe_float(row.get("sz_bot"))
    if sz_top is not None and sz_bot is not None and sz_top > sz_bot:
        zone_height = sz_top - sz_bot
        upper_cut = sz_bot + zone_height * 0.67
        lower_cut = sz_bot + zone_height * 0.33
        if plate_z >= upper_cut:
            return "high"
        if plate_z <= lower_cut:
            return "low"
        return "middle"
    if plate_z >= 3.0:
        return "high"
    if plate_z <= 2.0:
        return "low"
    return "middle"


def has_risp(item: dict[str, Any]) -> bool:
    return has_base_runner(item.get("on_2b")) or has_base_runner(item.get("on_3b"))


def has_base_runner(value: Any) -> bool:
    if value in (None, "", 0, "0"):
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    return True


def sortable_metric_value(value: Any) -> float:
    numeric = safe_float(value)
    return numeric if numeric is not None else float("-inf")


def format_metric_value(metric: StatcastEventMetricSpec, value: Any) -> str:
    numeric = safe_float(value)
    if numeric is None:
        return "n/a"
    formatted = f"{numeric:{metric.formatter}}"
    return f"{formatted} {metric.unit}".strip()


def format_location_label(horizontal_location: str | None, vertical_location: str | None) -> str | None:
    if horizontal_location == "middle" and vertical_location == "middle":
        return "middle-middle"
    parts = []
    if vertical_location:
        parts.append(vertical_location)
    if horizontal_location:
        parts.append(horizontal_location)
    return " ".join(parts) if parts else None


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


def safe_int(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
