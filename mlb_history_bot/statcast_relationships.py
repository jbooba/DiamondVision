from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .pybaseball_adapter import (
    load_statcast_pitcher,
    load_statcast_pitcher_pitch_arsenal,
    load_statcast_range,
)
from .query_utils import extract_date_window, extract_referenced_season
from .sporty_research import extract_play_id
from .sporty_video import SportyVideoClient
from .statcast_sync import iter_sync_chunks, resolve_statcast_sync_windows
from .storage import get_connection, get_metadata_value, set_metadata_value, table_exists


COUNT_HINTS = ("who has", "which pitcher has", "who threw", "who has thrown")
SHOW_HINTS = ("show me", "find", "list")
VISUAL_HINTS = ("clip", "clips", "video", "videos", "replay", "replays", "highlight", "highlights", "watch")
HIT_EVENTS = {"single", "double", "triple", "home_run"}
STRIKEOUT_EVENTS = {"strikeout", "strikeout_double_play"}
PITCH_TYPE_MAP = {
    "fastball": ("4-Seam Fastball", "Sinker", "Cutter", "2-Seam Fastball", "Fastball"),
    "changeup": ("Changeup", "Split-Finger", "Forkball", "Vulcan Change"),
    "curveball": ("Curveball", "Knuckle Curve", "Slow Curve", "Eephus"),
    "slider": ("Slider", "Sweeper", "Slurve"),
}
FAST_VELOCITY_SHORTLIST_LIMIT = 12
FAST_VELOCITY_MIN_THRESHOLD = 99.0
FAST_VELOCITY_MARGIN = 1.0
FAST_VELOCITY_COLUMNS = ("ff_avg_speed", "si_avg_speed", "fc_avg_speed", "fs_avg_speed")


@dataclass(slots=True)
class StatcastRelationshipQuery:
    mode: str
    start_season: int | None
    end_season: int | None
    start_date: date | None
    end_date: date | None
    scope_label: str
    pitch_family: str | None
    event_filter: set[str] | None
    metric_threshold_key: str | None
    metric_threshold_value: float | None
    batter_filter: str | None
    pitcher_filter: str | None
    horizontal_location: str | None
    vertical_location: str | None
    aggregate_by: str | None
    wants_visuals: bool


class StatcastRelationshipResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)
        self.sporty_video_client = SportyVideoClient(settings)

    def build_snippet(self, question: str) -> EvidenceSnippet | None:
        current_season = self.settings.live_season or date.today().year
        query = parse_statcast_relationship_query(question, current_season)
        if query is None:
            return None
        rows = load_cached_relationship_rows(self.settings, query)
        if rows is None:
            rows = run_statcast_relationship_query(self.settings, query, live_client=self.live_client)
            if rows:
                store_cached_relationship_rows(self.settings, query, rows)
        if not rows:
            return None
        clips = (
            build_statcast_relationship_clips(rows, self.live_client, self.sporty_video_client)
            if query.mode == "events" and query.wants_visuals
            else []
        )
        summary = build_statcast_relationship_summary(query, rows, clips)
        analysis_type = "statcast_relationship_aggregate" if query.aggregate_by else "statcast_relationship_events"
        return EvidenceSnippet(
            source="Statcast Relationships",
            title=f"{query.scope_label} Statcast relationship query",
            citation="pybaseball raw Statcast event feed filtered by pitch, outcome, player, and threshold context",
            summary=summary,
            payload={
                "analysis_type": analysis_type,
                "mode": "live" if query.scope_label in {str(current_season), "today", "yesterday", "this week", "last week"} else "historical",
                "scope_label": query.scope_label,
                "rows": rows,
                "clips": clips,
                "clip_count": len(clips),
                "pitch_family": query.pitch_family,
                "horizontal_location": query.horizontal_location,
                "vertical_location": query.vertical_location,
            },
        )


def parse_statcast_relationship_query(question: str, current_season: int) -> StatcastRelationshipQuery | None:
    lowered = question.lower().strip()
    if not any(hint in lowered for hint in COUNT_HINTS + SHOW_HINTS):
        return None

    pitch_family = detect_pitch_family(lowered)
    batter_filter = extract_batter_filter(question, lowered)
    horizontal_location, vertical_location = extract_location_filters(lowered)
    metric_threshold_key, metric_threshold_value = extract_threshold(lowered)
    wants_visuals = any(hint in lowered for hint in VISUAL_HINTS)

    if "most pitches over 100" in lowered or "over 100mph" in lowered or "100 mph" in lowered:
        aggregate_by = "pitcher"
        event_filter = None
    elif "most strikeouts" in lowered and pitch_family:
        aggregate_by = "pitcher"
        event_filter = STRIKEOUT_EVENTS
    elif lowered.startswith(SHOW_HINTS):
        aggregate_by = None
        event_filter = detect_show_event_filter(lowered)
    else:
        return None

    if aggregate_by is None and pitch_family is None and event_filter is None and metric_threshold_key is None and not batter_filter:
        return None

    start_season, end_season, start_date, end_date, scope_label = resolve_scope(question, current_season)
    return StatcastRelationshipQuery(
        mode="aggregate" if aggregate_by else "events",
        start_season=start_season,
        end_season=end_season,
        start_date=start_date,
        end_date=end_date,
        scope_label=scope_label,
        pitch_family=pitch_family,
        event_filter=event_filter,
        metric_threshold_key=metric_threshold_key,
        metric_threshold_value=metric_threshold_value,
        batter_filter=batter_filter,
        pitcher_filter=None,
        horizontal_location=horizontal_location,
        vertical_location=vertical_location,
        aggregate_by=aggregate_by,
        wants_visuals=wants_visuals,
    )


def detect_pitch_family(lowered: str) -> str | None:
    for family in PITCH_TYPE_MAP:
        if family in lowered:
            return family
    return None


def extract_batter_filter(question: str, lowered: str) -> str | None:
    if " off " not in lowered and not lowered.startswith("show me "):
        return None
    stripped = question.strip(" ?.!")
    patterns = (
        r"show me(?:\s+(?:clips?|videos?|replays?|highlights?))?(?:\s+of)?\s+(.+?)\s+"
        r"(?:home\s*-?\s*runs?|homeruns?|homers?|base hits|hits|singles|doubles|triples)\b",
        r"(?:find|list)(?:\s+(?:clips?|videos?|replays?|highlights?))?(?:\s+of)?\s+(.+?)\s+"
        r"(?:home\s*-?\s*runs?|homeruns?|homers?|base hits|hits|singles|doubles|triples)\b",
    )
    for pattern in patterns:
        match = re.match(pattern, stripped, re.IGNORECASE)
        if match is None:
            continue
        candidate = re.sub(r"'s\b", "", match.group(1), flags=re.IGNORECASE).strip(" ?.!,'\"")
        candidate = re.sub(r"^(?:clips?|videos?|replays?|highlights?)\s+of\s+", "", candidate, flags=re.IGNORECASE)
        if " " not in candidate:
            return None
        return " ".join(part.capitalize() for part in candidate.split())
    return None


def extract_threshold(lowered: str) -> tuple[str | None, float | None]:
    speed_match = re.search(r"over\s+(\d+(?:\.\d+)?)\s*mph", lowered)
    if speed_match:
        return "release_speed", float(speed_match.group(1))
    spin_match = re.search(r"spin rates?\s+over\s+(\d+(?:\.\d+)?)", lowered)
    if spin_match:
        return "release_spin_rate", float(spin_match.group(1))
    return None, None


def extract_location_filters(lowered: str) -> tuple[str | None, str | None]:
    horizontal = None
    vertical = None

    if any(token in lowered for token in ("middle-middle", "middle middle", "middle/middle")):
        return "middle", "middle"

    if any(
        token in lowered
        for token in ("in on the hands", "inner-half", "inner half", "up-and-in", "down-and-in")
    ) or re.search(r"\binside\b", lowered):
        horizontal = "inside"
    elif any(
        token in lowered
        for token in ("outer-half", "outer half", "up-and-away", "down-and-away")
    ) or re.search(r"\b(?:outside|away)\b", lowered):
        horizontal = "outside"
    elif any(token in lowered for token in ("center cut", "down the middle", "heart of the plate")) or re.search(
        r"\bmiddle\b",
        lowered,
    ):
        horizontal = "middle"

    if any(
        token in lowered
        for token in ("up in the zone", "elevated", "upper third", "top of the zone", "up-and-in", "up-and-away")
    ) or re.search(r"\b(?:high|up)\b", lowered):
        vertical = "high"
    elif any(token in lowered for token in ("bottom of the zone", "lower third", "down-and-in", "down-and-away")) or re.search(
        r"\b(?:low|down)\b",
        lowered,
    ):
        vertical = "low"
    elif any(token in lowered for token in ("belt high", "mid-zone")) or re.search(r"\bmiddle\b", lowered):
        vertical = "middle"

    return horizontal, vertical


def detect_show_event_filter(lowered: str) -> set[str] | None:
    if re.search(r"\b(?:home\s*-?\s*runs?|homeruns?|homers?)\b", lowered):
        return {"home_run"}
    if "base hits" in lowered or re.search(r"\bhits?\b", lowered):
        return HIT_EVENTS
    return None


def resolve_scope(question: str, current_season: int) -> tuple[int | None, int | None, date | None, date | None, str]:
    lowered = question.lower()
    date_window = extract_date_window(question, current_season)
    if date_window is not None:
        return None, None, date_window.start_date, date_window.end_date, date_window.label
    referenced_season = extract_referenced_season(question, current_season)
    if referenced_season is not None:
        season = referenced_season
        return season, season, None, None, str(season)
    if "this year" in lowered or "this season" in lowered or "current" in lowered:
        return current_season, current_season, None, None, str(current_season)
    return 2015, current_season, None, None, "Statcast era"


def run_statcast_relationship_query(
    settings: Settings,
    query: StatcastRelationshipQuery,
    *,
    live_client: LiveStatsClient | None = None,
) -> list[dict[str, Any]]:
    precomputed_rows = run_precomputed_statcast_relationship_query(settings, query)
    if precomputed_rows is not None:
        return precomputed_rows
    fast_rows = run_fast_statcast_relationship_query(settings, query)
    if fast_rows is not None:
        return fast_rows
    event_rows: list[dict[str, Any]] = []
    batter_ids = resolve_person_ids(live_client, query.batter_filter)
    pitcher_ids = resolve_person_ids(live_client, query.pitcher_filter)
    for window in resolve_windows(settings, query):
        for chunk_start, chunk_end in iter_sync_chunks(window.start_date, window.end_date, 21):
            rows = load_statcast_range(chunk_start.isoformat(), chunk_end.isoformat())
            for row in rows:
                parsed = filter_statcast_row(row, query, batter_ids=batter_ids, pitcher_ids=pitcher_ids)
                if parsed is not None:
                    event_rows.append(parsed)
    if not event_rows:
        return []
    if query.aggregate_by == "pitcher":
        aggregate_rows = aggregate_by_pitcher(event_rows)
        aggregate_rows.sort(key=lambda row: (row["count"], row.get("top_metric") or 0.0), reverse=True)
        return aggregate_rows[:8]
    event_rows.sort(
        key=lambda row: (
            row.get("metric_value") if row.get("metric_value") is not None else float("-inf"),
            row.get("game_date") or "",
        ),
        reverse=True,
    )
    return event_rows[:12]


def run_precomputed_statcast_relationship_query(
    settings: Settings,
    query: StatcastRelationshipQuery,
) -> list[dict[str, Any]] | None:
    connection = get_connection(settings.database_path)
    try:
        return query_precomputed_statcast_relationship(connection, query)
    finally:
        connection.close()


def query_precomputed_statcast_relationship(
    connection,
    query: StatcastRelationshipQuery,
) -> list[dict[str, Any]] | None:
    if not table_exists(connection, "statcast_pitcher_games"):
        return None
    if query.aggregate_by != "pitcher":
        return None
    coverage_clause, coverage_params = build_precomputed_coverage_filter(query)
    row = connection.execute(
        f"SELECT 1 FROM statcast_pitcher_games WHERE {coverage_clause} LIMIT 1",
        coverage_params,
    ).fetchone()
    if row is None:
        return None

    if query.metric_threshold_key == "release_speed":
        threshold_column = precomputed_velocity_threshold_column(query.metric_threshold_value)
        if threshold_column is None:
            return None
        return query_precomputed_pitcher_aggregate(
            connection,
            query,
            count_expression=f"SUM({threshold_column})",
            top_metric_expression="MAX(max_release_speed)",
        )

    if query.event_filter == STRIKEOUT_EVENTS and query.pitch_family:
        strikeout_column = precomputed_pitch_family_strikeout_column(query.pitch_family)
        if strikeout_column is None:
            return None
        return query_precomputed_pitcher_aggregate(
            connection,
            query,
            count_expression=f"SUM({strikeout_column})",
            top_metric_expression="MAX(max_release_speed)",
        )
    return None


def query_precomputed_pitcher_aggregate(
    connection,
    query: StatcastRelationshipQuery,
    *,
    count_expression: str,
    top_metric_expression: str,
) -> list[dict[str, Any]]:
    where_clause, params = build_precomputed_coverage_filter(query)
    if query.pitcher_filter:
        where_clause = f"{where_clause} AND LOWER(pitcher_name) = ?"
        params = [*params, normalize_name(query.pitcher_filter)]
    rows = connection.execute(
        f"""
        SELECT
            pitcher_name AS pitcher,
            {count_expression} AS count,
            {top_metric_expression} AS top_metric
        FROM statcast_pitcher_games
        WHERE {where_clause}
        GROUP BY pitcher_name
        HAVING {count_expression} > 0
        ORDER BY count DESC, top_metric DESC, pitcher_name ASC
        LIMIT 8
        """,
        params,
    ).fetchall()
    return [
        {
            "pitcher": str(row["pitcher"]),
            "count": safe_int(row["count"]) or 0,
            "top_metric": safe_float(row["top_metric"]),
        }
        for row in rows
    ]


def build_precomputed_coverage_filter(query: StatcastRelationshipQuery) -> tuple[str, list[Any]]:
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
    return " AND ".join(clauses) if clauses else "1=1", params


def precomputed_velocity_threshold_column(threshold: float | None) -> str | None:
    if threshold is None:
        return None
    normalized = int(round(float(threshold)))
    column_map = {
        95: "pitches_95_plus",
        97: "pitches_97_plus",
        98: "pitches_98_plus",
        99: "pitches_99_plus",
        100: "pitches_100_plus",
        101: "pitches_101_plus",
        102: "pitches_102_plus",
    }
    if abs(float(threshold) - normalized) > 0.001:
        return None
    return column_map.get(normalized)


def precomputed_pitch_family_strikeout_column(pitch_family: str) -> str | None:
    return {
        "fastball": "fastball_strikeouts",
        "changeup": "changeup_strikeouts",
        "curveball": "curveball_strikeouts",
        "slider": "slider_strikeouts",
    }.get(pitch_family)


def run_fast_statcast_relationship_query(
    settings: Settings,
    query: StatcastRelationshipQuery,
) -> list[dict[str, Any]] | None:
    if not can_use_fast_velocity_pitcher_path(query):
        return None
    assert query.start_season is not None
    assert query.end_season is not None
    season = query.start_season
    if season != query.end_season:
        return None
    candidates = shortlist_velocity_pitchers(season, float(query.metric_threshold_value or 0.0))
    if not candidates:
        return []
    windows = resolve_statcast_sync_windows(settings, start_season=season, end_season=season)
    if not windows:
        return []
    season_start = windows[0].start_date.isoformat()
    season_end = windows[-1].end_date.isoformat()
    event_rows: list[dict[str, Any]] = []
    for pitcher_id, _peak_speed in candidates:
        for row in load_statcast_pitcher(season_start, season_end, pitcher_id):
            parsed = filter_statcast_row(row, query, pitcher_ids={pitcher_id})
            if parsed is not None:
                event_rows.append(parsed)
    if not event_rows:
        return []
    aggregate_rows = aggregate_by_pitcher(event_rows)
    aggregate_rows.sort(key=lambda row: (row["count"], row.get("top_metric") or 0.0), reverse=True)
    return aggregate_rows[:8]


def can_use_fast_velocity_pitcher_path(query: StatcastRelationshipQuery) -> bool:
    if query.aggregate_by != "pitcher":
        return False
    if query.metric_threshold_key != "release_speed":
        return False
    if float(query.metric_threshold_value or 0.0) < FAST_VELOCITY_MIN_THRESHOLD:
        return False
    if query.start_date is not None or query.end_date is not None:
        return False
    if query.start_season is None or query.end_season is None:
        return False
    if query.pitch_family or query.event_filter or query.batter_filter or query.pitcher_filter:
        return False
    if query.horizontal_location or query.vertical_location:
        return False
    return True


def shortlist_velocity_pitchers(season: int, threshold: float) -> list[tuple[int, float]]:
    shortlist: list[tuple[int, float]] = []
    floor = threshold - FAST_VELOCITY_MARGIN
    for row in load_statcast_pitcher_pitch_arsenal(season, min_p=1):
        pitcher_id = safe_int(row.get("pitcher") or row.get("player_id"))
        if pitcher_id is None:
            continue
        peak_speed = peak_velocity_from_arsenal_row(row)
        if peak_speed is None or peak_speed < floor:
            continue
        shortlist.append((pitcher_id, peak_speed))
    shortlist.sort(key=lambda item: item[1], reverse=True)
    return shortlist[:FAST_VELOCITY_SHORTLIST_LIMIT]


def peak_velocity_from_arsenal_row(row: dict[str, Any]) -> float | None:
    peak: float | None = None
    for column in FAST_VELOCITY_COLUMNS:
        value = safe_float(row.get(column))
        if value is None:
            continue
        if peak is None or value > peak:
            peak = value
    return peak


def resolve_windows(settings: Settings, query: StatcastRelationshipQuery):
    if query.start_date is not None and query.end_date is not None:
        return [type("Window", (), {"start_date": query.start_date, "end_date": query.end_date})]
    return resolve_statcast_sync_windows(settings, start_season=query.start_season, end_season=query.end_season)


def filter_statcast_row(
    row: dict[str, Any],
    query: StatcastRelationshipQuery,
    *,
    batter_ids: set[int] | None = None,
    pitcher_ids: set[int] | None = None,
) -> dict[str, Any] | None:
    if str(row.get("game_type") or "R") not in {"R", ""}:
        return None
    pitch_name = str(row.get("pitch_name") or "").strip()
    if query.pitch_family and not pitch_matches_family(pitch_name, query.pitch_family):
        return None
    event_name = str(row.get("events") or "").strip().lower()
    if query.event_filter and event_name not in query.event_filter:
        return None
    if query.metric_threshold_key:
        metric_value = safe_float(row.get(query.metric_threshold_key))
        if metric_value is None or metric_value <= float(query.metric_threshold_value or 0.0):
            return None
    if not pitch_location_matches(row, query.horizontal_location, query.vertical_location):
        return None
    batter_id = safe_int(row.get("batter"))
    batter_name = extract_batter_name(row)
    if batter_ids:
        if batter_id is None or batter_id not in batter_ids:
            return None
    elif query.batter_filter and normalize_name(batter_name) != normalize_name(query.batter_filter):
        return None
    pitcher_id = safe_int(row.get("pitcher"))
    pitcher_name = format_pitcher_name(str(row.get("player_name") or ""))
    if pitcher_ids:
        if pitcher_id is None or pitcher_id not in pitcher_ids:
            return None
    if query.aggregate_by == "pitcher" and not pitcher_name:
        return None
    return {
        "game_pk": safe_int(row.get("game_pk")),
        "at_bat_number": safe_int(row.get("at_bat_number")),
        "pitch_number": safe_int(row.get("pitch_number")),
        "batter_id": batter_id,
        "pitcher_id": pitcher_id,
        "pitcher": pitcher_name,
        "batter": batter_name,
        "game_date": str(row.get("game_date") or ""),
        "team_matchup": f"{row.get('away_team') or ''} @ {row.get('home_team') or ''}".strip(),
        "pitch_name": pitch_name,
        "event": event_name,
        "description": str(row.get("des") or row.get("description") or "").strip(),
        "release_speed": safe_float(row.get("release_speed")),
        "release_spin_rate": safe_float(row.get("release_spin_rate")),
        "launch_speed": safe_float(row.get("launch_speed")),
        "launch_angle": safe_float(row.get("launch_angle")),
        "hit_distance": safe_float(row.get("hit_distance_sc")),
        "metric_value": safe_float(row.get(query.metric_threshold_key)) if query.metric_threshold_key else default_event_sort_metric(row),
    }


def aggregate_by_pitcher(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    aggregates: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row.get("pitcher") or "").strip()
        if not name:
            continue
        aggregate = aggregates.setdefault(
            name,
            {
                "pitcher": name,
                "count": 0,
                "top_metric": None,
                "latest_date": "",
            },
        )
        aggregate["count"] += 1
        metric = safe_float(row.get("metric_value"))
        if metric is not None and (aggregate["top_metric"] is None or metric > aggregate["top_metric"]):
            aggregate["top_metric"] = metric
        if str(row.get("game_date") or "") > str(aggregate["latest_date"] or ""):
            aggregate["latest_date"] = str(row.get("game_date") or "")
    return list(aggregates.values())


def build_statcast_relationship_summary(
    query: StatcastRelationshipQuery,
    rows: list[dict[str, Any]],
    clips: list[dict[str, Any]] | None = None,
) -> str:
    clips = clips or []
    if query.aggregate_by == "pitcher":
        leader = rows[0]
        if query.event_filter == STRIKEOUT_EVENTS and query.pitch_family:
            summary = (
                f"Across {query.scope_label}, {leader['pitcher']} has the most tracked strikeouts ending on "
                f"{query.pitch_family}s with {leader['count']}."
            )
        elif query.metric_threshold_key == "release_speed":
            summary = (
                f"Across {query.scope_label}, {leader['pitcher']} has thrown the most tracked pitches over "
                f"{int(query.metric_threshold_value or 0)} mph with {leader['count']}."
            )
        else:
            summary = f"Across {query.scope_label}, {leader['pitcher']} leads this filtered Statcast count with {leader['count']}."
        trailing = "; ".join(f"{row['pitcher']} {row['count']}" for row in rows[1:4])
        summary = append_location_summary(summary, query)
        return f"{summary} Next on the board: {trailing}." if trailing else summary

    lead = rows[0]
    summary = f"I found {len(rows)} tracked Statcast matches for this filter in {query.scope_label}."
    if clips:
        summary = f"{summary} I also found {len(clips)} public Baseball Savant clip(s)."
        summary = f"{summary} Top clip: {clips[0].get('title') or clips[0].get('description') or lead['description']}"
    elif query.batter_filter:
        summary = f"{summary} The first matching play is {lead['description']}"
    elif lead.get("description"):
        summary = f"{summary} Top match: {lead['description']}"
    if query.metric_threshold_key == "release_spin_rate":
        summary = f"{summary} Spin: {format_metric(lead.get('release_spin_rate'))} rpm."
    elif query.metric_threshold_key == "release_speed":
        summary = f"{summary} Velocity: {format_metric(lead.get('release_speed'))} mph."
    return append_location_summary(summary, query)


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
    if plate_x is None or stand not in {"R", "L"}:
        return False
    if horizontal_location and not horizontal_location_matches(plate_x, stand, horizontal_location):
        return False
    if vertical_location and not vertical_location_matches(plate_z, row, vertical_location):
        return False
    return True


def horizontal_location_matches(plate_x: float, stand: str, location: str) -> bool:
    if location == "inside":
        return (stand == "R" and plate_x <= -0.35) or (stand == "L" and plate_x >= 0.35)
    if location == "outside":
        return (stand == "R" and plate_x >= 0.35) or (stand == "L" and plate_x <= -0.35)
    if location == "middle":
        return abs(plate_x) < 0.35
    return True


def vertical_location_matches(plate_z: float | None, row: dict[str, Any], location: str) -> bool:
    if plate_z is None:
        return False
    sz_top = safe_float(row.get("sz_top"))
    sz_bot = safe_float(row.get("sz_bot"))
    if sz_top is not None and sz_bot is not None and sz_top > sz_bot:
        zone_height = sz_top - sz_bot
        upper_cut = sz_bot + zone_height * 0.67
        lower_cut = sz_bot + zone_height * 0.33
        if location == "high":
            return plate_z >= upper_cut
        if location == "low":
            return plate_z <= lower_cut
        if location == "middle":
            return lower_cut < plate_z < upper_cut
    if location == "high":
        return plate_z >= 3.0
    if location == "low":
        return plate_z <= 2.0
    if location == "middle":
        return 2.0 < plate_z < 3.0
    return True


def append_location_summary(summary: str, query: StatcastRelationshipQuery) -> str:
    label = format_location_label(query.horizontal_location, query.vertical_location)
    if not label:
        return summary
    return f"{summary} Location filter: {label}."


def format_location_label(horizontal_location: str | None, vertical_location: str | None) -> str | None:
    if horizontal_location == "middle" and vertical_location == "middle":
        return "middle-middle"
    parts = []
    if vertical_location:
        parts.append(vertical_location)
    if horizontal_location:
        parts.append(horizontal_location)
    return " ".join(parts) if parts else None


def extract_batter_name(row: dict[str, Any]) -> str:
    description = str(row.get("des") or row.get("description") or "").strip()
    match = re.match(
        r"(.+?)\s+(?:singles|doubles|triples|homers|hits a home run|walks|grounds out|flies out|lines out|strikes out|reaches)\b",
        description,
        re.IGNORECASE,
    )
    if match is not None:
        return " ".join(part.capitalize() for part in match.group(1).strip().split())
    return ""


def format_pitcher_name(value: str) -> str:
    if not value:
        return ""
    if "," not in value:
        return value
    last, first = [part.strip() for part in value.split(",", 1)]
    return f"{first} {last}".strip()


def default_event_sort_metric(row: dict[str, Any]) -> float | None:
    for key in ("release_speed", "release_spin_rate", "launch_speed"):
        value = safe_float(row.get(key))
        if value is not None:
            return value
    return None


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


def normalize_name(value: str) -> str:
    return " ".join(str(value or "").casefold().split())


def format_metric(value: Any) -> str:
    numeric = safe_float(value)
    if numeric is None:
        return "n/a"
    return f"{numeric:.1f}"


def relationship_cache_key(query: StatcastRelationshipQuery) -> str:
    signature = {
        "version": 2,
        "mode": query.mode,
        "start_season": query.start_season,
        "end_season": query.end_season,
        "start_date": query.start_date.isoformat() if query.start_date else None,
        "end_date": query.end_date.isoformat() if query.end_date else None,
        "scope_label": query.scope_label,
        "pitch_family": query.pitch_family,
        "event_filter": sorted(query.event_filter) if query.event_filter else None,
        "metric_threshold_key": query.metric_threshold_key,
        "metric_threshold_value": query.metric_threshold_value,
        "batter_filter": query.batter_filter,
        "pitcher_filter": query.pitcher_filter,
        "horizontal_location": query.horizontal_location,
        "vertical_location": query.vertical_location,
        "aggregate_by": query.aggregate_by,
    }
    return "statcast_relationship_cache::" + json.dumps(signature, sort_keys=True, separators=(",", ":"))


def load_cached_relationship_rows(settings: Settings, query: StatcastRelationshipQuery) -> list[dict[str, Any]] | None:
    connection = get_connection(settings.database_path)
    try:
        payload = get_metadata_value(connection, relationship_cache_key(query))
    finally:
        connection.close()
    if not payload:
        return None
    try:
        rows = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return rows if isinstance(rows, list) else None


def store_cached_relationship_rows(settings: Settings, query: StatcastRelationshipQuery, rows: list[dict[str, Any]]) -> None:
    connection = get_connection(settings.database_path)
    try:
        set_metadata_value(connection, relationship_cache_key(query), json.dumps(rows))
    finally:
        connection.close()


def resolve_person_ids(live_client: LiveStatsClient | None, person_query: str | None) -> set[int]:
    if live_client is None or not person_query:
        return set()
    ids: set[int] = set()
    normalized_target = normalize_name(person_query)
    for person in live_client.search_people(person_query):
        person_id = safe_int(person.get("id"))
        full_name = normalize_name(str(person.get("fullName") or ""))
        if person_id is None:
            continue
        if not normalized_target or full_name == normalized_target or normalized_target in full_name:
            ids.add(person_id)
    return ids


def build_statcast_relationship_clips(
    rows: list[dict[str, Any]],
    live_client: LiveStatsClient,
    sporty_video_client: SportyVideoClient,
) -> list[dict[str, Any]]:
    clip_rows: list[dict[str, Any]] = []
    feed_cache: dict[int, dict[str, Any]] = {}
    seen_play_ids: set[str] = set()
    for row in rows:
        play_id = find_row_play_id(row, live_client, feed_cache)
        if not play_id or play_id in seen_play_ids:
            continue
        sporty_page = sporty_video_client.fetch(play_id)
        if sporty_page is None or not (sporty_page.savant_url or sporty_page.mp4_url):
            continue
        seen_play_ids.add(play_id)
        title = sporty_page.title or str(row.get("description") or "")
        clip_rows.append(
            {
                "play_id": play_id,
                "title": title,
                "description": str(row.get("description") or ""),
                "explanation": build_clip_explanation(row),
                "game_date": str(row.get("game_date") or sporty_page.page_date or ""),
                "team_matchup": sporty_page.matchup or str(row.get("team_matchup") or ""),
                "batter_name": sporty_page.batter or str(row.get("batter") or ""),
                "pitcher_name": sporty_page.pitcher or str(row.get("pitcher") or ""),
                "savant_url": sporty_page.savant_url,
                "mp4_url": sporty_page.mp4_url,
                "hit_distance": sporty_page.hit_distance if sporty_page.hit_distance is not None else row.get("hit_distance"),
                "exit_velocity": sporty_page.exit_velocity if sporty_page.exit_velocity is not None else row.get("launch_speed"),
                "launch_angle": sporty_page.launch_angle if sporty_page.launch_angle is not None else row.get("launch_angle"),
                "hr_parks": sporty_page.hr_parks,
                "match_tags": [str(row.get("event") or "").replace("_", " "), str(row.get("pitch_name") or "").strip()],
            }
        )
        if len(clip_rows) >= 6:
            break
    return clip_rows


def find_row_play_id(
    row: dict[str, Any],
    live_client: LiveStatsClient,
    feed_cache: dict[int, dict[str, Any]],
) -> str | None:
    game_pk = safe_int(row.get("game_pk"))
    if game_pk is None:
        return None
    feed = feed_cache.get(game_pk)
    if feed is None:
        feed = live_client.game_feed(game_pk)
        feed_cache[game_pk] = feed
    play = find_matching_play(feed, row)
    if play is None:
        return None
    pitch_number = safe_int(row.get("pitch_number"))
    if pitch_number is not None:
        for event in play.get("playEvents", []):
            if safe_int(event.get("pitchNumber")) == pitch_number:
                play_id = str(event.get("playId") or "").strip()
                if play_id:
                    return play_id
    return extract_play_id(play)


def find_matching_play(feed: dict[str, Any], row: dict[str, Any]) -> dict[str, Any] | None:
    target_batter_id = safe_int(row.get("batter_id"))
    target_pitcher_id = safe_int(row.get("pitcher_id"))
    target_at_bat_number = safe_int(row.get("at_bat_number"))
    target_event = str(row.get("event") or "").strip().lower()
    target_description = normalize_description(str(row.get("description") or ""))
    best_play: dict[str, Any] | None = None
    best_score = -1
    for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
        score = 0
        batter_id = safe_int(play.get("matchup", {}).get("batter", {}).get("id"))
        pitcher_id = safe_int(play.get("matchup", {}).get("pitcher", {}).get("id"))
        at_bat_index = safe_int(play.get("about", {}).get("atBatIndex"))
        event_type = str(play.get("result", {}).get("eventType") or "").strip().lower()
        description = normalize_description(str(play.get("result", {}).get("description") or ""))
        if target_batter_id is not None and batter_id == target_batter_id:
            score += 6
        elif target_batter_id is not None:
            continue
        if target_pitcher_id is not None and pitcher_id == target_pitcher_id:
            score += 2
        if target_at_bat_number is not None and at_bat_index is not None and (at_bat_index + 1) == target_at_bat_number:
            score += 6
        if target_event and event_type == target_event:
            score += 3
        if target_description and description == target_description:
            score += 4
        if score > best_score:
            best_score = score
            best_play = play
    return best_play if best_score >= 6 else None


def normalize_description(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().casefold())


def build_clip_explanation(row: dict[str, Any]) -> str:
    parts = []
    pitch_name = str(row.get("pitch_name") or "").strip()
    if pitch_name:
        parts.append(f"the pitch was a {pitch_name}")
    if row.get("event"):
        parts.append(f"the result was {str(row['event']).replace('_', ' ')}")
    if row.get("launch_speed") is not None:
        parts.append(f"exit velocity was {format_metric(row.get('launch_speed'))} mph")
    if row.get("hit_distance") is not None:
        parts.append(f"distance was {int(round(float(row['hit_distance'])))} ft")
    if not parts:
        return str(row.get("description") or "")
    return "Relevant because " + "; ".join(parts) + "."
