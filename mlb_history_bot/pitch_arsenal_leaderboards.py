from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .models import EvidenceSnippet
from .provider_metrics import extract_team_filter, normalize_team_value
from .pybaseball_adapter import load_statcast_pitcher_arsenal_stats, load_statcast_pitcher_pitch_arsenal
from .query_intent import detect_ranking_intent, looks_like_leaderboard_question, mentions_current_scope
from .query_utils import extract_referenced_season


HISTORICAL_HINTS = ("historically", "statcast era", "all time", "all-time", "ever")


@dataclass(frozen=True, slots=True)
class PitchArsenalMetricSpec:
    key: str
    label: str
    arsenal_type: str
    aliases: tuple[str, ...]
    higher_is_better: bool
    unit: str


@dataclass(frozen=True, slots=True)
class PitchFamilySpec:
    key: str
    label: str
    aliases: tuple[str, ...]
    prefixes: tuple[str, ...]


@dataclass(slots=True)
class PitchArsenalQuery:
    metric: PitchArsenalMetricSpec
    pitch_family: PitchFamilySpec
    descriptor: str
    sort_desc: bool
    start_season: int
    end_season: int
    scope_label: str
    min_pitches: int
    team_filter: str | None
    mode: str


SUPPORTED_PITCH_ARSENAL_METRICS: tuple[PitchArsenalMetricSpec, ...] = (
    PitchArsenalMetricSpec(
        key="avg_spin",
        label="spin rate",
        arsenal_type="avg_spin",
        aliases=("spin rate", "spin", "rpm", "spin rates", "average spin rate"),
        higher_is_better=True,
        unit="rpm",
    ),
    PitchArsenalMetricSpec(
        key="avg_speed",
        label="velocity",
        arsenal_type="avg_speed",
        aliases=("velocity", "velo", "speed", "average velocity", "average fastball velocity"),
        higher_is_better=True,
        unit="mph",
    ),
)

SUPPORTED_PITCH_FAMILIES: tuple[PitchFamilySpec, ...] = (
    PitchFamilySpec("slider", "slider", ("slider", "sliders"), ("sl", "st", "sv")),
    PitchFamilySpec("sweeper", "sweeper", ("sweeper", "sweepers"), ("st",)),
    PitchFamilySpec("slurve", "slurve", ("slurve", "slurves"), ("sv",)),
    PitchFamilySpec("curveball", "curveball", ("curveball", "curveballs", "curve", "curves"), ("cu",)),
    PitchFamilySpec("changeup", "changeup", ("changeup", "changeups", "change", "changes"), ("ch", "fs")),
    PitchFamilySpec("splitter", "splitter", ("splitter", "splitters", "split-finger", "split-fingers"), ("fs",)),
    PitchFamilySpec("fastball", "fastball", ("fastball", "fastballs"), ("ff", "si", "fc")),
    PitchFamilySpec("four_seam", "four-seam fastball", ("four-seam", "four seam", "4-seam", "four-seamer", "four seamer"), ("ff",)),
    PitchFamilySpec("sinker", "sinker", ("sinker", "sinkers", "two-seamer", "two seamer", "2-seamer"), ("si",)),
    PitchFamilySpec("cutter", "cutter", ("cutter", "cutters"), ("fc",)),
    PitchFamilySpec("knuckleball", "knuckleball", ("knuckleball", "knuckleballs"), ("kn",)),
)

PITCH_TYPE_LABELS = {
    "ff": "4-Seam Fastball",
    "si": "Sinker",
    "fc": "Cutter",
    "sl": "Slider",
    "st": "Sweeper",
    "sv": "Slurve",
    "ch": "Changeup",
    "fs": "Splitter",
    "cu": "Curveball",
    "kn": "Knuckleball",
}


class PitchArsenalLeaderboardResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build_snippet(self, question: str) -> EvidenceSnippet | None:
        current_season = self.settings.live_season or date.today().year
        query = parse_pitch_arsenal_query(question, current_season)
        if query is None:
            return None
        leaders = run_pitch_arsenal_query(query)
        if not leaders:
            return None
        mode = "live" if query.mode == "live" else "historical"
        summary = build_pitch_arsenal_summary(query, leaders)
        return EvidenceSnippet(
            source="Pitch Arsenal Leaderboards",
            title=f"{query.scope_label} {query.pitch_family.label} {query.metric.label} leaderboard",
            citation="pybaseball statcast_pitcher_pitch_arsenal plus statcast_pitcher_arsenal_stats",
            summary=summary,
            payload={
                "analysis_type": "pitch_arsenal_leaderboard",
                "mode": mode,
                "metric": query.metric.label,
                "pitch_family": query.pitch_family.label,
                "scope_label": query.scope_label,
                "leaders": leaders,
                "min_pitches": query.min_pitches,
                "team_filter": query.team_filter,
            },
        )


def parse_pitch_arsenal_query(question: str, current_season: int) -> PitchArsenalQuery | None:
    lowered = question.lower()
    if not looks_like_leaderboard_question(lowered):
        return None
    if "pitcher" not in lowered and "pitchers" not in lowered and "starter" not in lowered and "reliever" not in lowered:
        return None
    metric = find_pitch_arsenal_metric(lowered)
    if metric is None:
        return None
    pitch_family = find_pitch_family(lowered)
    if pitch_family is None:
        return None
    ranking = detect_ranking_intent(lowered, higher_is_better=metric.higher_is_better, require_hint=True)
    if ranking is None:
        return None
    referenced_season = extract_referenced_season(question, current_season)
    if referenced_season is not None:
        start_season = referenced_season
        end_season = referenced_season
        scope_label = str(referenced_season)
        mode = "live" if referenced_season == current_season else "historical"
    elif any(hint in lowered for hint in HISTORICAL_HINTS):
        start_season = 2015
        end_season = current_season
        scope_label = "Statcast era"
        mode = "historical"
    elif mentions_current_scope(lowered) or explicit_year is None:
        start_season = current_season
        end_season = current_season
        scope_label = str(current_season)
        mode = "live"
    else:
        start_season = current_season
        end_season = current_season
        scope_label = str(current_season)
        mode = "live"
    team_filter = extract_team_filter(lowered)
    min_pitches = extract_minimum_pitches(lowered) or 25
    return PitchArsenalQuery(
        metric=metric,
        pitch_family=pitch_family,
        descriptor=ranking.descriptor,
        sort_desc=ranking.sort_desc,
        start_season=start_season,
        end_season=end_season,
        scope_label=scope_label,
        min_pitches=min_pitches,
        team_filter=team_filter,
        mode=mode,
    )


def find_pitch_arsenal_metric(lowered_question: str) -> PitchArsenalMetricSpec | None:
    best: tuple[int, PitchArsenalMetricSpec] | None = None
    for metric in SUPPORTED_PITCH_ARSENAL_METRICS:
        for alias in metric.aliases:
            if alias in lowered_question:
                score = len(alias)
                if best is None or score > best[0]:
                    best = (score, metric)
    return best[1] if best else None


def find_pitch_family(lowered_question: str) -> PitchFamilySpec | None:
    best: tuple[int, PitchFamilySpec] | None = None
    for family in SUPPORTED_PITCH_FAMILIES:
        for alias in family.aliases:
            pattern = rf"(?<![a-z]){re.escape(alias)}(?![a-z])"
            if re.search(pattern, lowered_question) is None:
                continue
            score = len(alias)
            if best is None or score > best[0]:
                best = (score, family)
    return best[1] if best else None


def extract_minimum_pitches(lowered_question: str) -> int | None:
    match = re.search(r"(?:minimum|at least|min\.?)\s+of?\s*(\d+)\s+(?:pitch|pitches|" r"sliders|curveballs|changeups|fastballs)", lowered_question)
    if match:
        return int(match.group(1))
    return None


def run_pitch_arsenal_query(query: PitchArsenalQuery) -> list[dict[str, Any]]:
    leaders: list[dict[str, Any]] = []
    for season in range(query.start_season, query.end_season + 1):
        rows = load_pitch_arsenal_rows(season, query.metric.arsenal_type, query.min_pitches)
        if not rows:
            continue
        stats_lookup = build_pitch_stats_lookup(season)
        for row in rows:
            candidate = row_to_leader_candidate(row, stats_lookup, season, query)
            if candidate is None:
                continue
            leaders.append(candidate)
    if not leaders:
        return []
    leaders.sort(
        key=lambda row: (
            sortable_value(row["metric_value"]),
            row.get("pitch_count") or 0,
            row["pitcher_name"],
        ),
        reverse=query.sort_desc,
    )
    top_rows = leaders[:8]
    for index, row in enumerate(top_rows, start=1):
        row["rank"] = index
    return top_rows[:5]


def load_pitch_arsenal_rows(season: int, arsenal_type: str, min_pitches: int) -> list[dict[str, Any]]:
    thresholds = sorted({min_pitches, 100, 50, 25, 1}, reverse=True)
    usable_rows: list[dict[str, Any]] = []
    for threshold in thresholds:
        if threshold < min_pitches:
            continue
        rows = load_statcast_pitcher_pitch_arsenal(season, min_p=threshold, arsenal_type=arsenal_type)
        if rows:
            usable_rows = rows
    if usable_rows:
        return usable_rows
    return load_statcast_pitcher_pitch_arsenal(season, min_p=min_pitches, arsenal_type=arsenal_type)


def build_pitch_stats_lookup(season: int) -> dict[tuple[int, str], dict[str, Any]]:
    lookup: dict[tuple[int, str], dict[str, Any]] = {}
    for row in load_statcast_pitcher_arsenal_stats(season, min_pa=1):
        player_id = safe_int(row.get("player_id"))
        pitch_type = str(row.get("pitch_type") or "").strip().upper()
        if player_id is None or not pitch_type:
            continue
        lookup[(player_id, pitch_type)] = row
    return lookup


def row_to_leader_candidate(
    row: dict[str, Any],
    stats_lookup: dict[tuple[int, str], dict[str, Any]],
    season: int,
    query: PitchArsenalQuery,
) -> dict[str, Any] | None:
    player_id = safe_int(row.get("pitcher"))
    if player_id is None:
        return None
    metric_candidates: list[tuple[float, str, dict[str, Any] | None]] = []
    for prefix in query.pitch_family.prefixes:
        value = safe_float(row.get(f"{prefix}_{query.metric.key}"))
        if value is None:
            continue
        stats_row = stats_lookup.get((player_id, prefix.upper()))
        metric_candidates.append((value, prefix, stats_row))
    if not metric_candidates:
        return None
    metric_value, prefix, stats_row = (
        max(metric_candidates, key=lambda item: item[0])
        if query.sort_desc
        else min(metric_candidates, key=lambda item: item[0])
    )
    team = normalize_team_value((stats_row or {}).get("team_name_alt"))
    if query.team_filter and team != query.team_filter:
        return None
    pitch_count = safe_int((stats_row or {}).get("pitches"))
    if pitch_count is not None and pitch_count < query.min_pitches:
        return None
    pitcher_name = format_pitcher_name(str(row.get("last_name, first_name") or ""))
    return {
        "pitcher_name": pitcher_name,
        "player_id": player_id,
        "team": team,
        "season": season,
        "pitch_label": PITCH_TYPE_LABELS.get(prefix, query.pitch_family.label.title()),
        "pitch_type": prefix.upper(),
        "metric_value": metric_value,
        "pitch_count": pitch_count,
    }


def build_pitch_arsenal_summary(query: PitchArsenalQuery, leaders: list[dict[str, Any]]) -> str:
    leader = leaders[0]
    summary = (
        f"Using public Statcast pitch-arsenal leaderboards via pybaseball, the {query.scope_label} "
        f"{query.descriptor} {query.pitch_family.label} {query.metric.label} belongs to "
        f"{leader['pitcher_name']}"
    )
    if leader.get("team"):
        summary = f"{summary} ({leader['team']})"
    summary = f"{summary} at {format_metric_value(leader['metric_value'], query.metric.unit)}"
    if leader.get("pitch_label"):
        summary = f"{summary} on a tracked {leader['pitch_label']}."
    else:
        summary = f"{summary}."
    if leader.get("pitch_count"):
        summary = f"{summary} Sample: {leader['pitch_count']} pitches."
    trailing = leaders[1:4]
    if trailing:
        summary = (
            f"{summary} Next on the board: "
            + "; ".join(
                f"{row['pitcher_name']} {format_metric_value(row['metric_value'], query.metric.unit)}"
                + (f" ({row['season']})" if query.scope_label == 'Statcast era' else "")
                for row in trailing
            )
            + "."
        )
    if query.team_filter:
        summary = f"{summary} Team filter: {query.team_filter}."
    if query.scope_label != "Statcast era":
        summary = (
            f"{summary} This treats the question as a season-average pitch leaderboard, not a single-pitch maximum."
        )
    return summary


def format_pitcher_name(value: str) -> str:
    if not value:
        return ""
    if "," not in value:
        return value.strip()
    last, first = [part.strip() for part in value.split(",", 1)]
    return f"{first} {last}".strip()


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
        return int(float(value))
    except (TypeError, ValueError):
        return None


def sortable_value(value: Any) -> float:
    numeric = safe_float(value)
    return numeric if numeric is not None else float("-inf")


def format_metric_value(value: Any, unit: str) -> str:
    numeric = safe_float(value)
    if numeric is None:
        return "n/a"
    if unit == "rpm":
        return f"{numeric:.0f} rpm"
    return f"{numeric:.1f} {unit}"
