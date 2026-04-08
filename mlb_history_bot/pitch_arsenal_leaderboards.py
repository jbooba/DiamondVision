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
from .query_utils import extract_name_candidates, extract_referenced_season
from .storage import get_connection, table_exists


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


@dataclass(slots=True)
class PitchArsenalLookupQuery:
    pitcher_name: str
    season: int | None
    scope_label: str
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
        if query is not None:
            leaders = run_pitch_arsenal_query(self.settings, query)
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
        lookup_query = parse_pitch_arsenal_lookup_query(question, current_season)
        if lookup_query is None:
            return None
        repertoire = run_pitch_arsenal_lookup(self.settings, lookup_query)
        if not repertoire:
            return None
        summary = build_pitch_arsenal_lookup_summary(lookup_query, repertoire)
        return EvidenceSnippet(
            source="Pitch Arsenal Leaderboards",
            title=f"{repertoire[0]['season']} {lookup_query.pitcher_name} pitch mix",
            citation="local statcast_pitch_type_games summaries plus pybaseball statcast arsenal tables when needed",
            summary=summary,
            payload={
                "analysis_type": "pitch_arsenal_lookup",
                "mode": lookup_query.mode,
                "scope_label": lookup_query.scope_label,
                "pitcher_name": lookup_query.pitcher_name,
                "season": repertoire[0]["season"],
                "repertoire": repertoire,
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
    elif mentions_current_scope(lowered):
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


def parse_pitch_arsenal_lookup_query(question: str, current_season: int) -> PitchArsenalLookupQuery | None:
    lowered = question.lower()
    if not any(
        phrase in lowered
        for phrase in (
            "what pitches does",
            "what does",
            "pitch mix",
            "repertoire",
            "arsenal",
            "what does he throw",
            "what does she throw",
            "what does he feature",
            "what does she feature",
            "what does he mix",
            "what does she mix",
        )
    ):
        return None
    if looks_like_leaderboard_question(lowered):
        return None
    pitcher_name = extract_pitcher_lookup_name(question)
    if not pitcher_name:
        return None
    referenced_season = extract_referenced_season(question, current_season)
    if referenced_season is not None:
        return PitchArsenalLookupQuery(
            pitcher_name=pitcher_name,
            season=referenced_season,
            scope_label=str(referenced_season),
            mode="live" if referenced_season == current_season else "historical",
        )
    if mentions_current_scope(lowered):
        return PitchArsenalLookupQuery(
            pitcher_name=pitcher_name,
            season=current_season,
            scope_label=str(current_season),
            mode="live",
        )
    return PitchArsenalLookupQuery(
        pitcher_name=pitcher_name,
        season=None,
        scope_label="latest available season",
        mode="historical",
    )


def extract_pitcher_lookup_name(question: str) -> str | None:
    names = extract_name_candidates(question)
    if names:
        return names[0]
    stripped = question.strip(" ?!.")
    patterns = (
        re.compile(r"what pitches does\s+(.+?)\s+throw\b", re.IGNORECASE),
        re.compile(r"what does\s+(.+?)\s+throw\b", re.IGNORECASE),
        re.compile(r"(?:what is|what's)\s+(.+?)'s\s+(?:pitch mix|arsenal|repertoire)\b", re.IGNORECASE),
        re.compile(r"(?:pitch mix|arsenal|repertoire)\s+(?:for|of)\s+(.+?)\b", re.IGNORECASE),
    )
    for pattern in patterns:
        match = pattern.search(stripped)
        if not match:
            continue
        candidate = str(match.group(1) or "").strip(" ?!.,'\"")
        candidate = re.sub(r"^(?:the|a|an)\s+", "", candidate, flags=re.IGNORECASE)
        if " " in candidate:
            return " ".join(part.capitalize() for part in candidate.split())
    return None


def run_pitch_arsenal_query(settings: Settings, query: PitchArsenalQuery) -> list[dict[str, Any]]:
    local_leaders = run_local_pitch_arsenal_query(settings, query)
    if local_leaders:
        return local_leaders
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


def run_local_pitch_arsenal_query(settings: Settings, query: PitchArsenalQuery) -> list[dict[str, Any]]:
    connection = get_connection(settings.database_path)
    try:
        if not table_exists(connection, "statcast_pitch_type_games"):
            return []
        where_clause, params = build_local_pitch_type_filter(query)
        metric_sql = "AVG(avg_release_spin_rate)" if query.metric.key == "avg_spin" else "AVG(avg_release_speed)"
        rows = connection.execute(
            f"""
            SELECT
                pitcher_id AS player_id,
                pitcher_name,
                team,
                pitch_type,
                MIN(pitch_name) AS pitch_label,
                {metric_sql} AS metric_value,
                SUM(pitches) AS pitch_count
            FROM statcast_pitch_type_games
            WHERE {where_clause}
            GROUP BY pitcher_id, pitcher_name, team, pitch_type
            HAVING SUM(pitches) >= ?
               AND {metric_sql} IS NOT NULL
            ORDER BY metric_value {"DESC" if query.sort_desc else "ASC"}, pitch_count DESC, pitcher_name ASC
            LIMIT 8
            """,
            (*params, query.min_pitches),
        ).fetchall()
    finally:
        connection.close()
    leaders = [
        {
            "pitcher_name": str(row["pitcher_name"]),
            "player_id": safe_int(row["player_id"]),
            "team": normalize_team_value(row["team"]),
            "season": query.start_season if query.start_season == query.end_season else None,
            "pitch_label": str(row["pitch_label"] or ""),
            "pitch_type": str(row["pitch_type"] or ""),
            "metric_value": safe_float(row["metric_value"]),
            "pitch_count": safe_int(row["pitch_count"]),
        }
        for row in rows
    ]
    for index, row in enumerate(leaders, start=1):
        row["rank"] = index
    return leaders[:5]


def build_local_pitch_type_filter(query: PitchArsenalQuery) -> tuple[str, list[Any]]:
    clauses = ["season >= ?", "season <= ?"]
    params: list[Any] = [query.start_season, query.end_season]
    family_clause, family_params = pitch_family_sql_filter(query.pitch_family)
    clauses.append(family_clause)
    params.extend(family_params)
    if query.team_filter:
        clauses.append("team = ?")
        params.append(query.team_filter)
    return " AND ".join(clauses), params


def run_pitch_arsenal_lookup(settings: Settings, query: PitchArsenalLookupQuery) -> list[dict[str, Any]]:
    resolved_season: int | None = query.season
    rows: list[dict[str, Any]] = []
    connection = get_connection(settings.database_path)
    try:
        if table_exists(connection, "statcast_pitch_type_games"):
            resolved_season = query.season or lookup_latest_pitch_arsenal_season(connection, query.pitcher_name)
            if resolved_season is not None:
                rows = load_local_pitch_arsenal_repertoire(connection, query.pitcher_name, resolved_season)
    finally:
        connection.close()
    if not rows:
        resolved_season = resolved_season or query.season or settings.live_season or date.today().year
        rows = load_provider_pitch_arsenal_repertoire(query.pitcher_name, resolved_season)
    if not rows:
        return []
    total_pitches = sum(row["pitch_count"] or 0 for row in rows)
    repertoire: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        pitch_count = row["pitch_count"] or 0
        usage_pct = (pitch_count / total_pitches * 100.0) if total_pitches else None
        repertoire.append(
            {
                "rank": index,
                "season": resolved_season,
                "pitcher_name": row["pitcher_name"],
                "team": row["team"],
                "pitch_label": row["pitch_label"],
                "pitch_family": row["pitch_family"],
                "pitch_type": row["pitch_type"],
                "pitch_count": pitch_count,
                "usage_pct": usage_pct,
                "avg_speed": row["avg_speed"],
                "avg_spin": row["avg_spin"],
            }
        )
    return repertoire


def load_provider_pitch_arsenal_repertoire(pitcher_name: str, season: int) -> list[dict[str, Any]]:
    rows = load_statcast_pitcher_arsenal_stats(season, min_pa=1)
    if not rows:
        return []
    lowered_name = pitcher_name.lower()
    matching_rows = [
        row
        for row in rows
        if lowered_name == str(row.get("last_name, first_name") or "").lower()
        or lowered_name in format_pitcher_name(str(row.get("last_name, first_name") or "")).lower()
        or lowered_name == str(row.get("player_name") or "").lower()
        or lowered_name in str(row.get("player_name") or "").lower()
    ]
    if not matching_rows:
        return []
    repertoire: list[dict[str, Any]] = []
    for row in matching_rows:
        pitch_type = str(row.get("pitch_type") or "").strip().upper()
        pitch_label = str(row.get("pitch_name") or PITCH_TYPE_LABELS.get(pitch_type.lower(), "")).strip()
        repertoire.append(
            {
                "pitcher_name": format_pitcher_name(str(row.get("last_name, first_name") or row.get("player_name") or pitcher_name)),
                "team": normalize_team_value(row.get("team_name_alt")),
                "pitch_type": pitch_type,
                "pitch_label": pitch_label,
                "pitch_family": str(row.get("pitch_family") or infer_pitch_family_from_type(pitch_type)),
                "pitch_count": safe_int(row.get("pitches")),
                "avg_speed": safe_float(row.get("release_speed")),
                "avg_spin": safe_float(row.get("release_spin_rate")),
            }
        )
    repertoire.sort(key=lambda row: ((row["pitch_count"] or 0), row["pitch_label"]), reverse=True)
    return repertoire


def lookup_latest_pitch_arsenal_season(connection, pitcher_name: str) -> int | None:
    row = connection.execute(
        """
        SELECT MAX(season) AS season
        FROM statcast_pitch_type_games
        WHERE lower(pitcher_name) = ?
           OR lower(pitcher_name) LIKE ?
        """,
        (pitcher_name.lower(), f"%{pitcher_name.lower()}%"),
    ).fetchone()
    return safe_int(row["season"]) if row is not None else None


def load_local_pitch_arsenal_repertoire(connection, pitcher_name: str, season: int) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
            pitcher_name,
            team,
            pitch_type,
            MIN(pitch_name) AS pitch_label,
            MIN(pitch_family) AS pitch_family,
            SUM(pitches) AS pitch_count,
            AVG(avg_release_speed) AS avg_speed,
            AVG(avg_release_spin_rate) AS avg_spin
        FROM statcast_pitch_type_games
        WHERE season = ?
          AND (lower(pitcher_name) = ? OR lower(pitcher_name) LIKE ?)
        GROUP BY pitcher_name, team, pitch_type
        HAVING SUM(pitches) > 0
        ORDER BY pitch_count DESC, pitch_label ASC
        """,
        (season, pitcher_name.lower(), f"%{pitcher_name.lower()}%"),
    ).fetchall()
    return [
        {
            "pitcher_name": str(row["pitcher_name"] or pitcher_name),
            "team": normalize_team_value(row["team"]),
            "pitch_type": str(row["pitch_type"] or ""),
            "pitch_label": str(row["pitch_label"] or PITCH_TYPE_LABELS.get(str(row["pitch_type"] or "").lower(), "")),
            "pitch_family": str(row["pitch_family"] or ""),
            "pitch_count": safe_int(row["pitch_count"]),
            "avg_speed": safe_float(row["avg_speed"]),
            "avg_spin": safe_float(row["avg_spin"]),
        }
        for row in rows
    ]


def pitch_family_sql_filter(family: PitchFamilySpec) -> tuple[str, list[Any]]:
    if family.key == "slider":
        return "(pitch_family = ?)", ["slider"]
    if family.key == "curveball":
        return "(pitch_family = ?)", ["curveball"]
    if family.key == "changeup":
        return "(pitch_family = ?)", ["changeup"]
    if family.key == "fastball":
        return "(pitch_family = ?)", ["fastball"]
    if family.key == "sweeper":
        return "(pitch_type = ?)", ["ST"]
    if family.key == "slurve":
        return "(pitch_type = ?)", ["SV"]
    if family.key == "splitter":
        return "(pitch_type = ?)", ["FS"]
    if family.key == "four_seam":
        return "(pitch_type = ?)", ["FF"]
    if family.key == "sinker":
        return "(pitch_type = ?)", ["SI"]
    if family.key == "cutter":
        return "(pitch_type = ?)", ["FC"]
    if family.key == "knuckleball":
        return "(pitch_type = ?)", ["KN"]
    return "(pitch_family = ?)", [family.key]


def infer_pitch_family_from_type(pitch_type: str) -> str:
    normalized = pitch_type.strip().upper()
    if normalized in {"FF", "SI", "FC"}:
        return "fastball"
    if normalized in {"SL", "ST", "SV"}:
        return "slider"
    if normalized in {"CU"}:
        return "curveball"
    if normalized in {"CH", "FS"}:
        return "changeup"
    if normalized in {"KN"}:
        return "knuckleball"
    return ""


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


def build_pitch_arsenal_lookup_summary(query: PitchArsenalLookupQuery, repertoire: list[dict[str, Any]]) -> str:
    leader = repertoire[0]
    team_text = f" for {leader['team']}" if leader.get("team") else ""
    summary = (
        f"{leader['pitcher_name']}'s tracked {leader['season']} pitch mix{team_text} is led by "
        f"{leader['pitch_label']} ({format_usage_pct(leader['usage_pct'])}, {leader['pitch_count']} pitches"
    )
    if leader.get("avg_speed") is not None:
        summary = f"{summary}, {leader['avg_speed']:.1f} mph"
    summary = f"{summary})."
    trailing = repertoire[1:5]
    if trailing:
        summary = (
            f"{summary} Other core offerings: "
            + "; ".join(
                f"{row['pitch_label']} {format_usage_pct(row['usage_pct'])}"
                + (f", {row['avg_speed']:.1f} mph" if row.get("avg_speed") is not None else "")
                for row in trailing
            )
            + "."
        )
    summary = (
        f"{summary} Ranked by usage share from local Statcast pitch-type summaries, so this reflects repertoire mix rather than just raw pitch labels."
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


def format_usage_pct(value: Any) -> str:
    numeric = safe_float(value)
    if numeric is None:
        return "n/a"
    return f"{numeric:.1f}%"
