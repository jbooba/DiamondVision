from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .metrics import MetricCatalog
from .models import EvidenceSnippet
from .query_intent import detect_ranking_intent, looks_like_leaderboard_question, mentions_current_scope
from .pybaseball_adapter import load_batting_stats, load_pitching_stats
from .query_utils import extract_minimum_qualifier, extract_referenced_season
from .statcast_sync import TEAM_NAMES


SINGLE_GAME_HINTS = {"single game", "single-game", "in a game", "in one game", "tonight", "today"}
BATTING_GROUP_HINTS = {"batter", "batter's", "hitter", "hitters", "offense", "offensive", "position player"}
PITCHING_GROUP_HINTS = {"pitcher", "pitchers", "pitching", "starter", "starters", "reliever", "relievers", "bullpen"}


@dataclass(slots=True)
class ProviderMetricSpec:
    metric_name: str
    aliases: tuple[str, ...]
    batting_column: str | None
    pitching_column: str | None
    higher_is_better: bool
    label: str
    qualified_only: bool


SUPPORTED_PROVIDER_METRICS: tuple[ProviderMetricSpec, ...] = (
    ProviderMetricSpec("WAR", ("wins above replacement",), "WAR", "WAR", True, "WAR", False),
    ProviderMetricSpec("BA", ("batting average", "avg"), "AVG", None, True, "BA", True),
    ProviderMetricSpec("OBP", ("on-base percentage",), "OBP", None, True, "OBP", True),
    ProviderMetricSpec("SLG", ("slugging percentage",), "SLG", None, True, "SLG", True),
    ProviderMetricSpec("OPS", ("on-base plus slugging",), "OPS", None, True, "OPS", True),
    ProviderMetricSpec("ISO", ("isolated power",), "ISO", None, True, "ISO", True),
    ProviderMetricSpec("BABIP", ("batting average on balls in play",), "BABIP", "BABIP", True, "BABIP", True),
    ProviderMetricSpec("BB%", ("walk rate", "bb rate"), "BB%", "BB%", True, "BB%", True),
    ProviderMetricSpec("K%", ("strikeout rate", "k rate"), "K%", "K%", False, "K%", True),
    ProviderMetricSpec("BB/K", ("walks per strikeout", "bb per k", "walk-to-strikeout ratio"), "BB/K", None, True, "BB/K", True),
    ProviderMetricSpec("wRC+", ("weighted runs created plus",), "wRC+", None, True, "wRC+", True),
    ProviderMetricSpec("wOBA", ("weighted on-base average",), "wOBA", None, True, "wOBA", True),
    ProviderMetricSpec("wRAA", ("weighted runs above average",), "wRAA", None, True, "wRAA", False),
    ProviderMetricSpec("wRC", ("weighted runs created",), "wRC", None, True, "wRC", False),
    ProviderMetricSpec("xwOBA", ("expected woba",), "xwOBA", None, True, "xwOBA", True),
    ProviderMetricSpec("xBA", ("expected batting average",), "xBA", None, True, "xBA", True),
    ProviderMetricSpec("xSLG", ("expected slugging",), "xSLG", None, True, "xSLG", True),
    ProviderMetricSpec("Hard-Hit Rate", ("hard-hit rate", "hard hit rate"), "HardHit%", None, True, "Hard-Hit Rate", True),
    ProviderMetricSpec("Barrel Rate", ("barrel rate",), "Barrel%", None, True, "Barrel Rate", True),
    ProviderMetricSpec("EV", ("exit velocity", "average exit velocity"), "EV", None, True, "EV", True),
    ProviderMetricSpec("maxEV", ("max exit velocity", "maximum exit velocity"), "maxEV", None, True, "maxEV", True),
    ProviderMetricSpec("Bat", ("batting runs", "bat value"), "Bat", None, True, "Bat", False),
    ProviderMetricSpec("Fld", ("fielding runs", "fielding value"), "Fld", None, True, "Fld", False),
    ProviderMetricSpec("Def", ("fangraphs def", "defensive value"), "Def", None, True, "Def", False),
    ProviderMetricSpec("Off", ("offensive value",), "Off", None, True, "Off", False),
    ProviderMetricSpec("BsR", ("baserunning",), "BsR", None, True, "BsR", False),
    ProviderMetricSpec("RAR", ("runs above replacement",), "RAR", None, True, "RAR", False),
    ProviderMetricSpec("Spd", ("speed score",), "Spd", None, True, "Spd", False),
    ProviderMetricSpec("WPA", ("win probability added",), "WPA", "WPA", True, "WPA", False),
    ProviderMetricSpec("RE24", ("run expectancy 24", "base-out runs added"), "RE24", "RE24", True, "RE24", False),
    ProviderMetricSpec("REW", ("run expectancy wins",), "REW", "REW", True, "REW", False),
    ProviderMetricSpec("WPA/LI", ("wpa per li", "context-neutral wpa"), "WPA/LI", "WPA/LI", True, "WPA/LI", False),
    ProviderMetricSpec("Clutch", ("clutch",), "Clutch", "Clutch", True, "Clutch", False),
    ProviderMetricSpec("AVG+", ("adjusted batting average",), "AVG+", None, True, "AVG+", True),
    ProviderMetricSpec("OBP+", ("adjusted on-base percentage",), "OBP+", None, True, "OBP+", True),
    ProviderMetricSpec("SLG+", ("adjusted slugging percentage",), "SLG+", None, True, "SLG+", True),
    ProviderMetricSpec("ISO+", ("adjusted isolated power",), "ISO+", None, True, "ISO+", True),
    ProviderMetricSpec("BABIP+", ("adjusted babip",), "BABIP+", None, True, "BABIP+", True),
    ProviderMetricSpec("Pull%", ("pull rate",), "Pull%", "Pull%", True, "Pull%", True),
    ProviderMetricSpec("Cent%", ("center-field rate", "up-the-middle rate", "center rate"), "Cent%", "Cent%", True, "Cent%", True),
    ProviderMetricSpec("Oppo%", ("opposite-field rate", "oppo rate"), "Oppo%", "Oppo%", True, "Oppo%", True),
    ProviderMetricSpec("Soft%", ("soft contact rate", "soft-hit rate"), "Soft%", "Soft%", False, "Soft%", True),
    ProviderMetricSpec("Med%", ("medium contact rate",), "Med%", "Med%", True, "Med%", True),
    ProviderMetricSpec("Hard%", ("hard contact rate",), "Hard%", "Hard%", True, "Hard%", True),
    ProviderMetricSpec("CSW%", ("called strikes plus whiffs", "called strike plus whiff rate"), "CSW%", "CSW%", True, "CSW%", True),
    ProviderMetricSpec("FIP", ("fielding independent pitching",), None, "FIP", False, "FIP", True),
    ProviderMetricSpec("xFIP", ("expected fip",), None, "xFIP", False, "xFIP", True),
    ProviderMetricSpec("tERA", ("true era",), None, "tERA", False, "tERA", True),
    ProviderMetricSpec("SIERA", ("skill-interactive era",), None, "SIERA", False, "SIERA", True),
    ProviderMetricSpec("ERA", ("earned run average",), None, "ERA", False, "ERA", True),
    ProviderMetricSpec("WHIP", ("walks plus hits per inning pitched",), None, "WHIP", False, "WHIP", True),
    ProviderMetricSpec("LOB%", ("left on base percentage", "strand rate"), None, "LOB%", True, "LOB%", True),
    ProviderMetricSpec("K/9", ("strikeouts per nine",), None, "K/9", True, "K/9", True),
    ProviderMetricSpec("BB/9", ("walks per nine",), None, "BB/9", False, "BB/9", True),
    ProviderMetricSpec("HR/9", ("home runs per nine",), None, "HR/9", False, "HR/9", True),
    ProviderMetricSpec("K/BB", ("strikeout-to-walk ratio", "k bb ratio"), None, "K/BB", True, "K/BB", True),
    ProviderMetricSpec("K-BB%", ("strikeout minus walk rate", "k minus bb percentage"), None, "K-BB%", True, "K-BB%", True),
    ProviderMetricSpec("ERA-", ("adjusted era",), None, "ERA-", False, "ERA-", True),
    ProviderMetricSpec("FIP-", ("adjusted fip",), None, "FIP-", False, "FIP-", True),
    ProviderMetricSpec("xFIP-", ("adjusted xfip",), None, "xFIP-", False, "xFIP-", True),
    ProviderMetricSpec("RA9-WAR", ("ra9 war",), None, "RA9-WAR", True, "RA9-WAR", False),
    ProviderMetricSpec("xERA", ("expected era",), None, "xERA", False, "xERA", True),
    ProviderMetricSpec("Stuff+", ("stuff plus",), None, "Stuff+", True, "Stuff+", False),
    ProviderMetricSpec("Location+", ("location plus",), None, "Location+", True, "Location+", False),
    ProviderMetricSpec("Pitching+", ("pitching plus",), None, "Pitching+", True, "Pitching+", False),
)


@dataclass(slots=True)
class ProviderMetricQuery:
    metric: ProviderMetricSpec
    season: int
    wants_current: bool
    sort_desc: bool
    descriptor: str
    wants_comparison: bool
    group_preference: str | None
    team_filter: str | None
    minimum_starts: int | None


class ProviderMetricResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.catalog = MetricCatalog.load(settings.project_root)
        self._cache: dict[tuple[str, int, bool], list[dict[str, Any]]] = {}

    def build_snippet(self, question: str) -> EvidenceSnippet | None:
        query = parse_provider_metric_query(question, self.catalog, self.settings.live_season)
        if query is None:
            return None
        rows = fetch_provider_rows(query, self._cache)
        if not rows:
            return build_provider_gap_snippet(query)
        summary = build_provider_summary(query, rows, self.settings.live_season)
        source_title = (
            f"{query.season} {query.metric.label} leaderboard"
            if not query.wants_current
            else f"{query.season} current {query.metric.label} leaderboard"
        )
        return EvidenceSnippet(
            source="FanGraphs via pybaseball",
            title=source_title,
            citation="pybaseball FanGraphs batting_stats/pitching_stats season leaderboards",
            summary=summary,
            payload={
                "analysis_type": "provider_metric_leaderboard",
                "metric": query.metric.metric_name,
                "season": query.season,
                "wants_current": query.wants_current,
                "wants_comparison": query.wants_comparison,
                "leaders": rows,
            },
        )


def parse_provider_metric_query(question: str, catalog: MetricCatalog, live_season: int | None) -> ProviderMetricQuery | None:
    lowered = question.lower()
    if "team" in lowered or "roster" in lowered:
        return None
    if any(hint in lowered for hint in SINGLE_GAME_HINTS):
        return None
    metric = find_provider_metric(lowered, catalog)
    if metric is None:
        return None
    if not looks_like_provider_leader_query(lowered):
        return None
    current_season = live_season or date.today().year
    referenced_season = extract_referenced_season(question, current_season)
    season = referenced_season or current_season
    wants_current = mentions_current_scope(lowered) or referenced_season is None
    ranking_intent = detect_ranking_intent(
        lowered,
        higher_is_better=metric.higher_is_better,
        fallback_label="leader",
    )
    descriptor = ranking_intent.descriptor if ranking_intent is not None else "leader"
    sort_desc = ranking_intent.sort_desc if ranking_intent is not None else metric.higher_is_better
    wants_comparison = any(term in lowered for term in ("compare", "compared", "previous", "historical", "same point", "at this point in the season"))
    group_preference = infer_group_preference(lowered)
    minimum_starts = extract_minimum_qualifier(question, ("start", "starts", "gs"))
    if minimum_starts is not None and group_preference is None:
        group_preference = "pitching"
    if minimum_starts is not None and not mentions_current_scope(lowered) and referenced_season is None:
        wants_current = False
    team_filter = extract_team_filter(lowered)
    return ProviderMetricQuery(
        metric=metric,
        season=season,
        wants_current=wants_current,
        sort_desc=sort_desc,
        descriptor=descriptor,
        wants_comparison=wants_comparison,
        group_preference=group_preference,
        team_filter=team_filter,
        minimum_starts=minimum_starts,
    )


def find_provider_metric(lowered_question: str, catalog: MetricCatalog) -> ProviderMetricSpec | None:
    exact_metric_names = {metric.name for metric in catalog.search(lowered_question, limit=5)}
    best_match: tuple[int, ProviderMetricSpec] | None = None
    for metric in SUPPORTED_PROVIDER_METRICS:
        if metric.metric_name == "Bat" and "bat speed" in lowered_question:
            continue
        score = 0
        if metric.metric_name in exact_metric_names:
            score += 20
        for alias in (metric.metric_name, *metric.aliases):
            if contains_metric_term(lowered_question, alias):
                score = max(score, len(alias))
        if score and (best_match is None or score > best_match[0]):
            best_match = (score, metric)
    return best_match[1] if best_match else None


def looks_like_provider_leader_query(lowered_question: str) -> bool:
    return looks_like_leaderboard_question(lowered_question)


def fetch_provider_rows(query: ProviderMetricQuery, cache: dict[tuple[str, int, bool], list[dict[str, Any]]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    if query.metric.batting_column and query.group_preference != "pitching":
        candidates.extend(
            fetch_provider_group_rows(
                "batting",
                query.metric.batting_column,
                query.season,
                query.metric.qualified_only,
                cache,
                team_filter=query.team_filter,
                minimum_starts=query.minimum_starts,
            )
        )
    if query.metric.pitching_column and query.group_preference != "batting":
        candidates.extend(
            fetch_provider_group_rows(
                "pitching",
                query.metric.pitching_column,
                query.season,
                query.metric.qualified_only,
                cache,
                team_filter=query.team_filter,
                minimum_starts=query.minimum_starts,
            )
        )
    if not candidates:
        return []
    candidates.sort(key=lambda row: (float(row["metric_value"]), row["name"]), reverse=query.sort_desc)
    return candidates[:5]


def fetch_provider_group_rows(
    group: str,
    column_name: str,
    season: int,
    qualified_only: bool,
    cache: dict[tuple[str, int, bool], list[dict[str, Any]]],
    *,
    team_filter: str | None = None,
    minimum_starts: int | None = None,
) -> list[dict[str, Any]]:
    key = (group, season, qualified_only)
    if key not in cache:
        cache[key] = load_provider_group(group, season, qualified_only)
    rows = cache[key]
    qualification_threshold = estimate_qualification_threshold(group, season, cache) if qualified_only else None
    filtered = []
    for row in rows:
        if column_name not in row or row[column_name] in (None, ""):
            continue
        if team_filter and normalize_team_value(row.get("Team")) != team_filter:
            continue
        if minimum_starts is not None:
            try:
                starts_value = float(row.get("GS") or 0)
            except (TypeError, ValueError):
                continue
            if starts_value < minimum_starts:
                continue
        if qualification_threshold is not None and not meets_qualification(row, group, qualification_threshold):
            continue
        try:
            metric_value = float(row[column_name])
        except (TypeError, ValueError):
            continue
        filtered.append(
            {
                "name": str(row.get("Name") or "").strip(),
                "team": str(row.get("Team") or "").strip(),
                "season": season,
                "group": group,
                "metric_value": metric_value,
                "starts": safe_int(row.get("GS")),
            }
        )
    return filtered


def load_provider_group(group: str, season: int, qualified_only: bool) -> list[dict[str, Any]]:
    if group == "batting":
        return load_batting_stats(season, season)
    return load_pitching_stats(season, season)


def build_provider_summary(query: ProviderMetricQuery, rows: list[dict[str, Any]], live_season: int | None) -> str:
    leader = rows[0]
    season_label = "current" if query.wants_current and query.season == (live_season or date.today().year) else str(query.season)
    if query.descriptor == "leader":
        summary = (
            f"Using public FanGraphs leaderboards via pybaseball, the {season_label} {query.metric.label} leader "
            f"is {leader['name']} ({leader['team']}) at {format_provider_value(leader['metric_value'])}."
        )
    else:
        summary = (
            f"Using public FanGraphs leaderboards via pybaseball, the {season_label} {query.descriptor} "
            f"{query.metric.label} mark belongs to {leader['name']} ({leader['team']}) at {format_provider_value(leader['metric_value'])}."
        )
    trailing_rows = rows[1:4]
    if trailing_rows:
        trailing = "; ".join(
            f"{row['name']} ({row['team']}) {format_provider_value(row['metric_value'])}"
            for row in trailing_rows
        )
        summary = f"{summary} Next on the board: {trailing}."
    if query.team_filter:
        summary = f"{summary} Team filter: {query.team_filter}."
    if query.minimum_starts is not None:
        summary = f"{summary} Qualification: at least {query.minimum_starts} starts."
    if query.wants_comparison:
        summary = (
            f"{summary} Same-point-in-season comparison snapshots for {query.metric.label} are not yet synced locally, "
            "so this answers the current leaderboard half of the request only."
        )
    return summary


def build_provider_gap_snippet(query: ProviderMetricQuery) -> EvidenceSnippet:
    request_bits = []
    if query.team_filter:
        request_bits.append(f"team filter {query.team_filter}")
    if query.minimum_starts is not None:
        request_bits.append(f"minimum {query.minimum_starts} starts")
    if query.wants_comparison:
        request_bits.append("comparison framing")
    request_shape = ", ".join(request_bits) if request_bits else "leaderboard filters"
    return EvidenceSnippet(
        source="Provider Metric Planner",
        title=f"{query.metric.label} provider gap",
        citation="pybaseball provider metric planner",
        summary=(
            f"I understand this as a provider-backed {query.metric.label} leaderboard request"
            f"{f' with {request_shape}' if request_shape else ''}. "
            f"The planner can parse that shape, but the underlying public provider tables did not return usable rows in "
            f"this environment for season {query.season}. When provider scrapes fail, the bot should answer with a "
            "clear source gap instead of pretending the local database is empty."
        ),
        payload={
            "analysis_type": "contextual_source_gap",
            "metric": query.metric.label,
            "context": request_shape or "provider leaderboard",
        },
    )


def format_provider_value(value: float) -> str:
    if abs(value) >= 10:
        return f"{value:.1f}"
    return f"{value:.3f}".rstrip("0").rstrip(".")


def estimate_qualification_threshold(
    group: str,
    season: int,
    cache: dict[tuple[str, int, bool], list[dict[str, Any]]],
) -> float | None:
    batting_key = ("batting", season, False)
    if batting_key not in cache:
        cache[batting_key] = load_provider_group("batting", season, False)
    batting_rows = cache.get(batting_key, [])
    if not batting_rows:
        return None
    try:
        team_games = max(float(row.get("G") or 0) for row in batting_rows)
    except (TypeError, ValueError):
        return None
    if team_games <= 0:
        return None
    if group == "batting":
        return team_games * 3.1
    return team_games * 1.0


def meets_qualification(row: dict[str, Any], group: str, threshold: float) -> bool:
    stat_key = "PA" if group == "batting" else "IP"
    try:
        value = float(row.get(stat_key) or 0)
    except (TypeError, ValueError):
        return False
    return value >= threshold


def infer_group_preference(lowered_question: str) -> str | None:
    if any(hint in lowered_question for hint in PITCHING_GROUP_HINTS):
        return "pitching"
    if any(hint in lowered_question for hint in BATTING_GROUP_HINTS):
        return "batting"
    return None


def extract_team_filter(lowered_question: str) -> str | None:
    alias_map = build_team_alias_to_code()
    best_match: tuple[int, str] | None = None
    for alias, code in alias_map.items():
        pattern = rf"(?<![a-z]){re.escape(alias)}(?![a-z])"
        if re.search(pattern, lowered_question) is None:
            continue
        score = len(alias)
        if best_match is None or score > best_match[0]:
            best_match = (score, code)
    return best_match[1] if best_match else None


def build_team_alias_to_code() -> dict[str, str]:
    alias_map: dict[str, str] = {}
    preferred_code_by_name: dict[str, str] = {}
    for code, name in TEAM_NAMES.items():
        previous = preferred_code_by_name.get(name)
        if previous is None or len(code) < len(previous):
            preferred_code_by_name[name] = code
    for name, code in preferred_code_by_name.items():
        words = name.casefold().split()
        alias_map[name.casefold()] = code
        if words:
            alias_map[words[-1]] = code
        if len(words) >= 2:
            alias_map[" ".join(words[-2:])] = code
            alias_map[words[0]] = code
    return alias_map


def normalize_team_value(value: Any) -> str:
    raw = str(value or "").strip().casefold()
    if not raw:
        return ""
    alias_map = build_team_alias_to_code()
    if raw in alias_map:
        return alias_map[raw]
    for alias, code in alias_map.items():
        if raw == code.casefold():
            return code
    return raw.upper()


def safe_int(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def contains_metric_term(query_lower: str, term: str) -> bool:
    needle = term.strip().lower()
    if not needle:
        return False
    compact = re.sub(r"[^a-z0-9%+/.-]+", "", needle)
    if compact and len(compact) <= 5:
        pattern = rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])"
        return re.search(pattern, query_lower) is not None
    return needle in query_lower
