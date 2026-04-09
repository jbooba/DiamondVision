from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .comparison_context import (
    build_percentile_blurb,
    comparison_gap_sentence,
    format_comparison_value,
    percentile_band,
    percentile_for_population,
)
from .config import Settings
from .live import LiveStatsClient
from .metrics import MetricCatalog
from .models import EvidenceSnippet
from .person_query import choose_best_person_match
from .provider_metrics import (
    ProviderMetricSpec,
    contains_metric_term,
    estimate_qualification_threshold,
    find_provider_metric,
    infer_group_preference,
    meets_qualification,
)
from .pybaseball_adapter import load_batting_stats, load_pitching_stats
from .query_utils import extract_explicit_year, normalize_person_name, ordinal
from .storage import table_exists
from .team_evaluator import safe_float, safe_int
from .team_season_compare import compute_ops


SKIP_HINTS = {
    "team",
    "roster",
    "lineup",
    "rotation",
    "bullpen",
    "pitching staff",
    "pitching roster",
    "previous season starts",
    "prior season starts",
}
CURRENT_SCOPE_HINTS = {"this season", "this year", "current", "so far", "season to date"}
PRONOUN_HINTS = {"his", "her", "their"}
COMPARISON_PATTERNS = (
    re.compile(r"^\s*compare\s+(.+?)\s+(?:to|vs\.?|versus|with|and)\s+(.+?)\s*[?!.]*$", re.IGNORECASE),
    re.compile(r"^\s*how\s+does\s+(.+?)\s+compare\s+to\s+(.+?)\s*[?!.]*$", re.IGNORECASE),
    re.compile(r"^\s*(?:is|was|are|were)\s+(.+?)\s+(better|worse)\s+than\s+(.+?)\s*[?!.]*$", re.IGNORECASE),
    re.compile(r"^\s*(.+?)\s+vs\.?\s+(.+?)\s*[?!.]*$", re.IGNORECASE),
)


@dataclass(frozen=True, slots=True)
class CoreMetricSpec:
    key: str
    label: str
    role: str
    higher_is_better: bool
    digits: int
    aliases: tuple[str, ...]
    integer: bool = False


SUPPORTED_CORE_METRICS: tuple[CoreMetricSpec, ...] = (
    CoreMetricSpec("ops", "OPS", "hitting", True, 3, ("ops",)),
    CoreMetricSpec("avg", "AVG", "hitting", True, 3, ("avg", "ba", "batting average")),
    CoreMetricSpec("obp", "OBP", "hitting", True, 3, ("obp", "on-base percentage")),
    CoreMetricSpec("slg", "SLG", "hitting", True, 3, ("slg", "slugging percentage")),
    CoreMetricSpec("home_runs", "HR", "hitting", True, 0, ("home runs", "homeruns", "homers", "hr"), integer=True),
    CoreMetricSpec("hits", "Hits", "hitting", True, 0, ("hits", "hit"), integer=True),
    CoreMetricSpec("runs_batted_in", "RBI", "hitting", True, 0, ("rbi", "rbis", "runs batted in"), integer=True),
    CoreMetricSpec("walks", "BB", "hitting", True, 0, ("walks", "walk", "bb"), integer=True),
    CoreMetricSpec("strikeouts", "SO", "hitting", False, 0, ("strikeouts", "strikeout"), integer=True),
    CoreMetricSpec("stolen_bases", "SB", "hitting", True, 0, ("stolen bases", "stolen base", "sb", "steals"), integer=True),
    CoreMetricSpec("era", "ERA", "pitching", False, 2, ("era", "earned run average")),
    CoreMetricSpec("whip", "WHIP", "pitching", False, 2, ("whip",)),
    CoreMetricSpec("strikeouts_per_9", "K/9", "pitching", True, 1, ("k/9", "strikeouts per nine")),
    CoreMetricSpec("walks_per_9", "BB/9", "pitching", False, 1, ("bb/9", "walks per nine")),
    CoreMetricSpec("home_runs_per_9", "HR/9", "pitching", False, 1, ("hr/9", "home runs per nine")),
    CoreMetricSpec("innings", "IP", "pitching", True, 1, ("innings", "ip")),
    CoreMetricSpec("wins", "Wins", "pitching", True, 0, ("wins", "win"), integer=True),
    CoreMetricSpec("saves", "Saves", "pitching", True, 0, ("saves", "save", "sv"), integer=True),
    CoreMetricSpec("pitcher_strikeouts", "SO", "pitching", True, 0, ("pitcher strikeouts", "pitching strikeouts"), integer=True),
)

PROVIDER_TO_CORE_METRIC = {
    "OPS": "ops",
    "BA": "avg",
    "OBP": "obp",
    "SLG": "slg",
    "ERA": "era",
    "WHIP": "whip",
    "K/9": "strikeouts_per_9",
    "BB/9": "walks_per_9",
    "HR/9": "home_runs_per_9",
}


@dataclass(slots=True)
class PlayerSeasonReference:
    player_query: str
    player_name: str
    season: int
    live_player_id: int | None
    lahman_player_id: str | None


@dataclass(slots=True)
class PlayerSeasonComparisonQuery:
    left: PlayerSeasonReference
    right: PlayerSeasonReference
    comparator: str
    provider_metric: ProviderMetricSpec | None
    core_metric: CoreMetricSpec | None
    group_preference: str | None


@dataclass(slots=True)
class PlayerSeasonSnapshot:
    display_name: str
    player_name: str
    season: int
    scope: str
    team: str
    role: str
    games: int
    plate_appearances: int
    avg: float | None
    obp: float | None
    slg: float | None
    ops: float | None
    hits: int
    home_runs: int
    runs_batted_in: int
    walks: int
    strikeouts: int
    stolen_bases: int
    innings: float | None
    wins: int
    losses: int
    saves: int
    era: float | None
    whip: float | None
    strikeouts_per_9: float | None
    walks_per_9: float | None
    home_runs_per_9: float | None
    pitcher_strikeouts: int


@dataclass(slots=True)
class ProviderPlayerMetricSnapshot:
    display_name: str
    player_name: str
    season: int
    team: str
    group: str
    metric_label: str
    metric_value: float
    higher_is_better: bool
    season_rank: int | None
    peer_count: int | None
    percentile: float | None
    context_1: str
    context_2: str


class PlayerSeasonComparisonResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)
        self.catalog = MetricCatalog.load(settings.project_root)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        current_season = self.settings.live_season or date.today().year
        query = parse_player_season_comparison_query(
            connection,
            question,
            self.live_client,
            self.catalog,
            current_season,
        )
        if query is None:
            return None

        if query.provider_metric is not None:
            provider_snippet = self._build_provider_metric_snippet(query)
            if provider_snippet is not None:
                return provider_snippet

        fallback_metric = query.core_metric or fallback_core_metric(query.provider_metric)
        if query.provider_metric is not None and fallback_metric is None:
            return None

        left_snapshot = self._build_snapshot(connection, query.left, current_season)
        right_snapshot = self._build_snapshot(connection, query.right, current_season)
        if left_snapshot is None or right_snapshot is None:
            return None

        role = choose_comparison_role(
            left_snapshot,
            right_snapshot,
            fallback_metric or default_metric_for_snapshots(left_snapshot, right_snapshot, query.group_preference),
            query.group_preference,
        )
        if role is None:
            return None
        metric = fallback_metric if fallback_metric is not None and fallback_metric.role == role else default_metric_for_role(role)
        if metric is None:
            return None

        left_value = snapshot_metric_value(left_snapshot, metric)
        right_value = snapshot_metric_value(right_snapshot, metric)
        if left_value is None or right_value is None:
            return None

        historical_percentiles = historical_percentile_pair(connection, metric, left_value, right_value)
        summary = build_core_comparison_summary(
            query,
            metric=metric,
            role=role,
            left=left_snapshot,
            right=right_snapshot,
            left_percentile=historical_percentiles[0],
            right_percentile=historical_percentiles[1],
        )
        mode = comparison_mode(query.left.season, query.right.season, current_season)
        return EvidenceSnippet(
            source="Player Season Comparison",
            title=f"{query.left.player_name} vs {query.right.player_name}",
            citation="Lahman batting/pitching tables with MLB Stats API current-season player snapshots",
            summary=summary,
            payload={
                "analysis_type": "player_season_comparison",
                "mode": mode,
                "role": role,
                "metric": metric.label,
                "rows": [
                    core_snapshot_row(left_snapshot, metric, historical_percentiles[0]),
                    core_snapshot_row(right_snapshot, metric, historical_percentiles[1]),
                ],
            },
        )

    def _build_provider_metric_snippet(self, query: PlayerSeasonComparisonQuery) -> EvidenceSnippet | None:
        metric = query.provider_metric
        if metric is None:
            return None
        left_snapshot = build_provider_player_metric_snapshot(query.left, metric, query.group_preference)
        right_snapshot = build_provider_player_metric_snapshot(query.right, metric, query.group_preference)
        if left_snapshot is None or right_snapshot is None:
            return None
        summary = build_provider_comparison_summary(query, metric, left_snapshot, right_snapshot)
        mode = comparison_mode(query.left.season, query.right.season, self.settings.live_season or date.today().year)
        return EvidenceSnippet(
            source="Player Season Comparison",
            title=f"{query.left.player_name} vs {query.right.player_name} {metric.label}",
            citation="pybaseball FanGraphs batting_stats/pitching_stats season rows plus season leaderboard context",
            summary=summary,
            payload={
                "analysis_type": "player_season_comparison",
                "mode": mode,
                "role": left_snapshot.group,
                "metric": metric.label,
                "rows": [
                    provider_snapshot_row(left_snapshot),
                    provider_snapshot_row(right_snapshot),
                ],
            },
        )

    def _build_snapshot(
        self,
        connection,
        reference: PlayerSeasonReference,
        current_season: int,
    ) -> PlayerSeasonSnapshot | None:
        if reference.season == current_season:
            live_snapshot = build_live_player_snapshot(self.live_client, reference)
            if live_snapshot is not None:
                return live_snapshot
        return build_historical_player_snapshot(connection, reference)


def parse_player_season_comparison_query(
    connection,
    question: str,
    live_client: LiveStatsClient,
    catalog: MetricCatalog,
    current_season: int,
) -> PlayerSeasonComparisonQuery | None:
    lowered = question.lower()
    if not any(token in lowered for token in ("compare", " vs ", " versus ", " better than ", " worse than ", "how does")):
        return None
    if any(hint in lowered for hint in SKIP_HINTS):
        return None
    split = split_player_comparison_question(question)
    if split is None:
        return None
    left_phrase, right_phrase, comparator = split
    provider_metric = find_provider_metric(lowered, catalog)
    core_metric = find_core_metric(lowered, provider_metric)
    group_preference = infer_group_preference(lowered)
    metric_terms = metric_terms_for_query(provider_metric, core_metric)
    left_reference = resolve_player_reference(
        connection,
        left_phrase,
        live_client,
        current_season=current_season,
        metric_terms=metric_terms,
    )
    if left_reference is None:
        return None
    right_reference = resolve_player_reference(
        connection,
        right_phrase,
        live_client,
        current_season=current_season,
        metric_terms=metric_terms,
        fallback_name=left_reference.player_name,
    )
    if right_reference is None:
        return None
    return PlayerSeasonComparisonQuery(
        left=left_reference,
        right=right_reference,
        comparator=comparator,
        provider_metric=provider_metric,
        core_metric=core_metric,
        group_preference=group_preference,
    )


def split_player_comparison_question(question: str) -> tuple[str, str, str] | None:
    stripped = question.strip()
    for pattern in COMPARISON_PATTERNS:
        match = pattern.match(stripped)
        if not match:
            continue
        if pattern.groups == 3:
            return match.group(1), match.group(3), str(match.group(2) or "compare").lower()
        return match.group(1), match.group(2), "compare"
    return None


def find_core_metric(lowered_question: str, provider_metric: ProviderMetricSpec | None) -> CoreMetricSpec | None:
    if provider_metric is not None:
        fallback_key = PROVIDER_TO_CORE_METRIC.get(provider_metric.metric_name)
        if fallback_key:
            return next((metric for metric in SUPPORTED_CORE_METRICS if metric.key == fallback_key), None)
    best_match: tuple[int, CoreMetricSpec] | None = None
    for metric in SUPPORTED_CORE_METRICS:
        score = 0
        for alias in metric.aliases:
            if contains_metric_term(lowered_question, alias):
                score = max(score, len(alias))
        if score and (best_match is None or score > best_match[0]):
            best_match = (score, metric)
    return best_match[1] if best_match else None


def metric_terms_for_query(provider_metric: ProviderMetricSpec | None, core_metric: CoreMetricSpec | None) -> tuple[str, ...]:
    terms: list[str] = []
    if provider_metric is not None:
        terms.extend([provider_metric.metric_name, *provider_metric.aliases])
    if core_metric is not None:
        terms.extend(core_metric.aliases)
    return tuple(sorted({term.strip() for term in terms if term.strip()}, key=len, reverse=True))


def resolve_player_reference(
    connection,
    phrase: str,
    live_client: LiveStatsClient,
    *,
    current_season: int,
    metric_terms: tuple[str, ...],
    fallback_name: str | None = None,
) -> PlayerSeasonReference | None:
    season = extract_explicit_year(phrase)
    lowered = phrase.lower()
    if season is None and any(hint in lowered for hint in CURRENT_SCOPE_HINTS):
        season = current_season
    candidate = clean_player_reference_phrase(phrase, metric_terms)
    if fallback_name and candidate.lower() in PRONOUN_HINTS:
        candidate = fallback_name
    if not candidate and fallback_name and any(pronoun in lowered.split() for pronoun in PRONOUN_HINTS):
        candidate = fallback_name
    if not candidate:
        return None

    live_people = live_client.search_people(candidate)
    live_person = choose_best_person_match(live_people, candidate) if live_people else None
    lahman_person = find_lahman_person(connection, candidate, season or current_season)
    if live_person is None and lahman_person is None:
        return None
    player_name = (
        str(live_person.get("fullName") or "").strip()
        if live_person is not None
        else str(lahman_person.get("player_name") or candidate).strip()
    )
    if not player_name:
        return None
    return PlayerSeasonReference(
        player_query=candidate,
        player_name=player_name,
        season=season or current_season,
        live_player_id=int(live_person.get("id") or 0) if live_person is not None and live_person.get("id") else None,
        lahman_player_id=str(lahman_person.get("playerid") or "") if lahman_person is not None else None,
    )


def clean_player_reference_phrase(phrase: str, metric_terms: tuple[str, ...]) -> str:
    cleaned = phrase.strip(" ?.!,'\"")
    cleaned = re.sub(r"'s\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\b(?:through\s+)?(?:the\s+)?(?:first|last|previous|prior)\s+(?:\d+|[a-z]+)\s+(?:games?|starts?)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\b(?:games?|starts?)\s+of\s+(?:their|his|her)?\s*(?:season|year)?\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    for term in metric_terms:
        if term:
            cleaned = re.sub(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(18\d{2}|19\d{2}|20\d{2})\b", " ", cleaned)
    cleaned = re.sub(
        r"\b(?:compare|how|does|did|is|was|are|were|to|vs|versus|with|and|than|the|a|an|season|stats?|performance|year|current|this|so|far|through|over|across|for|in|of)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    if not cleaned or (" " not in cleaned and cleaned.lower() not in PRONOUN_HINTS):
        return ""
    return " ".join(part.capitalize() for part in cleaned.split())


def find_lahman_person(connection, candidate: str, season: int) -> dict[str, Any] | None:
    if not table_exists(connection, "lahman_people"):
        return None
    normalized_candidate = normalize_person_name(candidate)
    rows = connection.execute(
        """
        SELECT
            playerid,
            namefirst,
            namelast,
            debut,
            finalgame
        FROM lahman_people
        WHERE lower(namefirst || ' ' || namelast) = ?
           OR lower(namegiven) = ?
           OR lower(namefirst || ' ' || namelast) LIKE ?
        """,
        (normalized_candidate, normalized_candidate, f"{normalized_candidate}%"),
    ).fetchall()
    if not rows:
        return None
    best_row = None
    best_score = None
    for row in rows:
        full_name = normalize_person_name(f"{row['namefirst']} {row['namelast']}")
        debut_year = int(str(row["debut"] or "9999")[:4]) if row["debut"] else 9999
        finalgame = str(row["finalgame"] or "")
        final_year = int(finalgame[:4]) if finalgame[:4].isdigit() else 9999
        active_for_season = debut_year <= season <= final_year
        score = (
            0 if full_name == normalized_candidate else 1,
            0 if active_for_season else 1,
            abs(season - debut_year),
            full_name,
        )
        if best_score is None or score < best_score:
            best_score = score
            best_row = row
    if best_row is None:
        return None
    return {
        "playerid": best_row["playerid"],
        "player_name": f"{best_row['namefirst']} {best_row['namelast']}".strip(),
    }


def build_live_player_snapshot(live_client: LiveStatsClient, reference: PlayerSeasonReference) -> PlayerSeasonSnapshot | None:
    snapshot = live_client.player_season_snapshot(reference.player_name, reference.season)
    if snapshot is None:
        return None
    hitting = snapshot.get("hitting", {}) or {}
    pitching = snapshot.get("pitching", {}) or {}
    team = str(snapshot.get("current_team") or "").strip()
    games = safe_int(hitting.get("gamesPlayed")) or safe_int(pitching.get("gamesPlayed")) or 0
    plate_appearances = safe_int(hitting.get("plateAppearances")) or 0
    innings = safe_float(pitching.get("inningsPitched"))
    role = infer_snapshot_role(plate_appearances, innings)
    return PlayerSeasonSnapshot(
        display_name=f"{reference.season} {reference.player_name}",
        player_name=reference.player_name,
        season=reference.season,
        scope="season to date",
        team=team,
        role=role,
        games=games,
        plate_appearances=plate_appearances,
        avg=safe_float(hitting.get("avg")),
        obp=safe_float(hitting.get("obp")),
        slg=safe_float(hitting.get("slg")),
        ops=safe_float(hitting.get("ops")),
        hits=safe_int(hitting.get("hits")) or 0,
        home_runs=safe_int(hitting.get("homeRuns")) or 0,
        runs_batted_in=safe_int(hitting.get("rbi")) or 0,
        walks=safe_int(hitting.get("baseOnBalls")) or 0,
        strikeouts=safe_int(hitting.get("strikeOuts")) or 0,
        stolen_bases=safe_int(hitting.get("stolenBases")) or 0,
        innings=innings,
        wins=safe_int(pitching.get("wins")) or 0,
        losses=safe_int(pitching.get("losses")) or 0,
        saves=safe_int(pitching.get("saves")) or 0,
        era=safe_float(pitching.get("era")),
        whip=safe_float(pitching.get("whip")),
        strikeouts_per_9=safe_float(pitching.get("strikeoutsPer9Inn")),
        walks_per_9=safe_float(pitching.get("walksPer9Inn")),
        home_runs_per_9=safe_float(pitching.get("homeRunsPer9")),
        pitcher_strikeouts=safe_int(pitching.get("strikeOuts")) or 0,
    )


def build_historical_player_snapshot(connection, reference: PlayerSeasonReference) -> PlayerSeasonSnapshot | None:
    if reference.lahman_player_id is None or not (table_exists(connection, "lahman_batting") and table_exists(connection, "lahman_pitching")):
        return None
    batting_row = connection.execute(
        """
        SELECT
            SUM(CAST(COALESCE(g, '0') AS INTEGER)) AS g,
            SUM(CAST(COALESCE(ab, '0') AS INTEGER)) AS ab,
            SUM(CAST(COALESCE(h, '0') AS INTEGER)) AS h,
            SUM(CAST(COALESCE(c_2b, '0') AS INTEGER)) AS d2,
            SUM(CAST(COALESCE(c_3b, '0') AS INTEGER)) AS d3,
            SUM(CAST(COALESCE(hr, '0') AS INTEGER)) AS hr,
            SUM(CAST(COALESCE(rbi, '0') AS INTEGER)) AS rbi,
            SUM(CAST(COALESCE(sb, '0') AS INTEGER)) AS sb,
            SUM(CAST(COALESCE(bb, '0') AS INTEGER)) AS bb,
            SUM(CAST(COALESCE(so, '0') AS INTEGER)) AS so,
            SUM(CAST(COALESCE(hbp, '0') AS INTEGER)) AS hbp,
            SUM(CAST(COALESCE(sf, '0') AS INTEGER)) AS sf,
            SUM(CAST(COALESCE(sh, '0') AS INTEGER)) AS sh,
            GROUP_CONCAT(DISTINCT teamid) AS team_ids
        FROM lahman_batting
        WHERE playerid = ? AND yearid = ?
        """,
        (reference.lahman_player_id, reference.season),
    ).fetchone()
    pitching_row = connection.execute(
        """
        SELECT
            SUM(CAST(COALESCE(g, '0') AS INTEGER)) AS g,
            SUM(CAST(COALESCE(w, '0') AS INTEGER)) AS w,
            SUM(CAST(COALESCE(l, '0') AS INTEGER)) AS l,
            SUM(CAST(COALESCE(sv, '0') AS INTEGER)) AS sv,
            SUM(CAST(COALESCE(ipouts, '0') AS INTEGER)) AS ipouts,
            SUM(CAST(COALESCE(h, '0') AS INTEGER)) AS h,
            SUM(CAST(COALESCE(er, '0') AS INTEGER)) AS er,
            SUM(CAST(COALESCE(hr, '0') AS INTEGER)) AS hr,
            SUM(CAST(COALESCE(bb, '0') AS INTEGER)) AS bb,
            SUM(CAST(COALESCE(so, '0') AS INTEGER)) AS so,
            GROUP_CONCAT(DISTINCT teamid) AS team_ids
        FROM lahman_pitching
        WHERE playerid = ? AND yearid = ?
        """,
        (reference.lahman_player_id, reference.season),
    ).fetchone()
    if batting_row is None and pitching_row is None:
        return None

    at_bats = int((batting_row["ab"] if batting_row is not None else 0) or 0)
    hits = int((batting_row["h"] if batting_row is not None else 0) or 0)
    doubles = int((batting_row["d2"] if batting_row is not None else 0) or 0)
    triples = int((batting_row["d3"] if batting_row is not None else 0) or 0)
    home_runs = int((batting_row["hr"] if batting_row is not None else 0) or 0)
    walks = int((batting_row["bb"] if batting_row is not None else 0) or 0)
    hit_by_pitch = int((batting_row["hbp"] if batting_row is not None else 0) or 0)
    sacrifice_flies = int((batting_row["sf"] if batting_row is not None else 0) or 0)
    sacrifice_hits = int((batting_row["sh"] if batting_row is not None else 0) or 0)
    plate_appearances = at_bats + walks + hit_by_pitch + sacrifice_flies + sacrifice_hits
    innings_outs = int((pitching_row["ipouts"] if pitching_row is not None else 0) or 0)
    innings = (innings_outs / 3.0) if innings_outs else None
    role = infer_snapshot_role(plate_appearances, innings)
    team = team_string_for_snapshot(batting_row, pitching_row)
    obp = compute_obp(hits, at_bats, walks, hit_by_pitch, sacrifice_flies)
    slg = compute_slg(hits, doubles, triples, home_runs, at_bats)
    return PlayerSeasonSnapshot(
        display_name=f"{reference.season} {reference.player_name}",
        player_name=reference.player_name,
        season=reference.season,
        scope="full season",
        team=team,
        role=role,
        games=max(int((batting_row["g"] if batting_row is not None else 0) or 0), int((pitching_row["g"] if pitching_row is not None else 0) or 0)),
        plate_appearances=plate_appearances,
        avg=(hits / at_bats) if at_bats else None,
        obp=obp,
        slg=slg,
        ops=compute_ops(hits, doubles, triples, home_runs, at_bats, walks, hit_by_pitch, sacrifice_flies),
        hits=hits,
        home_runs=home_runs,
        runs_batted_in=int((batting_row["rbi"] if batting_row is not None else 0) or 0),
        walks=walks,
        strikeouts=int((batting_row["so"] if batting_row is not None else 0) or 0),
        stolen_bases=int((batting_row["sb"] if batting_row is not None else 0) or 0),
        innings=innings,
        wins=int((pitching_row["w"] if pitching_row is not None else 0) or 0),
        losses=int((pitching_row["l"] if pitching_row is not None else 0) or 0),
        saves=int((pitching_row["sv"] if pitching_row is not None else 0) or 0),
        era=((27.0 * int(pitching_row["er"] or 0)) / innings_outs) if innings_outs else None,
        whip=((int(pitching_row["bb"] or 0) + int(pitching_row["h"] or 0)) / innings) if innings else None,
        strikeouts_per_9=((27.0 * int(pitching_row["so"] or 0)) / innings_outs) if innings_outs else None,
        walks_per_9=((27.0 * int(pitching_row["bb"] or 0)) / innings_outs) if innings_outs else None,
        home_runs_per_9=((27.0 * int(pitching_row["hr"] or 0)) / innings_outs) if innings_outs else None,
        pitcher_strikeouts=int((pitching_row["so"] if pitching_row is not None else 0) or 0),
    )


def team_string_for_snapshot(batting_row: Any, pitching_row: Any) -> str:
    team_tokens: list[str] = []
    for row in (batting_row, pitching_row):
        if row is None:
            continue
        for part in str(row["team_ids"] or "").split(","):
            token = part.strip()
            if token and token not in team_tokens:
                team_tokens.append(token)
    return "/".join(team_tokens)


def infer_snapshot_role(plate_appearances: int, innings: float | None) -> str:
    if plate_appearances > 0 and (innings is None or innings <= 0):
        return "hitting"
    if innings and innings > 0 and plate_appearances <= 0:
        return "pitching"
    if plate_appearances > 0 and innings and innings > 0:
        return "two-way"
    return "unknown"


def choose_comparison_role(
    left: PlayerSeasonSnapshot,
    right: PlayerSeasonSnapshot,
    metric: CoreMetricSpec,
    group_preference: str | None,
) -> str | None:
    if group_preference in {"batting", "pitching"}:
        return "hitting" if group_preference == "batting" else "pitching"
    if metric.role in {"hitting", "pitching"}:
        return metric.role
    if left.role == right.role and left.role in {"hitting", "pitching"}:
        return left.role
    if left.role in {"hitting", "pitching"} and right.role == "two-way":
        return left.role
    if right.role in {"hitting", "pitching"} and left.role == "two-way":
        return right.role
    return None


def default_metric_for_snapshots(
    left: PlayerSeasonSnapshot,
    right: PlayerSeasonSnapshot,
    group_preference: str | None,
) -> CoreMetricSpec | None:
    if group_preference == "pitching":
        return default_metric_for_role("pitching")
    if group_preference == "batting":
        return default_metric_for_role("hitting")
    candidate_roles = {left.role, right.role}
    if candidate_roles <= {"hitting", "two-way"}:
        return default_metric_for_role("hitting")
    if candidate_roles <= {"pitching", "two-way"}:
        return default_metric_for_role("pitching")
    return None


def default_metric_for_role(role: str) -> CoreMetricSpec | None:
    metric_key = "ops" if role == "hitting" else "era" if role == "pitching" else None
    if metric_key is None:
        return None
    return next((metric for metric in SUPPORTED_CORE_METRICS if metric.key == metric_key), None)


def fallback_core_metric(provider_metric: ProviderMetricSpec | None) -> CoreMetricSpec | None:
    if provider_metric is None:
        return None
    metric_key = PROVIDER_TO_CORE_METRIC.get(provider_metric.metric_name)
    if metric_key is None:
        return None
    return next((metric for metric in SUPPORTED_CORE_METRICS if metric.key == metric_key), None)


def snapshot_metric_value(snapshot: PlayerSeasonSnapshot, metric: CoreMetricSpec) -> float | None:
    return safe_float(getattr(snapshot, metric.key, None))


def historical_percentile_pair(
    connection,
    metric: CoreMetricSpec,
    left_value: float | None,
    right_value: float | None,
) -> tuple[float | None, float | None]:
    values = historical_population_values(connection, metric)
    if not values:
        return None, None
    return (
        percentile_for_population(left_value, values, higher_is_better=metric.higher_is_better),
        percentile_for_population(right_value, values, higher_is_better=metric.higher_is_better),
    )


def historical_population_values(connection, metric: CoreMetricSpec) -> list[float]:
    if metric.role == "hitting":
        rows = connection.execute(
            """
            SELECT
                SUM(CAST(COALESCE(ab, '0') AS INTEGER)) AS ab,
                SUM(CAST(COALESCE(h, '0') AS INTEGER)) AS h,
                SUM(CAST(COALESCE(c_2b, '0') AS INTEGER)) AS d2,
                SUM(CAST(COALESCE(c_3b, '0') AS INTEGER)) AS d3,
                SUM(CAST(COALESCE(hr, '0') AS INTEGER)) AS hr,
                SUM(CAST(COALESCE(rbi, '0') AS INTEGER)) AS rbi,
                SUM(CAST(COALESCE(sb, '0') AS INTEGER)) AS sb,
                SUM(CAST(COALESCE(bb, '0') AS INTEGER)) AS bb,
                SUM(CAST(COALESCE(so, '0') AS INTEGER)) AS so,
                SUM(CAST(COALESCE(hbp, '0') AS INTEGER)) AS hbp,
                SUM(CAST(COALESCE(sf, '0') AS INTEGER)) AS sf
            FROM lahman_batting
            GROUP BY playerid, yearid
            HAVING SUM(CAST(COALESCE(ab, '0') AS INTEGER)) >= 200
            """
        ).fetchall()
        values: list[float] = []
        for row in rows:
            metric_value = hitter_metric_from_row(row, metric)
            if metric_value is not None:
                values.append(metric_value)
        return values

    rows = connection.execute(
        """
        SELECT
            SUM(CAST(COALESCE(ipouts, '0') AS INTEGER)) AS ipouts,
            SUM(CAST(COALESCE(w, '0') AS INTEGER)) AS w,
            SUM(CAST(COALESCE(sv, '0') AS INTEGER)) AS sv,
            SUM(CAST(COALESCE(h, '0') AS INTEGER)) AS h,
            SUM(CAST(COALESCE(er, '0') AS INTEGER)) AS er,
            SUM(CAST(COALESCE(hr, '0') AS INTEGER)) AS hr,
            SUM(CAST(COALESCE(bb, '0') AS INTEGER)) AS bb,
            SUM(CAST(COALESCE(so, '0') AS INTEGER)) AS so
        FROM lahman_pitching
        GROUP BY playerid, yearid
        HAVING SUM(CAST(COALESCE(ipouts, '0') AS INTEGER)) >= 300
        """
    ).fetchall()
    values: list[float] = []
    for row in rows:
        metric_value = pitcher_metric_from_row(row, metric)
        if metric_value is not None:
            values.append(metric_value)
    return values


def hitter_metric_from_row(row: Any, metric: CoreMetricSpec) -> float | None:
    at_bats = int(row["ab"] or 0)
    hits = int(row["h"] or 0)
    doubles = int(row["d2"] or 0)
    triples = int(row["d3"] or 0)
    home_runs = int(row["hr"] or 0)
    walks = int(row["bb"] or 0)
    hit_by_pitch = int(row["hbp"] or 0)
    sacrifice_flies = int(row["sf"] or 0)
    return {
        "ops": compute_ops(hits, doubles, triples, home_runs, at_bats, walks, hit_by_pitch, sacrifice_flies),
        "avg": (hits / at_bats) if at_bats else None,
        "obp": compute_obp(hits, at_bats, walks, hit_by_pitch, sacrifice_flies),
        "slg": compute_slg(hits, doubles, triples, home_runs, at_bats),
        "home_runs": float(home_runs),
        "hits": float(hits),
        "runs_batted_in": float(int(row["rbi"] or 0)),
        "walks": float(walks),
        "strikeouts": float(int(row["so"] or 0)),
        "stolen_bases": float(int(row["sb"] or 0)),
    }.get(metric.key)


def pitcher_metric_from_row(row: Any, metric: CoreMetricSpec) -> float | None:
    innings_outs = int(row["ipouts"] or 0)
    innings = innings_outs / 3.0 if innings_outs else None
    return {
        "innings": innings,
        "era": ((27.0 * int(row["er"] or 0)) / innings_outs) if innings_outs else None,
        "whip": ((int(row["bb"] or 0) + int(row["h"] or 0)) / innings) if innings else None,
        "strikeouts_per_9": ((27.0 * int(row["so"] or 0)) / innings_outs) if innings_outs else None,
        "walks_per_9": ((27.0 * int(row["bb"] or 0)) / innings_outs) if innings_outs else None,
        "home_runs_per_9": ((27.0 * int(row["hr"] or 0)) / innings_outs) if innings_outs else None,
        "wins": float(int(row["w"] or 0)),
        "saves": float(int(row["sv"] or 0)),
        "pitcher_strikeouts": float(int(row["so"] or 0)),
    }.get(metric.key)


def build_core_comparison_summary(
    query: PlayerSeasonComparisonQuery,
    *,
    metric: CoreMetricSpec,
    role: str,
    left: PlayerSeasonSnapshot,
    right: PlayerSeasonSnapshot,
    left_percentile: float | None,
    right_percentile: float | None,
) -> str:
    left_value = snapshot_metric_value(left, metric)
    right_value = snapshot_metric_value(right, metric)
    left_better = compare_values(left_value, right_value, metric.higher_is_better) > 0
    verdict = ""
    if query.comparator == "better":
        verdict = "Yes. " if left_better else "No. "
    elif query.comparator == "worse":
        verdict = "Yes. " if not left_better else "No. "

    if role == "hitting":
        summary = (
            f"{verdict}{left.display_name} hit {rate_text(left.avg)}/{rate_text(left.obp)}/{rate_text(left.slg)} "
            f"with {left.home_runs} HR and {left.runs_batted_in} RBI in {left.plate_appearances} PA. "
            f"{right.display_name} hit {rate_text(right.avg)}/{rate_text(right.obp)}/{rate_text(right.slg)} "
            f"with {right.home_runs} HR and {right.runs_batted_in} RBI in {right.plate_appearances} PA."
        )
    else:
        summary = (
            f"{verdict}{left.display_name} logged {format_comparison_value(left.innings, digits=1)} IP with a "
            f"{format_comparison_value(left.era, digits=2)} ERA, {format_comparison_value(left.whip, digits=2)} WHIP, "
            f"and {format_comparison_value(left.strikeouts_per_9, digits=1)} K/9. "
            f"{right.display_name} logged {format_comparison_value(right.innings, digits=1)} IP with a "
            f"{format_comparison_value(right.era, digits=2)} ERA, {format_comparison_value(right.whip, digits=2)} WHIP, "
            f"and {format_comparison_value(right.strikeouts_per_9, digits=1)} K/9."
        )

    summary = (
        f"{summary} "
        f"{comparison_gap_sentence(
            left_label=left.display_name,
            right_label=right.display_name,
            metric_label=metric.label,
            left_value=left_value,
            right_value=right_value,
            higher_is_better=metric.higher_is_better,
            digits=metric.digits,
            integer=metric.integer,
        )}"
    )
    left_blurb = build_percentile_blurb(left_percentile, f"tracked qualifying MLB {role} seasons")
    right_blurb = build_percentile_blurb(right_percentile, f"tracked qualifying MLB {role} seasons")
    if left_blurb and right_blurb:
        summary = f"{summary} Historically, {left.display_name}: {left_blurb} {right.display_name}: {right_blurb}"
    elif left_blurb or right_blurb:
        summary = f"{summary} {left_blurb or right_blurb}"
    if left.scope != "full season" or right.scope != "full season":
        summary = f"{summary} The current-season side is still a partial sample, so the comparison can move quickly."
    return summary.strip()


def build_provider_comparison_summary(
    query: PlayerSeasonComparisonQuery,
    metric: ProviderMetricSpec,
    left: ProviderPlayerMetricSnapshot,
    right: ProviderPlayerMetricSnapshot,
) -> str:
    left_better = compare_values(left.metric_value, right.metric_value, metric.higher_is_better) > 0
    verdict = ""
    if query.comparator == "better":
        verdict = "Yes. " if left_better else "No. "
    elif query.comparator == "worse":
        verdict = "Yes. " if not left_better else "No. "
    summary = (
        f"{verdict}{left.display_name} checked in at {format_comparison_value(left.metric_value, digits=3)} "
        f"{metric.label} for {left.team or 'unknown team'}. "
        f"{right.display_name} checked in at {format_comparison_value(right.metric_value, digits=3)} "
        f"{metric.label} for {right.team or 'unknown team'}."
    )
    summary = (
        f"{summary} "
        f"{comparison_gap_sentence(
            left_label=left.display_name,
            right_label=right.display_name,
            metric_label=metric.label,
            left_value=left.metric_value,
            right_value=right.metric_value,
            higher_is_better=metric.higher_is_better,
            digits=3,
        )}"
    )
    if left.season_rank and left.peer_count and right.season_rank and right.peer_count:
        summary = (
            f"{summary} In league context, {left.display_name} ranked {ordinal(left.season_rank)} of "
            f"{left.peer_count} {left.group} qualifiers in {left.season}, which reads as {percentile_band(left.percentile)}. "
            f"{right.display_name} ranked {ordinal(right.season_rank)} of {right.peer_count} in {right.season}, "
            f"which reads as {percentile_band(right.percentile)}."
        )
    return summary.strip()


def build_provider_player_metric_snapshot(
    reference: PlayerSeasonReference,
    metric: ProviderMetricSpec,
    group_preference: str | None,
) -> ProviderPlayerMetricSnapshot | None:
    candidates: list[ProviderPlayerMetricSnapshot] = []
    if metric.batting_column and group_preference != "pitching":
        snapshot = provider_snapshot_for_group(reference, metric, "batting", metric.batting_column)
        if snapshot is not None:
            candidates.append(snapshot)
    if metric.pitching_column and group_preference != "batting":
        snapshot = provider_snapshot_for_group(reference, metric, "pitching", metric.pitching_column)
        if snapshot is not None:
            candidates.append(snapshot)
    if not candidates:
        return None
    if group_preference == "pitching":
        candidates.sort(key=lambda item: (0 if item.group == "pitching" else 1, item.group))
    elif group_preference == "batting":
        candidates.sort(key=lambda item: (0 if item.group == "batting" else 1, item.group))
    else:
        candidates.sort(key=lambda item: (0 if item.group == "batting" else 1, item.group))
    return candidates[0]


def provider_snapshot_for_group(
    reference: PlayerSeasonReference,
    metric: ProviderMetricSpec,
    group: str,
    column_name: str,
) -> ProviderPlayerMetricSnapshot | None:
    rows = load_batting_stats(reference.season, reference.season) if group == "batting" else load_pitching_stats(reference.season, reference.season)
    if not rows:
        return None
    qualification_threshold = None
    if metric.qualified_only:
        qualification_threshold = estimate_qualification_threshold(group, reference.season, {(group, reference.season, False): rows})
    normalized_target = normalize_person_name(reference.player_name)
    eligible_rows: list[dict[str, Any]] = []
    target_row: dict[str, Any] | None = None
    for row in rows:
        if column_name not in row or row[column_name] in (None, ""):
            continue
        if qualification_threshold is not None and not meets_qualification(row, group, qualification_threshold):
            continue
        name = str(row.get("Name") or "").strip()
        try:
            metric_value = float(row[column_name])
        except (TypeError, ValueError):
            continue
        candidate = {"name": name, "team": str(row.get("Team") or "").strip(), "metric_value": metric_value, "row": row}
        eligible_rows.append(candidate)
        if normalize_person_name(name) == normalized_target:
            target_row = candidate
    if target_row is None:
        return None
    eligible_rows.sort(key=lambda item: item["metric_value"], reverse=metric.higher_is_better)
    season_rank = next(
        (index for index, candidate in enumerate(eligible_rows, start=1) if normalize_person_name(candidate["name"]) == normalized_target),
        None,
    )
    peer_count = len(eligible_rows)
    percentile = round(100.0 * (peer_count - season_rank) / (peer_count - 1), 1) if season_rank is not None and peer_count > 1 else None
    row = target_row["row"]
    return ProviderPlayerMetricSnapshot(
        display_name=f"{reference.season} {reference.player_name}",
        player_name=reference.player_name,
        season=reference.season,
        team=target_row["team"],
        group=group,
        metric_label=metric.label,
        metric_value=float(target_row["metric_value"]),
        higher_is_better=metric.higher_is_better,
        season_rank=season_rank,
        peer_count=peer_count,
        percentile=percentile,
        context_1=provider_context_one(group, row),
        context_2=provider_context_two(group, row),
    )


def provider_context_one(group: str, row: dict[str, Any]) -> str:
    if group == "batting":
        return f"{normalize_rate_text(row.get('AVG'))}/{normalize_rate_text(row.get('OBP'))}/{normalize_rate_text(row.get('SLG'))}"
    return f"{normalize_rate_text(row.get('ERA'))} ERA"


def provider_context_two(group: str, row: dict[str, Any]) -> str:
    if group == "batting":
        return f"{int(float(row.get('PA') or 0))} PA" if row.get("PA") not in (None, "") else ""
    return f"{row.get('IP') or ''} IP".strip()


def compare_values(left_value: float | None, right_value: float | None, higher_is_better: bool) -> int:
    if left_value is None or right_value is None:
        return 0
    if abs(left_value - right_value) < 0.001:
        return 0
    if higher_is_better:
        return 1 if left_value > right_value else -1
    return 1 if left_value < right_value else -1


def core_snapshot_row(snapshot: PlayerSeasonSnapshot, metric: CoreMetricSpec, percentile: float | None) -> dict[str, Any]:
    return {
        "player": snapshot.player_name,
        "season": snapshot.season,
        "scope": snapshot.scope,
        "team": snapshot.team,
        "role": snapshot.role,
        "games": snapshot.games,
        "metric_value": format_comparison_value(snapshot_metric_value(snapshot, metric), digits=metric.digits, integer=metric.integer),
        "season_rank": "",
        "historical_percentile": format_comparison_value(percentile, digits=1),
        "pa": str(snapshot.plate_appearances),
        "avg": rate_text(snapshot.avg),
        "obp": rate_text(snapshot.obp),
        "slg": rate_text(snapshot.slg),
        "ops": rate_text(snapshot.ops),
        "hr": str(snapshot.home_runs),
        "rbi": str(snapshot.runs_batted_in),
        "sb": str(snapshot.stolen_bases),
        "bb": str(snapshot.walks),
        "so": str(snapshot.strikeouts),
        "ip": format_comparison_value(snapshot.innings, digits=1),
        "era": format_comparison_value(snapshot.era, digits=2),
        "whip": format_comparison_value(snapshot.whip, digits=2),
        "k_per_9": format_comparison_value(snapshot.strikeouts_per_9, digits=1),
        "bb_per_9": format_comparison_value(snapshot.walks_per_9, digits=1),
        "wins": str(snapshot.wins),
        "saves": str(snapshot.saves),
    }


def provider_snapshot_row(snapshot: ProviderPlayerMetricSnapshot) -> dict[str, Any]:
    return {
        "player": snapshot.player_name,
        "season": snapshot.season,
        "scope": "season leaderboard context",
        "team": snapshot.team,
        "role": snapshot.group,
        "games": "",
        "metric_value": format_comparison_value(snapshot.metric_value, digits=3),
        "season_rank": snapshot.season_rank,
        "historical_percentile": format_comparison_value(snapshot.percentile, digits=1),
        "pa": snapshot.context_2 if snapshot.group == "batting" else "",
        "avg": snapshot.context_1 if snapshot.group == "batting" else "",
        "obp": "",
        "slg": "",
        "ops": "",
        "hr": "",
        "rbi": "",
        "sb": "",
        "bb": "",
        "so": "",
        "ip": snapshot.context_2 if snapshot.group == "pitching" else "",
        "era": snapshot.context_1 if snapshot.group == "pitching" else "",
        "whip": "",
        "k_per_9": "",
        "bb_per_9": "",
        "wins": "",
        "saves": "",
    }


def rate_text(value: float | None) -> str:
    converted = safe_float(value)
    return f"{converted:.3f}" if converted is not None else "unknown"


def normalize_rate_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "unknown"
    return f"0{text}" if text.startswith(".") else text


def compute_obp(hits: int, at_bats: int, walks: int, hit_by_pitch: int, sacrifice_flies: int) -> float | None:
    denominator = at_bats + walks + hit_by_pitch + sacrifice_flies
    return ((hits + walks + hit_by_pitch) / denominator) if denominator > 0 else None


def compute_slg(hits: int, doubles: int, triples: int, home_runs: int, at_bats: int) -> float | None:
    if at_bats <= 0:
        return None
    singles = hits - doubles - triples - home_runs
    total_bases = singles + (2 * doubles) + (3 * triples) + (4 * home_runs)
    return total_bases / at_bats


def comparison_mode(left_season: int, right_season: int, current_season: int) -> str:
    if left_season == current_season and right_season == current_season:
        return "live"
    if left_season == current_season or right_season == current_season:
        return "hybrid"
    return "historical"
