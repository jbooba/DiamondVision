from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .metrics import MetricCatalog
from .models import EvidenceSnippet
from .person_query import choose_best_person_match, clean_player_phrase as shared_clean_player_phrase, extract_player_candidate
from .player_season_comparison import (
    metric_terms_for_query,
    resolve_player_reference,
    split_player_comparison_question,
)
from .provider_metrics import find_provider_metric
from .query_utils import extract_explicit_year, extract_first_n_games, question_requests_current_scope
from .team_evaluator import format_float, safe_float, safe_int
from .team_season_compare import compute_ops, game_log_sort_key


START_COMPARISON_HINTS = {
    "previous season starts",
    "prior season starts",
    "previous starts",
    "prior starts",
    "previous season start",
    "prior season start",
}
COMPARISON_HINTS = {"compare", "compared", "comparison", "how does", "how did", "with his", "with her", "with their"}
FIRST_N_STARTS_PATTERN = re.compile(
    r"\b(?:through|thru|over|in|across)\s+(?:the\s+)?first\s+([a-z0-9-]+(?:\s+[a-z0-9-]+){0,3})\s+starts?\b"
    r"|\bfirst\s+([a-z0-9-]+(?:\s+[a-z0-9-]+){0,3})\s+starts?\b",
    re.IGNORECASE,
)

@dataclass(frozen=True, slots=True)
class StartMetricSpec:
    key: str
    label: str
    group: str
    higher_is_better: bool
    aliases: tuple[str, ...]


SUPPORTED_START_METRICS: tuple[StartMetricSpec, ...] = (
    StartMetricSpec("ops", "OPS", "hitting", True, ("ops",)),
    StartMetricSpec("avg", "AVG", "hitting", True, ("avg", "ba", "batting average")),
    StartMetricSpec("obp", "OBP", "hitting", True, ("obp", "on-base percentage")),
    StartMetricSpec("slg", "SLG", "hitting", True, ("slg", "slugging percentage")),
    StartMetricSpec("home_runs", "HR", "hitting", True, ("home runs", "homeruns", "homers", "hr")),
    StartMetricSpec("hits", "Hits", "hitting", True, ("hits", "hit")),
    StartMetricSpec("rbi", "RBI", "hitting", True, ("rbi", "rbis", "runs batted in")),
    StartMetricSpec("walks", "BB", "hitting", True, ("walks", "walk", "bb")),
    StartMetricSpec("strikeouts", "SO", "hitting", False, ("strikeouts", "strikeout", "so", "k")),
    StartMetricSpec("plate_appearances", "PA", "hitting", True, ("plate appearances", "plate appearance", "pa")),
    StartMetricSpec("total_bases", "TB", "hitting", True, ("total bases", "tb")),
    StartMetricSpec("era", "ERA", "pitching", False, ("era", "earned run average")),
    StartMetricSpec("whip", "WHIP", "pitching", False, ("whip",)),
    StartMetricSpec("innings", "IP", "pitching", True, ("innings", "ip")),
    StartMetricSpec("wins", "Wins", "pitching", True, ("wins", "win")),
    StartMetricSpec("strikeouts_p", "SO", "pitching", True, ("pitcher strikeouts", "pitching strikeouts")),
    StartMetricSpec("strikeouts_per_9", "K/9", "pitching", True, ("k/9", "strikeouts per nine")),
    StartMetricSpec("walks_per_9", "BB/9", "pitching", False, ("bb/9", "walks per nine")),
    StartMetricSpec("home_runs_per_9", "HR/9", "pitching", False, ("hr/9", "home runs per nine")),
)


@dataclass(slots=True)
class PlayerStartComparisonQuery:
    player_name: str
    player_id: int
    season: int
    metric: StartMetricSpec
    mode: str
    left_player_name: str | None = None
    left_player_id: int | None = None
    left_season: int | None = None
    right_player_name: str | None = None
    right_player_id: int | None = None
    right_season: int | None = None
    explicit_compare: bool = False
    first_n: int | None = None
    window_kind: str = "games"


@dataclass(slots=True)
class PlayerStartSnapshot:
    season: int
    team: str
    games: int
    metric_value: float | None
    at_bats: int
    hits: int
    walks: int
    strikeouts: int
    rbi: int
    home_runs: int
    plate_appearances: int
    avg: float | None
    obp: float | None
    slg: float | None
    ops: float | None
    innings: float | None = None
    era: float | None = None
    whip: float | None = None
    strikeouts_per_9: float | None = None
    walks_per_9: float | None = None
    home_runs_per_9: float | None = None
    role: str = "hitting"


class PlayerStartComparisonResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)
        self.catalog = MetricCatalog.load(settings.project_root)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        current_season = self.settings.live_season or date.today().year
        query = parse_player_start_comparison_query(connection, question, self.live_client, self.catalog, current_season)
        if query is None:
            return None

        if query.explicit_compare and query.first_n and query.left_player_id and query.right_player_id and query.left_season and query.right_season:
            left_logs = load_window_logs(self.live_client, query.left_player_id, query.left_season, query.window_kind, query.metric.group)
            right_logs = load_window_logs(self.live_client, query.right_player_id, query.right_season, query.window_kind, query.metric.group)
            if len(left_logs) < query.first_n or len(right_logs) < query.first_n:
                return None
            left_snapshot = aggregate_window_snapshot(left_logs[: query.first_n], query.metric)
            right_snapshot = aggregate_window_snapshot(right_logs[: query.first_n], query.metric)
            summary = build_explicit_window_comparison_summary(query, left_snapshot, right_snapshot)
            rows = [
                snapshot_row(left_snapshot, query.metric, current_season),
                snapshot_row(right_snapshot, query.metric, current_season),
            ]
            return EvidenceSnippet(
                source="Player Start Comparison",
                title=f"{query.left_player_name} vs {query.right_player_name} first-{query.first_n}-{query.window_kind}",
                citation="MLB Stats API player game logs aggregated over explicit first-N game/start windows",
                summary=summary,
                payload={
                    "analysis_type": "player_start_comparison",
                    "mode": query.mode,
                    "player": query.left_player_name,
                    "metric": query.metric.label,
                    "season": query.left_season,
                    "games_compared": query.first_n,
                    "window_kind": query.window_kind,
                    "rows": rows,
                },
            )

        current_logs = load_regular_season_logs(self.live_client, query.player_id, query.season)
        if not current_logs:
            return None
        games = len(current_logs)
        current_snapshot = aggregate_start_snapshot(current_logs[:games], query.metric)

        comparison_snapshots: list[PlayerStartSnapshot] = [current_snapshot]
        recent_seasons_without_games = 0
        for season in range(query.season - 1, max(1871, query.season - 12), -1):
            logs = load_regular_season_logs(self.live_client, query.player_id, season)
            if not logs:
                recent_seasons_without_games += 1
                if recent_seasons_without_games >= 3 and comparison_snapshots:
                    break
                continue
            recent_seasons_without_games = 0
            if len(logs) < games:
                continue
            comparison_snapshots.append(aggregate_start_snapshot(logs[:games], query.metric))

        if len(comparison_snapshots) < 2:
            return None

        sorted_rows = sorted(
            comparison_snapshots,
            key=lambda snapshot: (
                -(snapshot.metric_value or -9999.0) if query.metric.higher_is_better else (snapshot.metric_value or 9999.0),
                -snapshot.season,
            ),
        )
        current_rank = next(
            (index for index, snapshot in enumerate(sorted_rows, start=1) if snapshot.season == query.season),
            1,
        )
        previous_snapshots = [snapshot for snapshot in comparison_snapshots if snapshot.season != query.season]
        most_recent_previous = max(previous_snapshots, key=lambda snapshot: snapshot.season)
        best_previous = sorted_rows[0] if sorted_rows[0].season != query.season else (sorted_rows[1] if len(sorted_rows) > 1 else most_recent_previous)

        summary = (
            f"Through {games} game(s) in {query.season}, {query.player_name} is at "
            f"{format_metric(query.metric, current_snapshot.metric_value)} {query.metric.label}. "
            f"Against his previous {games}-game season starts, that ranks {ordinal(current_rank)} out of "
            f"{len(comparison_snapshots)} tracked seasons. "
            f"His most recent prior start was {format_metric(query.metric, most_recent_previous.metric_value)} "
            f"in {most_recent_previous.season}; the best previous mark was "
            f"{format_metric(query.metric, best_previous.metric_value)} in {best_previous.season}."
        )
        rows = [snapshot_row(snapshot, query.metric, query.season) for snapshot in sorted(comparison_snapshots, key=lambda item: item.season, reverse=True)]
        return EvidenceSnippet(
            source="Player Start Comparison",
            title=f"{query.player_name} {query.metric.label} season-start comparison",
            citation="MLB Stats API player game logs aggregated through the current game count of the selected season",
            summary=summary,
            payload={
                "analysis_type": "player_start_comparison",
                "mode": query.mode,
                "player": query.player_name,
                "metric": query.metric.label,
                "season": query.season,
                "games_compared": games,
                "rows": rows,
            },
        )


def parse_player_start_comparison_query(
    connection,
    question: str,
    live_client: LiveStatsClient,
    catalog: MetricCatalog,
    current_season: int,
) -> PlayerStartComparisonQuery | None:
    lowered = question.lower()
    first_n_games = extract_first_n_games(question)
    first_n_starts = extract_first_n_starts(question)
    split = split_player_comparison_question(question)
    if split is not None and (first_n_games or first_n_starts):
        left_phrase, right_phrase, _ = split
        provider_metric = find_provider_metric(lowered, catalog)
        metric = detect_start_metric(lowered, catalog) or default_metric_for_window("starts" if first_n_starts else "games")
        metric_terms = metric_terms_for_query(provider_metric, None)
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
        left_player_id = resolve_live_player_id(live_client, left_reference.player_name)
        right_player_id = resolve_live_player_id(live_client, right_reference.player_name)
        if not left_player_id or not right_player_id:
            return None
        return PlayerStartComparisonQuery(
            player_name=left_reference.player_name,
            player_id=left_player_id,
            season=left_reference.season,
            metric=metric,
            mode=explicit_window_mode(left_reference.season, right_reference.season, current_season),
            left_player_name=left_reference.player_name,
            left_player_id=left_player_id,
            left_season=left_reference.season,
            right_player_name=right_reference.player_name,
            right_player_id=right_player_id,
            right_season=right_reference.season,
            explicit_compare=True,
            first_n=first_n_starts or first_n_games,
            window_kind="starts" if first_n_starts else "games",
        )

    if not any(hint in lowered for hint in START_COMPARISON_HINTS):
        return None
    if not any(hint in lowered for hint in COMPARISON_HINTS):
        return None
    if not question_requests_current_scope(question) and extract_explicit_year(question) is None:
        return None
    metric = detect_start_metric(lowered, catalog)
    if metric is None:
        return None
    player_query = extract_player_query(question, metric)
    if not player_query:
        return None
    people = live_client.search_people(player_query)
    if not people:
        return None
    person = choose_best_person_match(people, player_query)
    player_id = int(person.get("id") or 0)
    if not player_id:
        return None
    season = extract_explicit_year(question) or current_season
    return PlayerStartComparisonQuery(
        player_name=str(person.get("fullName") or player_query).strip(),
        player_id=player_id,
        season=season,
        metric=metric,
        mode="live" if season == current_season else "historical",
    )


def detect_start_metric(lowered_question: str, catalog: MetricCatalog) -> StartMetricSpec | None:
    provider_metric = find_provider_metric(lowered_question, catalog)
    if provider_metric is not None:
        for metric in SUPPORTED_START_METRICS:
            if metric.label == provider_metric.label or metric.label == provider_metric.metric_name:
                return metric
    for metric in SUPPORTED_START_METRICS:
        if any(term_in_question(lowered_question, alias) for alias in metric.aliases):
            return metric
    return None


def extract_player_query(question: str, metric: StartMetricSpec) -> str | None:
    metric_pattern = "|".join(re.escape(alias) for alias in sorted(metric.aliases, key=len, reverse=True))
    patterns = (
        re.compile(rf"(?:compare|how\s+does|how\s+did)\s+(.+?)(?:'s)?\s+(?:{metric_pattern})\b", re.IGNORECASE),
        re.compile(rf"^(.+?)(?:'s)?\s+(?:{metric_pattern})\b", re.IGNORECASE),
    )
    return extract_player_candidate(question, patterns=patterns)


def clean_player_fragment(value: str) -> str:
    return shared_clean_player_phrase(value)


def load_regular_season_logs(live_client: LiveStatsClient, player_id: int, season: int) -> list[dict[str, Any]]:
    rows = live_client.player_game_logs(player_id, season=season, group="hitting")
    filtered = [row for row in rows if str(row.get("gameType") or "").upper() == "R"]
    return sorted(filtered, key=game_log_sort_key)


def load_window_logs(
    live_client: LiveStatsClient,
    player_id: int,
    season: int,
    window_kind: str,
    group: str,
) -> list[dict[str, Any]]:
    rows = live_client.player_game_logs(player_id, season=season, group=group)
    filtered = [row for row in rows if str(row.get("gameType") or "").upper() == "R"]
    sorted_rows = sorted(filtered, key=game_log_sort_key)
    if window_kind == "starts":
        return [row for row in sorted_rows if (safe_int(row.get("stat", {}).get("gamesStarted")) or 0) > 0]
    return sorted_rows


def aggregate_start_snapshot(rows: list[dict[str, Any]], metric: StartMetricSpec) -> PlayerStartSnapshot:
    at_bats = sum(safe_int(row.get("stat", {}).get("atBats")) or 0 for row in rows)
    hits = sum(safe_int(row.get("stat", {}).get("hits")) or 0 for row in rows)
    doubles = sum(safe_int(row.get("stat", {}).get("doubles")) or 0 for row in rows)
    triples = sum(safe_int(row.get("stat", {}).get("triples")) or 0 for row in rows)
    home_runs = sum(safe_int(row.get("stat", {}).get("homeRuns")) or 0 for row in rows)
    walks = sum(safe_int(row.get("stat", {}).get("baseOnBalls")) or 0 for row in rows)
    hit_by_pitch = sum(safe_int(row.get("stat", {}).get("hitByPitch")) or 0 for row in rows)
    sacrifice_flies = sum(safe_int(row.get("stat", {}).get("sacFlies")) or 0 for row in rows)
    strikeouts = sum(safe_int(row.get("stat", {}).get("strikeOuts")) or 0 for row in rows)
    rbi = sum(safe_int(row.get("stat", {}).get("rbi")) or 0 for row in rows)
    plate_appearances = sum(safe_int(row.get("stat", {}).get("plateAppearances")) or 0 for row in rows)
    total_bases = sum(safe_int(row.get("stat", {}).get("totalBases")) or 0 for row in rows)
    avg = (hits / at_bats) if at_bats else None
    obp_denom = at_bats + walks + hit_by_pitch + sacrifice_flies
    obp = ((hits + walks + hit_by_pitch) / obp_denom) if obp_denom else None
    slg = (total_bases / at_bats) if at_bats else None
    ops = compute_ops(hits, doubles, triples, home_runs, at_bats, walks, hit_by_pitch, sacrifice_flies)
    team = str(rows[0].get("team", {}).get("name") or "").strip() if rows else ""
    metric_value = {
        "ops": ops,
        "avg": avg,
        "obp": obp,
        "slg": slg,
        "home_runs": float(home_runs),
        "hits": float(hits),
        "rbi": float(rbi),
        "walks": float(walks),
        "strikeouts": float(strikeouts),
        "plate_appearances": float(plate_appearances),
        "total_bases": float(total_bases),
    }.get(metric.key)
    return PlayerStartSnapshot(
        season=int(rows[0].get("season") or date.today().year) if rows else date.today().year,
        team=team,
        games=len(rows),
        metric_value=metric_value,
        at_bats=at_bats,
        hits=hits,
        walks=walks,
        strikeouts=strikeouts,
        rbi=rbi,
        home_runs=home_runs,
        plate_appearances=plate_appearances,
        avg=avg,
        obp=obp,
        slg=slg,
        ops=ops,
        role="hitting",
    )


def aggregate_pitching_window_snapshot(rows: list[dict[str, Any]], metric: StartMetricSpec) -> PlayerStartSnapshot:
    innings = sum(parse_innings_pitched(row.get("stat", {}).get("inningsPitched")) for row in rows)
    hits_allowed = sum(safe_int(row.get("stat", {}).get("hits")) or 0 for row in rows)
    walks = sum(safe_int(row.get("stat", {}).get("baseOnBalls")) or 0 for row in rows)
    strikeouts = sum(safe_int(row.get("stat", {}).get("strikeOuts")) or 0 for row in rows)
    home_runs = sum(safe_int(row.get("stat", {}).get("homeRuns")) or 0 for row in rows)
    earned_runs = sum(safe_int(row.get("stat", {}).get("earnedRuns")) or 0 for row in rows)
    wins = sum(safe_int(row.get("stat", {}).get("wins")) or 0 for row in rows)
    outs = int(round(innings * 3))
    era = ((27.0 * earned_runs) / outs) if outs else None
    whip = ((walks + hits_allowed) / innings) if innings else None
    k_per_9 = ((27.0 * strikeouts) / outs) if outs else None
    bb_per_9 = ((27.0 * walks) / outs) if outs else None
    hr_per_9 = ((27.0 * home_runs) / outs) if outs else None
    metric_value = {
        "era": era,
        "whip": whip,
        "innings": innings,
        "wins": float(wins),
        "strikeouts_p": float(strikeouts),
        "strikeouts_per_9": k_per_9,
        "walks_per_9": bb_per_9,
        "home_runs_per_9": hr_per_9,
    }.get(metric.key)
    team = str(rows[0].get("team", {}).get("name") or "").strip() if rows else ""
    return PlayerStartSnapshot(
        season=int(rows[0].get("season") or date.today().year) if rows else date.today().year,
        team=team,
        games=len(rows),
        metric_value=metric_value,
        at_bats=0,
        hits=0,
        walks=walks,
        strikeouts=strikeouts,
        rbi=0,
        home_runs=home_runs,
        plate_appearances=0,
        avg=None,
        obp=None,
        slg=None,
        ops=None,
        innings=innings,
        era=era,
        whip=whip,
        strikeouts_per_9=k_per_9,
        walks_per_9=bb_per_9,
        home_runs_per_9=hr_per_9,
        role="pitching",
    )


def aggregate_window_snapshot(rows: list[dict[str, Any]], metric: StartMetricSpec) -> PlayerStartSnapshot:
    if metric.group == "pitching":
        return aggregate_pitching_window_snapshot(rows, metric)
    return aggregate_start_snapshot(rows, metric)


def snapshot_row(snapshot: PlayerStartSnapshot, metric: StartMetricSpec, current_season: int) -> dict[str, Any]:
    return {
        "season": snapshot.season,
        "scope": "current season to date" if snapshot.season == current_season else f"first {snapshot.games} games",
        "team": snapshot.team,
        "games": snapshot.games,
        "metric_value": format_metric(metric, snapshot.metric_value),
        "avg": format_rate(snapshot.avg),
        "obp": format_rate(snapshot.obp),
        "slg": format_rate(snapshot.slg),
        "ops": format_rate(snapshot.ops),
        "hr": str(snapshot.home_runs),
        "rbi": str(snapshot.rbi),
        "bb": str(snapshot.walks),
        "so": str(snapshot.strikeouts),
        "pa": str(snapshot.plate_appearances),
        "ip": format_float(snapshot.innings, 1) if snapshot.innings is not None else "",
        "era": format_float(snapshot.era, 2) if snapshot.era is not None else "",
        "whip": format_float(snapshot.whip, 2) if snapshot.whip is not None else "",
        "k_per_9": format_float(snapshot.strikeouts_per_9, 1) if snapshot.strikeouts_per_9 is not None else "",
    }


def format_metric(metric: StartMetricSpec, value: float | None) -> str:
    if value is None:
        return "unknown"
    if metric.key in {"ops", "avg", "obp", "slg"}:
        return format_rate(value)
    if metric.key in {"era", "whip"}:
        return format_float(value, 2)
    if metric.key in {"strikeouts_per_9", "walks_per_9", "home_runs_per_9", "innings"}:
        return format_float(value, 1)
    return str(int(round(value)))


def format_rate(value: float | None) -> str:
    return format_float(value, 3)


def term_in_question(question_lower: str, term: str) -> bool:
    needle = term.strip().lower()
    if not needle:
        return False
    pattern = rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])"
    return re.search(pattern, question_lower) is not None


def ordinal(value: int) -> str:
    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def extract_first_n_starts(question: str) -> int | None:
    match = FIRST_N_STARTS_PATTERN.search(question)
    if match is None:
        return None
    value = match.group(1) or match.group(2)
    return parse_number_token(value) if value else None


def parse_number_token(value: str | None) -> int | None:
    if not value:
        return None
    token = value.strip().lower().replace("-", " ")
    if token.isdigit():
        return int(token)
    number_words = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
        "eleven": 11,
        "twelve": 12,
        "thirteen": 13,
        "fourteen": 14,
        "fifteen": 15,
        "sixteen": 16,
        "seventeen": 17,
        "eighteen": 18,
        "nineteen": 19,
        "twenty": 20,
    }
    parts = [part for part in token.split() if part]
    if len(parts) == 1:
        return number_words.get(parts[0])
    total = 0
    for part in parts:
        value_part = number_words.get(part)
        if value_part is None:
            return None
        total += value_part
    return total or None


def resolve_live_player_id(live_client: LiveStatsClient, player_name: str) -> int | None:
    people = live_client.search_people(player_name)
    if not people:
        return None
    person = choose_best_person_match(people, player_name)
    person_id = safe_int(person.get("id"))
    return person_id or None


def default_metric_for_window(window_kind: str) -> StartMetricSpec:
    target_key = "era" if window_kind == "starts" else "ops"
    return next(metric for metric in SUPPORTED_START_METRICS if metric.key == target_key)


def explicit_window_mode(left_season: int, right_season: int, current_season: int) -> str:
    if left_season == current_season and right_season == current_season:
        return "live"
    if left_season == current_season or right_season == current_season:
        return "hybrid"
    return "historical"


def parse_innings_pitched(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    if "." not in text:
        return safe_float(text) or 0.0
    whole, _, frac = text.partition(".")
    whole_value = safe_int(whole) or 0
    frac_value = safe_int(frac) or 0
    return whole_value + (frac_value / 3.0)


def build_explicit_window_comparison_summary(
    query: PlayerStartComparisonQuery,
    left: PlayerStartSnapshot,
    right: PlayerStartSnapshot,
) -> str:
    if query.metric.group == "pitching":
        return (
            f"Through the first {query.first_n} {query.window_kind}, {query.left_player_name} in {query.left_season} "
            f"posted {format_metric(query.metric, left.metric_value)} {query.metric.label}, "
            f"{format_float(left.era, 2)} ERA, {format_float(left.whip, 2)} WHIP, and "
            f"{format_float(left.innings, 1)} IP. "
            f"In {query.right_season}, {query.right_player_name} was at {format_metric(query.metric, right.metric_value)} "
            f"{query.metric.label}, {format_float(right.era, 2)} ERA, {format_float(right.whip, 2)} WHIP, and "
            f"{format_float(right.innings, 1)} IP over the same first-{query.first_n}-{query.window_kind} window."
        )
    return (
        f"Through the first {query.first_n} {query.window_kind}, {query.left_player_name} in {query.left_season} "
        f"hit {format_rate(left.avg)}/{format_rate(left.obp)}/{format_rate(left.slg)} with {left.home_runs} HR, "
        f"{left.rbi} RBI, {left.plate_appearances} PA, and a {format_rate(left.ops)} OPS. "
        f"In {query.right_season}, {query.right_player_name} hit {format_rate(right.avg)}/{format_rate(right.obp)}/"
        f"{format_rate(right.slg)} with {right.home_runs} HR, {right.rbi} RBI, {right.plate_appearances} PA, "
        f"and a {format_rate(right.ops)} OPS over the same first-{query.first_n}-{query.window_kind} window."
    )
