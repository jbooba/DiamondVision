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
from .provider_metrics import find_provider_metric
from .query_utils import extract_explicit_year, question_requests_current_scope
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

@dataclass(frozen=True, slots=True)
class StartMetricSpec:
    key: str
    label: str
    higher_is_better: bool
    aliases: tuple[str, ...]


SUPPORTED_START_METRICS: tuple[StartMetricSpec, ...] = (
    StartMetricSpec("ops", "OPS", True, ("ops",)),
    StartMetricSpec("avg", "AVG", True, ("avg", "ba", "batting average")),
    StartMetricSpec("obp", "OBP", True, ("obp", "on-base percentage")),
    StartMetricSpec("slg", "SLG", True, ("slg", "slugging percentage")),
    StartMetricSpec("home_runs", "HR", True, ("home runs", "homeruns", "homers", "hr")),
    StartMetricSpec("hits", "Hits", True, ("hits", "hit")),
    StartMetricSpec("rbi", "RBI", True, ("rbi", "rbis", "runs batted in")),
    StartMetricSpec("walks", "BB", True, ("walks", "walk", "bb")),
    StartMetricSpec("strikeouts", "SO", False, ("strikeouts", "strikeout", "so", "k")),
    StartMetricSpec("plate_appearances", "PA", True, ("plate appearances", "plate appearance", "pa")),
    StartMetricSpec("total_bases", "TB", True, ("total bases", "tb")),
)


@dataclass(slots=True)
class PlayerStartComparisonQuery:
    player_name: str
    player_id: int
    season: int
    metric: StartMetricSpec
    mode: str


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


class PlayerStartComparisonResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)
        self.catalog = MetricCatalog.load(settings.project_root)

    def build_snippet(self, question: str) -> EvidenceSnippet | None:
        current_season = self.settings.live_season or date.today().year
        query = parse_player_start_comparison_query(question, self.live_client, self.catalog, current_season)
        if query is None:
            return None

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
    question: str,
    live_client: LiveStatsClient,
    catalog: MetricCatalog,
    current_season: int,
) -> PlayerStartComparisonQuery | None:
    lowered = question.lower()
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
    )


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
    }


def format_metric(metric: StartMetricSpec, value: float | None) -> str:
    if value is None:
        return "unknown"
    if metric.key in {"ops", "avg", "obp", "slg"}:
        return format_rate(value)
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
