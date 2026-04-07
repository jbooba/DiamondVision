from __future__ import annotations

from dataclasses import dataclass

from .config import Settings
from .metrics import MetricCatalog, MetricDefinition
from .models import EvidenceSnippet
from .provider_metrics import find_provider_metric
from .query_utils import extract_first_n_games


LEADERBOARD_HINTS = {"lowest", "highest", "most", "least", "best", "worst", "leader", "leaders"}
CURRENT_SCOPE_HINTS = {
    "current",
    "right now",
    "latest",
    "this season",
    "season so far",
    "today",
    "tonight",
    "at this point in the season",
}
SINGLE_GAME_HINTS = {"single game", "single-game", "in a game", "in one game", "tonight", "today"}
COMPARISON_HINTS = {
    "compare",
    "compared",
    "comparison",
    "previous",
    "past",
    "historical",
    "same point",
    "at this point in the season",
}
STATCAST_GAP_TERMS = {
    "xba": "xBA",
    "expected batting average": "xBA",
    "xwoba": "xwOBA",
    "expected woba": "xwOBA",
    "xslg": "xSLG",
    "expected slugging": "xSLG",
    "barrel rate": "Barrel Rate",
    "hard-hit rate": "Hard-Hit Rate",
    "hard hit rate": "Hard-Hit Rate",
}
DIRECT_PROVIDER_GAP_TERMS = {
    "war": "WAR",
    "wins above replacement": "WAR",
    "ops+": "OPS+",
    "era+": "ERA+",
    "wrc+": "wRC+",
    "woba": "wOBA",
    "rew24": "RE24",
    "re24": "RE24",
}
EXCLUDED_GAP_METRICS = {"DRS", "Def", "rPM", "rARM", "rSB", "rBU", "rGDP", "rHR"}


@dataclass(slots=True)
class MetricGapQuery:
    metric_name: str
    first_n_games: int | None
    wants_leaderboard: bool
    wants_team_scope: bool
    wants_current_scope: bool
    wants_comparison: bool
    wants_single_game_scope: bool


class MetricGapResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.catalog = MetricCatalog.load(settings.project_root)

    def build_snippet(self, question: str) -> EvidenceSnippet | None:
        query = parse_metric_gap_query(question, self.catalog)
        if query is None:
            return None
        metric = self.catalog.find_exact(query.metric_name) or fallback_metric_definition(query.metric_name)
        if metric is None:
            return None
        summary = build_metric_gap_summary(metric, query)
        return EvidenceSnippet(
            source="Metric Planner",
            title=f"{query.metric_name} source gap",
            citation="Curated sabermetric catalog plus current source-support rules",
            summary=summary,
            payload={
                "analysis_type": "metric_source_gap",
                "metric": query.metric_name,
                "first_n_games": query.first_n_games,
                "team_scope": query.wants_team_scope,
                "leaderboard": query.wants_leaderboard,
            },
        )


def parse_metric_gap_query(question: str, catalog: MetricCatalog) -> MetricGapQuery | None:
    lowered = question.lower()
    metric_name = detect_gap_metric_name(lowered, catalog)
    if metric_name is None:
        return None
    wants_leaderboard = any(hint in lowered for hint in LEADERBOARD_HINTS)
    first_n_games = extract_first_n_games(question)
    wants_team_scope = "team" in lowered or "roster" in lowered
    wants_current_scope = any(hint in lowered for hint in CURRENT_SCOPE_HINTS)
    wants_comparison = any(hint in lowered for hint in COMPARISON_HINTS)
    wants_single_game_scope = any(hint in lowered for hint in SINGLE_GAME_HINTS)
    if not wants_leaderboard and first_n_games is None and not wants_current_scope and not wants_comparison and not wants_single_game_scope:
        return None
    return MetricGapQuery(
        metric_name=metric_name,
        first_n_games=first_n_games,
        wants_leaderboard=wants_leaderboard,
        wants_team_scope=wants_team_scope,
        wants_current_scope=wants_current_scope,
        wants_comparison=wants_comparison,
        wants_single_game_scope=wants_single_game_scope,
    )


def detect_gap_metric_name(lowered_question: str, catalog: MetricCatalog) -> str | None:
    for term, metric_name in STATCAST_GAP_TERMS.items():
        if term in lowered_question:
            return metric_name
    for term, metric_name in DIRECT_PROVIDER_GAP_TERMS.items():
        if term in lowered_question:
            return metric_name
    provider_metric = find_provider_metric(lowered_question, catalog)
    if provider_metric is not None:
        return provider_metric.metric_name
    matches = catalog.search(lowered_question, limit=3)
    for metric in matches:
        if metric.name in EXCLUDED_GAP_METRICS:
            continue
        if metric.name in {"xBA", "xwOBA", "xSLG", "Barrel Rate", "Hard-Hit Rate", "WAR", "OPS+", "ERA+", "wRC+", "wOBA", "RE24"}:
            return metric.name
        if metric_requires_provider_import(metric):
            return metric.name
    return None


def build_metric_gap_summary(metric: MetricDefinition, query: MetricGapQuery) -> str:
    request_bits = []
    if query.wants_team_scope:
        request_bits.append("team")
    if query.wants_leaderboard:
        request_bits.append("leaderboard")
    if query.first_n_games is not None:
        request_bits.append(f"first-{query.first_n_games}-games window")
    if query.wants_single_game_scope:
        request_bits.append("single-game scope")
    if query.wants_current_scope:
        request_bits.append("current snapshot")
    if query.wants_comparison:
        request_bits.append("historical comparison")
    request_shape = ", ".join(request_bits) if request_bits else "query"
    source_gap = metric_source_gap(metric, query)
    return (
        f"I understand this as a {request_shape} request for {metric.name}. "
        f"{source_gap} Historical support note: {metric.historical_support} "
        f"Live support note: {metric.live_support} "
        f"If you want a grounded answer right now, I can usually pivot to batting average, OPS, runs per game, or another imported public metric over the same window."
    )


def metric_requires_provider_import(metric: MetricDefinition) -> bool:
    support_blob = " ".join((metric.notes, metric.historical_support, metric.live_support)).lower()
    provider_markers = (
        "must be imported",
        "provider",
        "not native to",
        "usually sourced from",
        "do not expose",
        "not expose",
        "relevant source",
    )
    return (not metric.exact_formula_public) and any(marker in support_blob for marker in provider_markers)


def metric_source_gap(metric: MetricDefinition, query: MetricGapQuery) -> str:
    if metric.name in {"xBA", "xwOBA", "xSLG", "Barrel Rate", "Hard-Hit Rate"}:
        return (
            f"The bot can define {metric.name}, but it cannot answer that exact question yet because the current local stack "
            f"does not include synced Statcast team-window data for that level of scope. "
            "Run `python -m mlb_history_bot sync-statcast --start-season 2025 --end-season 2026` "
            "or another Statcast date range first."
        )
    if metric.name == "WAR":
        if query.wants_single_game_scope:
            return (
                "WAR is fundamentally a season-scale provider metric, not a standard public single-game stat. "
                "The local stack should not reinterpret a single-game WAR question as a season leaderboard query."
            )
        return (
            "WAR is publicly available, but the current local stack does not yet sync provider WAR leaderboards or same-day "
            "snapshot history from FanGraphs or Baseball-Reference. MLB Stats API does not expose WAR directly, so the bot "
            "should not fake a live leader or a same-point historical comparison from scoreboard data."
        )
    if query.wants_current_scope or query.wants_comparison:
        return (
            f"{metric.name} is publicly available from external providers, but the current local stack does not yet keep "
            f"a synced leaderboard table or historical snapshot history for that metric."
        )
    return (
        f"The bot can define {metric.name}, but it cannot answer that exact question yet because the relevant provider-backed "
        f"dataset has not been synced into the local research tables."
    )


def fallback_metric_definition(metric_name: str) -> MetricDefinition | None:
    metric_name = metric_name.strip()
    if metric_name == "xBA":
        return MetricDefinition(
            name="xBA",
            aliases=["expected batting average"],
            category="batting",
            definition="xBA estimates the batting average a hitter or contact profile deserved based on Statcast inputs.",
            formula="Provider model based on Statcast quality-of-contact inputs.",
            exact_formula_public=False,
            notes="Exact implementation is provider-specific and tied to Statcast-era data.",
            historical_support="Requires Statcast batted-ball data and provider-level expected-outcome modeling.",
            live_support="Usually sourced from Baseball Savant or Statcast-facing tools rather than the MLB Stats API.",
            citations=["MLB Statcast glossary"],
        )
    return None
