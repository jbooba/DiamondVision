from __future__ import annotations

from dataclasses import dataclass

from .award_history import AWARD_HINTS
from .config import Settings
from .metrics import MetricCatalog
from .models import EvidenceSnippet
EXCLUDED_CONTEXT_METRICS = {"WAR"}


@dataclass(slots=True)
class AwardOpponentGapQuery:
    metric_name: str
    award_label: str


class SpecialLeaderboardResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.catalog = MetricCatalog.load(settings.project_root)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        award_gap_query = parse_award_opponent_gap_query(question, self.catalog)
        if award_gap_query:
            return build_award_opponent_gap_snippet(award_gap_query)
        return None


def parse_award_opponent_gap_query(question: str, catalog: MetricCatalog) -> AwardOpponentGapQuery | None:
    lowered = question.lower()
    if "against" not in lowered:
        return None
    award_label = None
    for label, hints in AWARD_HINTS.items():
        if any(hint in lowered for hint in hints):
            award_label = label
            break
    if award_label is None:
        return None
    metric_name = detect_metric_name(question, catalog)
    return AwardOpponentGapQuery(metric_name=metric_name or "requested metric", award_label=award_label)


def detect_metric_name(question: str, catalog: MetricCatalog) -> str | None:
    for metric in catalog.search(question, limit=5):
        if metric.name in EXCLUDED_CONTEXT_METRICS:
            continue
        return metric.name
    return None


def build_award_opponent_gap_snippet(query: AwardOpponentGapQuery) -> EvidenceSnippet:
    return EvidenceSnippet(
        source="Contextual Split Planner",
        title=f"{query.metric_name} vs {query.award_label} source gap",
        citation="Historical split planner plus source-support rules",
        summary=(
            f"I understand this as an opponent-quality split leaderboard for {query.metric_name} against {query.award_label}. "
            "The local historical stack already has game-level batting and pitching rows, but it does not yet keep an "
            "award-winner lookup table joined to opposing pitcher ids, so it cannot ground that exact leaderboard yet. "
            "This should be solved with an imported awards history table or an official award-recipient sync, not by "
            "falling back to an unrelated metric."
        ),
        payload={
            "analysis_type": "contextual_source_gap",
            "metric": query.metric_name,
            "context": query.award_label,
        },
    )
