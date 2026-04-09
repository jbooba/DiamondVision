from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from .cohort_timeline import parse_cohort_filter
from .metrics import MetricCatalog
from .provider_metrics import find_provider_metric
from .query_intent import detect_ranking_intent, has_ranking_hint
from .query_utils import (
    extract_date_window,
    extract_minimum_qualifier,
    extract_name_candidates,
    extract_referenced_season,
    extract_season_span,
)
from .season_metric_leaderboards import find_season_metric, find_statcast_history_metric

WEEKDAY_NAMES = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)

CONDITION_HINTS: dict[str, tuple[str, ...]] = {
    "birthday": ("birthday",),
    "weekday": WEEKDAY_NAMES,
    "park": (" at ", " in oracle park", " in fenway", " in yankee stadium"),
    "pitch_type": (
        "fastball",
        "four-seam",
        "sinker",
        "cutter",
        "slider",
        "sweeper",
        "curveball",
        "changeup",
        "splitter",
        "slurve",
        "knuckleball",
        "forkball",
        "screwball",
    ),
    "pitch_location": ("middle middle", "up and in", "up-and-in", "high fastballs", "low and away"),
    "batted_ball_direction": ("left field", "center field", "right field", "pull side", "opposite field"),
}


@dataclass(slots=True)
class QueryFrame:
    kind: str
    entities: list[str] = field(default_factory=list)
    metric_label: str | None = None
    metric_source: str | None = None
    ranking: str | None = None
    season: int | None = None
    season_span_label: str | None = None
    date_window_label: str | None = None
    qualifiers: dict[str, int] = field(default_factory=dict)
    cohort_label: str | None = None
    conditions: list[str] = field(default_factory=list)
    layer_count: int = 0

    def summary(self) -> str:
        parts = [f"kind={self.kind}"]
        if self.entities:
            parts.append(f"entities={', '.join(self.entities)}")
        if self.metric_label:
            source_suffix = f" [{self.metric_source}]" if self.metric_source else ""
            parts.append(f"metric={self.metric_label}{source_suffix}")
        if self.ranking:
            parts.append(f"ranking={self.ranking}")
        if self.season_span_label:
            parts.append(f"season_span={self.season_span_label}")
        elif self.season is not None:
            parts.append(f"season={self.season}")
        if self.date_window_label:
            parts.append(f"window={self.date_window_label}")
        if self.cohort_label:
            parts.append(f"cohort={self.cohort_label}")
        if self.conditions:
            parts.append(f"conditions={', '.join(self.conditions)}")
        if self.qualifiers:
            qualifier_text = ", ".join(f"{key}>={value}" for key, value in self.qualifiers.items())
            parts.append(f"qualifiers={qualifier_text}")
        parts.append(f"layers={self.layer_count}")
        return "Query frame -> " + " | ".join(parts)


def build_query_frame(
    question: str,
    *,
    current_season: int,
    catalog: MetricCatalog,
    connection: Any | None = None,
) -> QueryFrame:
    lowered = question.lower().strip()
    entities = extract_name_candidates(question)
    season = extract_referenced_season(question, current_season)
    season_span = extract_season_span(question, current_season)
    date_window = extract_date_window(question, current_season)
    cohort = parse_cohort_filter(question)

    qualifiers: dict[str, int] = {}
    for label, terms in (
        ("pa", ("pa", "plate appearance", "plate appearances")),
        ("ab", ("ab", "at-bat", "at bats", "at-bats")),
        ("games", ("game", "games")),
        ("starts", ("start", "starts", "gs")),
        ("pitches", ("pitch", "pitches")),
    ):
        value = extract_minimum_qualifier(question, terms)
        if value is not None:
            qualifiers[label] = int(value)

    metric_label: str | None = None
    metric_source: str | None = None
    if connection is not None:
        history_metric = find_statcast_history_metric(connection, lowered)
        if history_metric is not None:
            metric_label = history_metric.label
            metric_source = history_metric.source_family
    if metric_label is None:
        season_metric = find_season_metric(lowered)
        if season_metric is not None:
            metric_label = season_metric.label
            metric_source = season_metric.source_family
    if metric_label is None:
        provider_metric = find_provider_metric(lowered, catalog)
        if provider_metric is not None:
            metric_label = provider_metric.metric_name
            metric_source = "provider"

    ranking_intent = None
    if has_ranking_hint(lowered):
        ranking_intent = detect_ranking_intent(lowered, higher_is_better=True, require_hint=False)
    ranking = ranking_intent.descriptor if ranking_intent is not None else None

    conditions: list[str] = []
    for label, hints in CONDITION_HINTS.items():
        if any(hint in lowered for hint in hints):
            conditions.append(label)

    kind = infer_query_kind(
        lowered,
        entities=entities,
        metric_label=metric_label,
        ranking=ranking,
        cohort_label=cohort.label if cohort is not None else None,
        conditions=conditions,
    )

    layer_count = sum(
        1
        for value in (
            bool(entities),
            bool(metric_label),
            bool(ranking),
            bool(season or season_span or date_window),
            bool(cohort),
            bool(conditions),
            bool(qualifiers),
        )
        if value
    )

    return QueryFrame(
        kind=kind,
        entities=entities,
        metric_label=metric_label,
        metric_source=metric_source,
        ranking=ranking,
        season=season,
        season_span_label=season_span.label if season_span is not None else None,
        date_window_label=date_window.label if date_window is not None else None,
        qualifiers=qualifiers,
        cohort_label=cohort.label if cohort is not None else None,
        conditions=conditions,
        layer_count=layer_count,
    )


def infer_query_kind(
    lowered: str,
    *,
    entities: list[str],
    metric_label: str | None,
    ranking: str | None,
    cohort_label: str | None,
    conditions: list[str],
) -> str:
    if any(token in lowered for token in ("clip", "clips", "video", "videos", "highlight", "replay", "watch")):
        return "clip_lookup"
    if entities and metric_label and not ranking and not cohort_label and not conditions:
        return "direct_player_metric" if len(entities) == 1 else "direct_entity_metric"
    if metric_label and ranking and (cohort_label or conditions):
        return "cohort_or_condition_leaderboard"
    if metric_label and ranking:
        return "metric_leaderboard"
    if entities and not metric_label and any(token in lowered for token in ("who is", "who was", "tell me about", "birthday", "birth date")):
        return "direct_profile_lookup"
    if cohort_label:
        return "cohort_lookup"
    if conditions:
        return "condition_lookup"
    return "general_lookup"
