from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class MetricDefinition:
    name: str
    aliases: list[str]
    category: str
    definition: str
    formula: str
    exact_formula_public: bool
    notes: str
    historical_support: str
    live_support: str
    citations: list[str]

    def matches(self, query: str) -> int:
        query_lower = query.lower()
        score = 0
        if term_in_query(query_lower, self.name):
            score += 10
        for alias in self.aliases:
            if term_in_query(query_lower, alias):
                score += 8
        for token in query_lower.split():
            if token == self.name.lower():
                score += 3
            if any(token == alias.lower() for alias in self.aliases):
                score += 2
        return score


class MetricCatalog:
    def __init__(self, metrics: list[MetricDefinition]) -> None:
        self.metrics = metrics

    @classmethod
    def load(cls, project_root: Path) -> "MetricCatalog":
        path = project_root / "mlb_history_bot" / "data" / "sabermetrics.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        metrics = [MetricDefinition(**item) for item in payload]
        return cls(metrics)

    def search(self, query: str, limit: int = 5) -> list[MetricDefinition]:
        ranked = [
            (metric.matches(query), metric)
            for metric in self.metrics
            if metric.matches(query) > 0
        ]
        ranked.sort(key=lambda item: (-item[0], item[1].name))
        return [metric for _, metric in ranked[:limit]]

    def find_exact(self, term: str) -> MetricDefinition | None:
        needle = term.strip().lower()
        for metric in self.metrics:
            if metric.name.lower() == needle:
                return metric
            if needle in {alias.lower() for alias in metric.aliases}:
                return metric
        return None


def term_in_query(query_lower: str, term: str) -> bool:
    needle = term.strip().lower()
    if not needle:
        return False
    compact = re.sub(r"[^a-z0-9]+", "", needle)
    if compact and len(compact) <= 4:
        pattern = rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])"
        return re.search(pattern, query_lower) is not None
    return needle in query_lower
