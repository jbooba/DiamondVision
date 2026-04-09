from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class EvidenceSnippet:
    source: str
    title: str
    citation: str
    summary: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CompiledContext:
    classification: str
    question: str
    glossary_entries: list[EvidenceSnippet] = field(default_factory=list)
    historical_evidence: list[EvidenceSnippet] = field(default_factory=list)
    replay_evidence: list[EvidenceSnippet] = field(default_factory=list)
    live_evidence: list[EvidenceSnippet] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    trace: list[str] = field(default_factory=list)

    def all_snippets(self) -> list[EvidenceSnippet]:
        return [*self.historical_evidence, *self.replay_evidence, *self.live_evidence, *self.glossary_entries]


@dataclass(slots=True)
class ChatResult:
    answer: str
    citations: list[str]
    warnings: list[str]
    context: CompiledContext
