from __future__ import annotations

import json
import os
import re
import uuid
from collections import defaultdict

from .config import Settings
from .models import ChatResult
from .search import BaseballResearchEngine


SYSTEM_PROMPT = """You are a major-league-baseball-only research assistant.

Rules:
- Answer only MLB questions.
- Use the provided evidence and live data. Do not invent unsupported facts.
- If the exact answer is not available from the evidence, say so plainly and explain the source gap.
- When a metric is proprietary or not exactly computable from public inputs, say that clearly.
- Keep the answer concise but analytical.
- Use plain text only. Do not use Markdown emphasis, bold markers, italics, or bullet syntax that relies on Markdown rendering.
- If a payload says the leaderboard is complete and the rows are only a display slice, do not describe the evidence as partial or incomplete.
- End every answer with a 'Sources:' line listing the evidence labels you used.
"""

FALLBACK_REWRITE_PROMPT = """You rewrite MLB questions for a rules-based baseball search engine.

Return strict JSON with this shape:
{"queries":["normalized query 1","normalized query 2"]}

Rules:
- MLB only.
- Keep the user's intent the same.
- Prefer minimal rewrites: normalize player names, dates, metric aliases, and wording.
- Do not broaden the scope or invent facts.
- Use at most 3 candidate queries.
- Good patterns include:
  - "Pete Alonso xBA 2025"
  - "Cal Raleigh 2026 stats"
  - "did Yordan Alvarez hit any home runs this week"
  - "analyze the 1979 Cleveland Indians"
  - "compare the 2004 Expos to the 2026 Giants through the first 10 games of their seasons"
- If no safe rewrite is available, return {"queries":[]}.
"""

DEFINITION_HINTS = (
    "what is ",
    "what's ",
    "define ",
    "definition of ",
    "formula for ",
    "how is ",
    "how do you calculate ",
    "what does ",
    "meaning of ",
)
NON_DEFINITION_HINTS = (
    "highest",
    "lowest",
    "most",
    "least",
    "best",
    "worst",
    "leader",
    "leaders",
    "through",
    "across",
    "compare",
    "compared",
    "what team",
    "which team",
    "who ",
    "show me",
)
FOLLOW_UP_PATTERNS = (
    re.compile(r"^\s*(?:switch|change|swap|pivot)\s+(?:to\s+)?(?P<target>.+?)\s*[.?!]*\s*$", re.IGNORECASE),
    re.compile(r"^\s*(?:what|how)\s+about\s+(?P<target>.+?)\s*[.?!]*\s*$", re.IGNORECASE),
    re.compile(
        r"^\s*(?:same(?:\s+question|\s+thing)?|do the same)\s+(?:for|with|but for)\s+(?P<target>.+?)\s*[.?!]*\s*$",
        re.IGNORECASE,
    ),
    re.compile(r"^\s*(?:instead|rather)\s+(?:use\s+)?(?P<target>.+?)\s*[.?!]*\s*$", re.IGNORECASE),
)


class BaseballChatbot:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.engine = BaseballResearchEngine(settings)
        self.sessions: dict[str, list[dict[str, str]]] = defaultdict(list)

    def answer(self, question: str, *, session_id: str | None = None) -> ChatResult:
        active_session_id = session_id or str(uuid.uuid4())
        resolved_question = self._resolve_question(question, active_session_id)
        context = self.engine.compile_context(resolved_question)
        history = self.sessions[active_session_id][-8:]
        fallback_query, fallback_context = self._attempt_query_fallback(question, resolved_question, context)
        if fallback_query and fallback_context is not None:
            fallback_context.warnings.insert(
                0,
                f"Best-effort fallback interpreted this question as: {fallback_query}",
            )
            context = fallback_context
            resolved_question = fallback_query
        if not os.getenv("OPENAI_API_KEY"):
            answer = self._fallback_answer(question, context)
        else:
            answer = self._model_answer(question, resolved_question, context, history)
        answer = sanitize_answer_text(answer)
        citations = [snippet.title for snippet in context.all_snippets()]
        self.sessions[active_session_id].append(
            {"role": "user", "content": question, "resolved_question": resolved_question}
        )
        self.sessions[active_session_id].append({"role": "assistant", "content": answer})
        return ChatResult(answer=answer, citations=citations, warnings=context.warnings, context=context)

    def _model_answer(self, question: str, resolved_question: str, context, history: list[dict[str, str]]) -> str:
        from openai import OpenAI

        history_lines = []
        for item in history:
            if item["role"] == "user" and item.get("resolved_question") and item["resolved_question"] != item["content"]:
                history_lines.append(
                    f"user: {item['content']} [resolved as: {item['resolved_question']}]"
                )
            else:
                history_lines.append(f"{item['role']}: {item['content']}")
        history_text = "\n".join(history_lines)
        evidence_payload = {
            "classification": context.classification,
            "warnings": context.warnings,
            "glossary_entries": [self._snippet_payload(snippet) for snippet in context.glossary_entries],
            "historical_evidence": [self._snippet_payload(snippet) for snippet in context.historical_evidence],
            "replay_evidence": [self._snippet_payload(snippet) for snippet in context.replay_evidence],
            "live_evidence": [self._snippet_payload(snippet) for snippet in context.live_evidence],
        }
        prompt = (
            f"{SYSTEM_PROMPT}\n\n"
            f"Conversation history:\n{history_text or '(none)'}\n\n"
            f"User question:\n{question}\n\n"
            f"Resolved research question:\n{resolved_question}\n\n"
            f"Evidence bundle:\n{json.dumps(evidence_payload, indent=2)}"
        )
        client = OpenAI()
        response = client.responses.create(
            model=self.settings.openai_model,
            reasoning={"effort": self.settings.openai_reasoning_effort},
            input=prompt,
        )
        text = getattr(response, "output_text", "").strip()
        return text or self._fallback_answer(question, context)

    def _fallback_answer(self, question: str, context) -> str:
        lines = []
        if context.warnings:
            lines.extend(context.warnings)
        if context.replay_evidence:
            lines.append(context.replay_evidence[0].summary)
        if context.historical_evidence:
            lines.append(context.historical_evidence[0].summary)
        if context.live_evidence:
            lines.append(context.live_evidence[0].summary)
        if (looks_like_definition_question(question) or len(lines) == len(context.warnings)) and context.glossary_entries:
            lines.append(f"{context.glossary_entries[0].title}: {context.glossary_entries[0].summary}")
        citations = ", ".join(build_fallback_citation_titles(context)) or "No evidence loaded"
        lines.append(f"Sources: {citations}")
        return "\n".join(lines)

    def _resolve_question(self, question: str, session_id: str) -> str:
        previous_question = self._last_resolved_user_question(session_id)
        if not previous_question:
            return question
        rewritten = rewrite_follow_up_question(question, previous_question, self.engine.catalog)
        return rewritten or question

    def _attempt_query_fallback(self, question: str, resolved_question: str, context) -> tuple[str | None, object | None]:
        if not should_attempt_nlp_fallback(context):
            return None, None
        fallback_queries = self._rewrite_query_candidates(question, resolved_question)
        normalized_resolved = normalize_fallback_query(resolved_question)
        for candidate in fallback_queries:
            normalized_candidate = normalize_fallback_query(candidate)
            if not normalized_candidate or normalized_candidate == normalized_resolved:
                continue
            fallback_context = self.engine.compile_context(candidate)
            if has_grounded_evidence(fallback_context):
                return candidate, fallback_context
        return None, None

    def _rewrite_query_candidates(self, question: str, resolved_question: str) -> list[str]:
        if not os.getenv("OPENAI_API_KEY"):
            return []
        from openai import OpenAI

        prompt = (
            f"{FALLBACK_REWRITE_PROMPT}\n\n"
            f"Original user question:\n{question}\n\n"
            f"Current resolved question:\n{resolved_question}\n"
        )
        client = OpenAI()
        response = client.responses.create(
            model=self.settings.openai_model,
            reasoning={"effort": "low"},
            input=prompt,
        )
        return parse_fallback_query_response(getattr(response, "output_text", ""))

    def _last_resolved_user_question(self, session_id: str) -> str | None:
        for item in reversed(self.sessions[session_id]):
            if item.get("role") != "user":
                continue
            return item.get("resolved_question") or item.get("content")
        return None

    @staticmethod
    def _snippet_payload(snippet) -> dict:
        return {
            "source": snippet.source,
            "title": snippet.title,
            "citation": snippet.citation,
            "summary": snippet.summary,
            "payload": snippet.payload,
        }


def looks_like_definition_question(question: str) -> bool:
    lowered = question.strip().lower()
    if any(hint in lowered for hint in NON_DEFINITION_HINTS):
        return False
    return any(lowered.startswith(hint) for hint in DEFINITION_HINTS) or "definition" in lowered


def rewrite_follow_up_question(question: str, previous_question: str, catalog) -> str | None:
    target = extract_follow_up_target(question)
    if not target:
        return None
    previous_metric = detect_primary_metric(previous_question, catalog)
    if previous_metric is None:
        return f"{previous_question.rstrip(' ?.!')} using {target}"
    rewritten = replace_metric_phrase(previous_question, previous_metric, target)
    if rewritten == previous_question:
        return f"{previous_question.rstrip(' ?.!')} using {target}"
    return rewritten


def extract_follow_up_target(question: str) -> str | None:
    for pattern in FOLLOW_UP_PATTERNS:
        match = pattern.match(question)
        if match:
            return match.group("target").strip(" \"'")
    return None


def detect_primary_metric(question: str, catalog):
    matches = catalog.search(question, limit=3)
    return matches[0] if matches else None


def replace_metric_phrase(previous_question: str, metric, replacement: str) -> str:
    variants = sorted({metric.name, *metric.aliases}, key=len, reverse=True)
    rewritten = previous_question
    for variant in variants:
        pattern = re.compile(rf"(?<![a-z0-9]){re.escape(variant)}(?![a-z0-9])", re.IGNORECASE)
        rewritten, count = pattern.subn(replacement, rewritten, count=1)
        if count:
            return rewritten
    return previous_question


def should_attempt_nlp_fallback(context) -> bool:
    if has_grounded_evidence(context):
        return False
    if context.replay_evidence or context.live_evidence:
        return False
    if context.historical_evidence:
        return all(
            str(snippet.payload.get("analysis_type") or "") == "metric_source_gap"
            for snippet in context.historical_evidence
        )
    return True


def has_grounded_evidence(context) -> bool:
    for snippet in [*context.historical_evidence, *context.replay_evidence, *context.live_evidence]:
        if str(snippet.payload.get("analysis_type") or "") == "metric_source_gap":
            continue
        return True
    return False


def build_fallback_citation_titles(context) -> list[str]:
    non_glossary = [snippet.title for snippet in [*context.historical_evidence, *context.replay_evidence, *context.live_evidence]]
    if non_glossary:
        return non_glossary
    return [snippet.title for snippet in context.all_snippets()]


def normalize_fallback_query(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().casefold())


def parse_fallback_query_response(output_text: str) -> list[str]:
    payload = extract_first_json_object(output_text)
    if not payload:
        return []
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return []
    queries = parsed.get("queries")
    if not isinstance(queries, list):
        return []
    normalized: list[str] = []
    for item in queries:
        if not isinstance(item, str):
            continue
        cleaned = item.strip()
        if cleaned and cleaned not in normalized:
            normalized.append(cleaned)
    return normalized[:3]


def extract_first_json_object(value: str) -> str:
    stripped = value.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        return stripped
    match = re.search(r"\{.*\}", value, re.DOTALL)
    return match.group(0) if match else ""


def sanitize_answer_text(value: str) -> str:
    cleaned = value.replace("\r\n", "\n")
    cleaned = re.sub(r"\*\*(.*?)\*\*", r"\1", cleaned)
    cleaned = re.sub(r"__(.*?)__", r"\1", cleaned)
    cleaned = re.sub(r"(?<!\*)\*([^*\n]+)\*(?!\*)", r"\1", cleaned)
    cleaned = re.sub(r"(?m)^[ \t]*\*[ \t]+", "- ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()
