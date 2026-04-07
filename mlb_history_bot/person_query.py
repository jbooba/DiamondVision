from __future__ import annotations

import re
from typing import Any, Iterable

from .query_utils import extract_name_candidates, normalize_person_name


COMMON_PLAYER_STOPWORDS = {
    "what",
    "is",
    "show",
    "me",
    "current",
    "latest",
    "this",
    "season",
    "year",
    "so",
    "far",
    "in",
    "compare",
    "how",
    "does",
    "did",
    "with",
    "his",
    "her",
    "their",
    "previous",
    "prior",
    "starts",
    "start",
    "through",
    "the",
    "to",
    "date",
    "right",
    "now",
    "today",
    "tonight",
    "yesterday",
    "last",
    "night",
}


def choose_best_person_match(people: list[dict[str, Any]], requested_name: str | None = None) -> dict[str, Any]:
    normalized_requested = normalize_person_name(requested_name or "")

    def sort_key(person: dict[str, Any]) -> tuple[int, int, int, int, str, str]:
        full_name = normalize_person_name(str(person.get("fullName") or ""))
        return (
            0 if normalized_requested and full_name == normalized_requested else 1,
            0 if person.get("active") else 1,
            0 if person.get("isPlayer") else 1,
            0 if person.get("isVerified") else 1,
            str(person.get("lastPlayedDate") or ""),
            str(person.get("fullName") or ""),
        )

    return sorted(people, key=sort_key)[0]


def clean_player_phrase(value: str, *, extra_stopwords: Iterable[str] = ()) -> str:
    cleaned = value.strip(" ?.!,'\"")
    cleaned = re.sub(r"'s\b", "", cleaned, flags=re.IGNORECASE)
    stopwords = COMMON_PLAYER_STOPWORDS | {word.casefold() for word in extra_stopwords}
    if stopwords:
        pattern = r"\b(?:" + "|".join(re.escape(word) for word in sorted(stopwords, key=len, reverse=True)) + r")\b"
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(18\d{2}|19\d{2}|20\d{2})\b", " ", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    if not cleaned or " " not in cleaned:
        return ""
    return " ".join(part.capitalize() for part in cleaned.split())


def extract_player_candidate(
    question: str,
    *,
    patterns: Iterable[re.Pattern[str]] = (),
    extra_stopwords: Iterable[str] = (),
    allow_fallback: bool = False,
) -> str | None:
    candidates = extract_name_candidates(question)
    if candidates:
        return candidates[0]
    stripped = question.strip(" ?.!")
    for pattern in patterns:
        match = pattern.search(stripped)
        if not match:
            continue
        candidate = clean_player_phrase(match.group(1), extra_stopwords=extra_stopwords)
        if candidate:
            return candidate
    if not allow_fallback:
        return None
    fallback = clean_player_phrase(stripped, extra_stopwords=extra_stopwords)
    return fallback or None
