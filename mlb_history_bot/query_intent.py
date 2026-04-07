from __future__ import annotations

from dataclasses import dataclass


BEST_HINTS = {
    "best",
    "strongest",
    "greatest",
    "dominant",
}
WORST_HINTS = {
    "worst",
    "weakest",
    "poorest",
    "shakiest",
}
HIGH_HINTS = {
    "highest",
    "most",
    "top",
    "leader",
    "leaders",
    "biggest",
    "largest",
    "greatest",
}
LOW_HINTS = {
    "lowest",
    "least",
    "bottom",
    "fewest",
    "smallest",
}
CURRENT_SCOPE_HINTS = {
    "current",
    "today",
    "tonight",
    "right now",
    "latest",
    "this season",
    "season so far",
    "this year",
    "so far",
}
LEADERBOARD_START_HINTS = (
    "who ",
    "who's ",
    "whos ",
    "which player",
    "which pitcher",
    "which batter",
    "which hitter",
    "which team",
    "what player",
    "what pitcher",
    "show me",
    "list",
    "find",
)


@dataclass(slots=True, frozen=True)
class RankingIntent:
    descriptor: str
    sort_desc: bool
    matched: bool


def contains_any(lowered_question: str, hints: set[str] | tuple[str, ...]) -> bool:
    return any(hint in lowered_question for hint in hints)


def has_ranking_hint(lowered_question: str) -> bool:
    return any(
        hint in lowered_question
        for hint in BEST_HINTS | WORST_HINTS | HIGH_HINTS | LOW_HINTS
    )


def looks_like_leaderboard_question(lowered_question: str) -> bool:
    if lowered_question.startswith(LEADERBOARD_START_HINTS):
        return True
    return has_ranking_hint(lowered_question)


def mentions_current_scope(lowered_question: str) -> bool:
    return contains_any(lowered_question, CURRENT_SCOPE_HINTS)


def detect_ranking_intent(
    lowered_question: str,
    *,
    higher_is_better: bool,
    require_hint: bool = False,
    fallback_label: str | None = None,
) -> RankingIntent | None:
    wants_high = contains_any(lowered_question, HIGH_HINTS)
    wants_low = contains_any(lowered_question, LOW_HINTS)
    wants_best = contains_any(lowered_question, BEST_HINTS)
    wants_worst = contains_any(lowered_question, WORST_HINTS)

    if not (wants_high or wants_low or wants_best or wants_worst):
        if require_hint:
            return None
        return RankingIntent(
            descriptor=fallback_label or ("best" if higher_is_better else "worst"),
            sort_desc=higher_is_better,
            matched=False,
        )

    if wants_high:
        return RankingIntent(descriptor="highest", sort_desc=True, matched=True)
    if wants_low:
        return RankingIntent(descriptor="lowest", sort_desc=False, matched=True)
    if wants_worst:
        return RankingIntent(
            descriptor="worst",
            sort_desc=not higher_is_better,
            matched=True,
        )
    return RankingIntent(
        descriptor="best",
        sort_desc=higher_is_better,
        matched=True,
    )
