from __future__ import annotations

from dataclasses import dataclass


CURRENT_SCOPE_HINTS = {
    "today",
    "tonight",
    "this week",
    "this season",
    "this year",
    "current",
    "current season",
    "current year",
    "right now",
    "so far",
    "season so far",
    "to date",
}

TEAM_STATUS_HINTS = {
    "how are",
    "how is",
    "how're",
    "how's",
    "doing so far",
    "looking so far",
    "playing so far",
    "season going",
    "season so far",
    "start so far",
    "doing this year",
    "doing this season",
}

LEADER_DIRECTION_HINTS = {
    "best": "best",
    "top": "best",
    "highest": "best",
    "most": "best",
    "worst": "worst",
    "lowest": "worst",
    "least": "worst",
}

ROLE_HINTS = {
    "hitter": "hitter",
    "batter": "hitter",
    "offensive player": "hitter",
    "offense": "hitter",
    "lineup piece": "hitter",
    "pitcher": "pitcher",
    "starter": "starter",
    "starting pitcher": "starter",
    "rotation arm": "starter",
    "reliever": "reliever",
    "bullpen arm": "reliever",
    "closer": "reliever",
    "fielder": "fielder",
    "defender": "fielder",
    "defensive player": "fielder",
    "glove": "fielder",
    "player": "player",
}

OFFENSE_METRIC_HINTS = {
    "ops": "ops",
    "obp": "obp",
    "slg": "slg",
    "avg": "avg",
    "batting average": "avg",
    "home run": "home_runs",
    "home runs": "home_runs",
    "hr": "home_runs",
    "hits": "hits",
    "rbi": "rbi",
    "walks": "walks",
    "bb": "walks",
    "strikeouts": "strikeouts",
    "so": "strikeouts",
}

PITCHING_METRIC_HINTS = {
    "era": "era",
    "whip": "whip",
    "strikeouts": "strikeouts",
    "so": "strikeouts",
    "wins": "wins",
    "saves": "saves",
    "holds": "holds",
    "innings": "innings",
    "innings pitched": "innings",
    "k/9": "strikeouts_per_9",
    "strikeouts per 9": "strikeouts_per_9",
}

FIELDING_METRIC_HINTS = {
    "fielding percentage": "fielding_pct",
    "fielding pct": "fielding_pct",
    "fielding": "fielding_pct",
    "errors": "errors",
    "assists": "assists",
    "putouts": "putouts",
}


@dataclass(slots=True)
class TeamLeaderIntent:
    direction: str
    role: str
    metric: str


def mentions_current_scope(question: str) -> bool:
    lowered = question.lower()
    return any(token in lowered for token in CURRENT_SCOPE_HINTS)


def is_current_team_status_question(question: str) -> bool:
    lowered = question.lower()
    return any(token in lowered for token in TEAM_STATUS_HINTS) and mentions_current_scope(question)


def parse_team_leader_intent(question: str) -> TeamLeaderIntent | None:
    lowered = question.lower()
    direction = None
    for token, resolved in LEADER_DIRECTION_HINTS.items():
        if token in lowered:
            direction = resolved
            break
    if direction is None:
        return None

    role = None
    for token, resolved in ROLE_HINTS.items():
        if token in lowered:
            role = resolved
            break
    if role is None:
        return None

    metric = infer_default_metric(role)
    hints = metric_hints_for_role(role)
    for token, resolved in hints.items():
        if token in lowered:
            metric = resolved
            break
    return TeamLeaderIntent(direction=direction, role=role, metric=metric)


def infer_default_metric(role: str) -> str:
    if role == "pitcher":
        return "era"
    if role == "starter":
        return "era"
    if role == "reliever":
        return "era"
    if role == "fielder":
        return "fielding_pct"
    return "ops"


def metric_hints_for_role(role: str) -> dict[str, str]:
    if role in {"pitcher", "starter", "reliever"}:
        return PITCHING_METRIC_HINTS
    if role == "fielder":
        return FIELDING_METRIC_HINTS
    return OFFENSE_METRIC_HINTS


def is_lower_better_metric(metric: str) -> bool:
    return metric in {"era", "whip", "errors", "strikeouts"}
