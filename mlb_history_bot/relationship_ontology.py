from __future__ import annotations

from dataclasses import dataclass
import re


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
    "games played": "games",
    "games": "games",
    "plate appearances": "plate_appearances",
    "pa": "plate_appearances",
    "at bats": "at_bats",
    "ab": "at_bats",
    "ops": "ops",
    "obp": "obp",
    "slg": "slg",
    "avg": "avg",
    "batting average": "avg",
    "runs scored": "runs",
    "runs": "runs",
    "doubles": "doubles",
    "double": "doubles",
    "triples": "triples",
    "triple": "triples",
    "home run": "home_runs",
    "home runs": "home_runs",
    "hr": "home_runs",
    "hits": "hits",
    "rbi": "rbi",
    "stolen bases": "steals",
    "stolen base": "steals",
    "steals": "steals",
    "sb": "steals",
    "caught stealing": "caught_stealing",
    "cs": "caught_stealing",
    "hit by pitch": "hit_by_pitch",
    "hbp": "hit_by_pitch",
    "walks": "walks",
    "bb": "walks",
    "strikeouts": "strikeouts",
    "so": "strikeouts",
}

PITCHING_METRIC_HINTS = {
    "games pitched": "games",
    "games": "games",
    "starts": "games_started",
    "games started": "games_started",
    "gs": "games_started",
    "era": "era",
    "whip": "whip",
    "hits allowed": "hits_allowed",
    "earned runs": "earned_runs",
    "earned run": "earned_runs",
    "home runs allowed": "home_runs_allowed",
    "walks allowed": "walks",
    "strikeouts": "strikeouts",
    "so": "strikeouts",
    "wins": "wins",
    "losses": "losses",
    "saves": "saves",
    "holds": "holds",
    "innings": "innings",
    "innings pitched": "innings",
    "k/9": "strikeouts_per_9",
    "strikeouts per 9": "strikeouts_per_9",
}

FIELDING_METRIC_HINTS = {
    "games played": "games",
    "games": "games",
    "fielding percentage": "fielding_pct",
    "fielding pct": "fielding_pct",
    "fielding": "fielding_pct",
    "errors": "errors",
    "assists": "assists",
    "putouts": "putouts",
    "double plays": "double_plays",
}


@dataclass(slots=True)
class TeamLeaderIntent:
    direction: str
    role: str
    metric: str


def mentions_current_scope(question: str) -> bool:
    lowered = question.lower()
    return any(_contains_hint(lowered, token) for token in CURRENT_SCOPE_HINTS)


def is_current_team_status_question(question: str) -> bool:
    lowered = question.lower()
    return any(_contains_hint(lowered, token) for token in TEAM_STATUS_HINTS) and mentions_current_scope(question)


def parse_team_leader_intent(question: str) -> TeamLeaderIntent | None:
    lowered = question.lower()
    direction = None
    for token, resolved in LEADER_DIRECTION_HINTS.items():
        if _contains_hint(lowered, token):
            direction = resolved
            break
    if direction is None:
        return None

    role = None
    for token, resolved in ROLE_HINTS.items():
        if _contains_hint(lowered, token):
            role = resolved
            break
    metric = None
    inferred_role = None
    for role_name, hints in (
        ("pitcher", PITCHING_METRIC_HINTS),
        ("fielder", FIELDING_METRIC_HINTS),
        ("hitter", OFFENSE_METRIC_HINTS),
    ):
        for token, resolved in hints.items():
            if _contains_hint(lowered, token):
                metric = resolved
                inferred_role = role_name
                break
        if metric is not None:
            break

    if role is None:
        role = inferred_role
    if role is None:
        return None
    if metric is None:
        metric = infer_default_metric(role)
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


def metric_prefers_lower(role: str, metric: str) -> bool:
    if role in {"pitcher", "starter", "reliever"}:
        return metric in {
            "era",
            "whip",
            "losses",
            "hits_allowed",
            "earned_runs",
            "home_runs_allowed",
            "walks",
        }
    if role == "fielder":
        return metric in {"errors"}
    return metric in {"strikeouts", "caught_stealing"}


def is_lower_better_metric(metric: str) -> bool:
    return metric_prefers_lower("player", metric)


def _contains_hint(text: str, token: str) -> bool:
    pattern = rf"(?<![A-Za-z0-9]){re.escape(token)}(?![A-Za-z0-9])"
    return re.search(pattern, text) is not None
