from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .relationship_ontology import (
    TeamLeaderIntent,
    metric_prefers_lower,
    mentions_current_scope,
    parse_team_leader_intent,
)
from .team_evaluator import (
    TeamIdentity,
    extract_primary_stat_line,
    format_float,
    resolve_team_from_question,
    safe_float,
    safe_int,
)


@dataclass(slots=True)
class TeamRosterLeaderQuery:
    team: TeamIdentity
    season: int
    intent: TeamLeaderIntent


class TeamRosterLeaderResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)

    def build_snippet(self, question: str) -> EvidenceSnippet | None:
        query = self.parse_question(question)
        if query is None:
            return None
        roster = self.live_client.team_roster(query.team.team_id, season=query.season)
        person_ids = [entry.get("person", {}).get("id") for entry in roster if entry.get("person", {}).get("id")]
        people = self.live_client.people_with_stats(person_ids, season=query.season)
        rows = build_team_leader_rows(people, query)
        if not rows:
            return None
        leader = rows[0]
        summary = build_team_leader_summary(query, leader, rows[1:4])
        return EvidenceSnippet(
            source="Team Roster Leaders",
            title=f"{query.team.name} {query.season} current leaders",
            citation="MLB Stats API active roster plus current season player stats",
            summary=summary,
            payload={
                "analysis_type": "team_roster_leaderboard",
                "mode": "live",
                "team": query.team.name,
                "season": query.season,
                "role": query.intent.role,
                "metric": metric_label(query.intent.metric),
                "direction": query.intent.direction,
                "rows": rows[:8],
            },
        )

    def parse_question(self, question: str) -> TeamRosterLeaderQuery | None:
        lowered = question.lower()
        if "compare" in lowered or " versus " in lowered or " vs " in lowered:
            return None
        if not mentions_current_scope(question):
            return None
        intent = parse_team_leader_intent(question)
        if intent is None:
            return None
        season = self.settings.live_season or date.today().year
        teams = self.live_client.teams(season)
        team = resolve_team_from_question(question, teams)
        if team is None:
            return None
        return TeamRosterLeaderQuery(team=team, season=season, intent=intent)


def build_team_leader_rows(people: list[dict[str, Any]], query: TeamRosterLeaderQuery) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    role = query.intent.role
    metric = query.intent.metric
    direction = query.intent.direction
    lower_is_better = metric_prefers_lower(role, metric)
    higher_is_better = not lower_is_better
    if direction == "worst":
        higher_is_better = not higher_is_better

    for person in people:
        row = build_roster_candidate(person, query.team.abbreviation, role, metric)
        if row is None:
            continue
        rows.append(row)

    if not rows:
        return []
    rows = [row for row in rows if row.get("metric_value") is not None]
    if not rows:
        return []
    rows.sort(
        key=lambda row: (
            -(row["metric_value"]) if higher_is_better else row["metric_value"],
            -(row.get("sample_size") or 0.0),
            str(row.get("player_name") or ""),
        )
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def build_roster_candidate(
    person: dict[str, Any],
    team_abbreviation: str,
    role: str,
    metric: str,
) -> dict[str, Any] | None:
    position_type = str(person.get("primaryPosition", {}).get("type") or "").lower()
    hitting = extract_primary_stat_line(person, "hitting")
    pitching = extract_primary_stat_line(person, "pitching")
    fielding = extract_primary_stat_line(person, "fielding")

    if role in {"hitter", "player"}:
        if not hitting or position_type == "pitcher":
            return None
        return build_hitting_candidate(person, team_abbreviation, hitting, metric)
    if role in {"pitcher", "starter", "reliever"}:
        if not pitching:
            return None
        games_started = safe_int(pitching.get("gamesStarted")) or 0
        innings = safe_float(pitching.get("inningsPitched")) or 0.0
        if role == "starter" and games_started <= 0:
            return None
        if role == "reliever" and (games_started > 0 or innings <= 0):
            return None
        return build_pitching_candidate(person, team_abbreviation, pitching, metric)
    if role == "fielder":
        if not fielding:
            return None
        return build_fielding_candidate(person, team_abbreviation, fielding, metric)
    return None


def build_hitting_candidate(
    person: dict[str, Any],
    team_abbreviation: str,
    hitting: dict[str, Any],
    metric: str,
) -> dict[str, Any] | None:
    metric_value = select_hitting_metric(hitting, metric)
    plate_appearances = safe_float(hitting.get("plateAppearances")) or 0.0
    if metric in {"ops", "obp", "slg", "avg"} and plate_appearances < 10:
        return None
    if metric not in {"ops", "obp", "slg", "avg"} and plate_appearances < 3:
        return None
    return {
        "player_name": person.get("fullName"),
        "team": team_abbreviation,
        "metric": metric_label(metric),
        "metric_value": metric_value,
        "sample_size": plate_appearances,
        "games": safe_int(hitting.get("gamesPlayed")) or 0,
        "plate_appearances": int(round(plate_appearances)),
        "at_bats": safe_int(hitting.get("atBats")) or 0,
        "runs": safe_int(hitting.get("runs")) or 0,
        "avg": safe_float(hitting.get("avg")),
        "obp": safe_float(hitting.get("obp")),
        "slg": safe_float(hitting.get("slg")),
        "ops": safe_float(hitting.get("ops")),
        "doubles": safe_int(hitting.get("doubles")) or 0,
        "triples": safe_int(hitting.get("triples")) or 0,
        "home_runs": safe_int(hitting.get("homeRuns")) or 0,
        "hits": safe_int(hitting.get("hits")) or 0,
        "steals": safe_int(hitting.get("stolenBases")) or 0,
        "caught_stealing": safe_int(hitting.get("caughtStealing")) or 0,
        "hit_by_pitch": safe_int(hitting.get("hitByPitch")) or 0,
        "runs_batted_in": safe_int(hitting.get("rbi")) or 0,
        "walks": safe_int(hitting.get("baseOnBalls")) or 0,
        "strikeouts": safe_int(hitting.get("strikeOuts")) or 0,
    }


def build_pitching_candidate(
    person: dict[str, Any],
    team_abbreviation: str,
    pitching: dict[str, Any],
    metric: str,
) -> dict[str, Any] | None:
    innings = safe_float(pitching.get("inningsPitched")) or 0.0
    if metric in {"era", "whip", "strikeouts_per_9"} and innings < 5.0:
        return None
    if metric not in {"era", "whip", "strikeouts_per_9"} and innings <= 0 and (safe_int(pitching.get("gamesPlayed")) or 0) <= 0:
        return None
    metric_value = select_pitching_metric(pitching, metric)
    return {
        "player_name": person.get("fullName"),
        "team": team_abbreviation,
        "metric": metric_label(metric),
        "metric_value": metric_value,
        "sample_size": innings,
        "games": safe_int(pitching.get("gamesPlayed")) or 0,
        "innings": innings,
        "games_started": safe_int(pitching.get("gamesStarted")) or 0,
        "era": safe_float(pitching.get("era")),
        "whip": safe_float(pitching.get("whip")),
        "wins": safe_int(pitching.get("wins")) or 0,
        "losses": safe_int(pitching.get("losses")) or 0,
        "saves": safe_int(pitching.get("saves")) or 0,
        "holds": safe_int(pitching.get("holds")) or 0,
        "hits_allowed": safe_int(pitching.get("hits")) or 0,
        "earned_runs": safe_int(pitching.get("earnedRuns")) or 0,
        "home_runs_allowed": safe_int(pitching.get("homeRuns")) or 0,
        "walks": safe_int(pitching.get("baseOnBalls")) or 0,
        "strikeouts": safe_int(pitching.get("strikeOuts")) or 0,
        "strikeouts_per_9": safe_float(pitching.get("strikeoutsPer9Inn")),
    }


def build_fielding_candidate(
    person: dict[str, Any],
    team_abbreviation: str,
    fielding: dict[str, Any],
    metric: str,
) -> dict[str, Any] | None:
    games = safe_float(fielding.get("gamesPlayed")) or 0.0
    if games < 5.0:
        return None
    metric_value = select_fielding_metric(fielding, metric)
    return {
        "player_name": person.get("fullName"),
        "team": team_abbreviation,
        "metric": metric_label(metric),
        "metric_value": metric_value,
        "sample_size": games,
        "games": int(round(games)),
        "position": fielding.get("position", {}).get("abbreviation") or person.get("primaryPosition", {}).get("abbreviation"),
        "fielding_pct": safe_float(fielding.get("fielding")),
        "errors": safe_int(fielding.get("errors")) or 0,
        "assists": safe_int(fielding.get("assists")) or 0,
        "putouts": safe_int(fielding.get("putOuts")) or 0,
        "double_plays": safe_int(fielding.get("doublePlays")) or 0,
    }


def select_hitting_metric(hitting: dict[str, Any], metric: str) -> float | None:
    return {
        "games": safe_float(hitting.get("gamesPlayed")),
        "plate_appearances": safe_float(hitting.get("plateAppearances")),
        "at_bats": safe_float(hitting.get("atBats")),
        "ops": safe_float(hitting.get("ops")),
        "obp": safe_float(hitting.get("obp")),
        "slg": safe_float(hitting.get("slg")),
        "avg": safe_float(hitting.get("avg")),
        "runs": safe_float(hitting.get("runs")),
        "doubles": safe_float(hitting.get("doubles")),
        "triples": safe_float(hitting.get("triples")),
        "home_runs": safe_float(hitting.get("homeRuns")),
        "hits": safe_float(hitting.get("hits")),
        "rbi": safe_float(hitting.get("rbi")),
        "steals": safe_float(hitting.get("stolenBases")),
        "caught_stealing": safe_float(hitting.get("caughtStealing")),
        "hit_by_pitch": safe_float(hitting.get("hitByPitch")),
        "walks": safe_float(hitting.get("baseOnBalls")),
        "strikeouts": safe_float(hitting.get("strikeOuts")),
    }.get(metric)


def select_pitching_metric(pitching: dict[str, Any], metric: str) -> float | None:
    return {
        "games": safe_float(pitching.get("gamesPlayed")),
        "games_started": safe_float(pitching.get("gamesStarted")),
        "era": safe_float(pitching.get("era")),
        "whip": safe_float(pitching.get("whip")),
        "hits_allowed": safe_float(pitching.get("hits")),
        "earned_runs": safe_float(pitching.get("earnedRuns")),
        "home_runs_allowed": safe_float(pitching.get("homeRuns")),
        "walks": safe_float(pitching.get("baseOnBalls")),
        "strikeouts": safe_float(pitching.get("strikeOuts")),
        "wins": safe_float(pitching.get("wins")),
        "losses": safe_float(pitching.get("losses")),
        "saves": safe_float(pitching.get("saves")),
        "holds": safe_float(pitching.get("holds")),
        "innings": safe_float(pitching.get("inningsPitched")),
        "strikeouts_per_9": safe_float(pitching.get("strikeoutsPer9Inn")),
    }.get(metric)


def select_fielding_metric(fielding: dict[str, Any], metric: str) -> float | None:
    return {
        "games": safe_float(fielding.get("gamesPlayed")),
        "fielding_pct": safe_float(fielding.get("fielding")),
        "errors": safe_float(fielding.get("errors")),
        "assists": safe_float(fielding.get("assists")),
        "putouts": safe_float(fielding.get("putOuts")),
        "double_plays": safe_float(fielding.get("doublePlays")),
    }.get(metric)


def metric_label(metric: str) -> str:
    return {
        "ops": "OPS",
        "obp": "OBP",
        "slg": "SLG",
        "avg": "AVG",
        "games": "G",
        "plate_appearances": "PA",
        "at_bats": "AB",
        "runs": "Runs",
        "doubles": "2B",
        "triples": "3B",
        "home_runs": "HR",
        "hits": "Hits",
        "rbi": "RBI",
        "steals": "SB",
        "caught_stealing": "CS",
        "hit_by_pitch": "HBP",
        "walks": "BB",
        "strikeouts": "SO",
        "era": "ERA",
        "whip": "WHIP",
        "wins": "Wins",
        "losses": "Losses",
        "saves": "Saves",
        "holds": "Holds",
        "innings": "IP",
        "games_started": "GS",
        "hits_allowed": "H Allowed",
        "earned_runs": "ER",
        "home_runs_allowed": "HR Allowed",
        "strikeouts_per_9": "K/9",
        "fielding_pct": "Fld%",
        "errors": "Errors",
        "assists": "Assists",
        "putouts": "Putouts",
        "double_plays": "DP",
    }.get(metric, metric)


def build_team_leader_summary(
    query: TeamRosterLeaderQuery,
    leader: dict[str, Any],
    runners_up: list[dict[str, Any]],
) -> str:
    direction_phrase = "top" if query.intent.direction == "best" else "bottom"
    role_phrase = {
        "hitter": "hitter",
        "pitcher": "pitcher",
        "starter": "starter",
        "reliever": "reliever",
        "fielder": "defender",
        "player": "player",
    }.get(query.intent.role, query.intent.role)
    value_text = format_metric_value(query.intent.metric, leader.get("metric_value"))
    sample_text = build_sample_text(query.intent.role, leader)
    summary = (
        f"Right now the {query.team.club_name}' {direction_phrase} {role_phrase} by "
        f"{metric_label(query.intent.metric)} is {leader.get('player_name')} at {value_text}{sample_text}."
    )
    if runners_up:
        next_text = "; ".join(
            f"{row.get('player_name')} {format_metric_value(query.intent.metric, row.get('metric_value'))}"
            for row in runners_up
            if row.get("metric_value") is not None
        )
        if next_text:
            summary = f"{summary} Next on the board: {next_text}."
    return summary


def format_metric_value(metric: str, value: Any) -> str:
    converted = safe_float(value)
    if converted is None:
        return "unknown"
    if metric in {"ops", "obp", "slg", "avg", "era", "whip", "fielding_pct"}:
        return f"{converted:.3f}"
    if metric == "strikeouts_per_9":
        return f"{converted:.2f}"
    if metric == "innings":
        return f"{converted:.1f}"
    return f"{int(round(converted))}"


def build_sample_text(role: str, row: dict[str, Any]) -> str:
    if role in {"hitter", "player"}:
        return f" across {row.get('plate_appearances', 0)} PA"
    if role in {"pitcher", "starter", "reliever"}:
        innings = safe_float(row.get("innings"))
        if innings is not None:
            return f" over {innings:.1f} IP"
    if role == "fielder":
        games = row.get("games")
        if games:
            return f" across {games} game(s)"
    return ""
