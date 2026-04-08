from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .query_utils import extract_explicit_year
from .relationship_ontology import TeamLeaderIntent, metric_prefers_lower, parse_team_leader_intent
from .storage import table_exists
from .team_roster_leaders import (
    TeamRosterLeaderQuery,
    build_sample_text,
    build_team_leader_rows,
    format_metric_value,
    metric_label,
)
from .team_season_compare import TeamSeasonReference, clean_team_phrase, resolve_team_season_reference
from .team_evaluator import safe_float, safe_int


LEADER_FILLER_PATTERN = re.compile(
    r"\b(?:who|what|which|was|were|is|are|the|a|an|of|for|in|during|on|to|did|does|had|have|has|with|by|at)\b",
    re.IGNORECASE,
)
LEADER_ROLE_PATTERN = re.compile(
    r"\b(?:best|worst|highest|lowest|most|least|top|bottom|hitter|batter|pitcher|starter|starting\s+pitcher|rotation\s+arm|reliever|bullpen\s+arm|closer|fielder|defender|defensive\s+player|offensive\s+player|player)\b",
    re.IGNORECASE,
)
LEADER_METRIC_PATTERN = re.compile(
    r"\b(?:ops|obp|slg|avg|batting\s+average|games|games\s+played|plate\s+appearances|pa|at\s+bats|ab|runs|runs\s+scored|doubles|triples|home\s+runs?|hr|hits|rbi|stolen\s+bases?|steals|sb|caught\s+stealing|cs|hit\s+by\s+pitch|hbp|era|whip|wins|losses|saves|holds|innings(?:\s+pitched)?|games\s+started|starts|gs|hits\s+allowed|earned\s+runs?|home\s+runs?\s+allowed|walks(?:\s+allowed)?|strikeouts?|k/9|strikeouts\s+per\s+9|fielding\s+percentage|fielding\s+pct|fielding|errors|assists|putouts|double\s+plays)\b",
    re.IGNORECASE,
)
YEAR_PATTERN = re.compile(r"\b(18\d{2}|19\d{2}|20\d{2})\b")


@dataclass(slots=True)
class TeamSeasonLeaderQuery:
    reference: TeamSeasonReference
    intent: TeamLeaderIntent


class TeamSeasonLeaderResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = self.parse_question(connection, question)
        if query is None:
            return None
        if query.reference.live_team is not None:
            return self._build_live_snippet(query)
        return self._build_historical_snippet(connection, query)

    def parse_question(self, connection, question: str) -> TeamSeasonLeaderQuery | None:
        lowered = question.lower()
        if any(token in lowered for token in ("compare", " versus ", " vs ", "better than", "worse than", "matched up against")):
            return None
        season = extract_explicit_year(question)
        if season is None:
            return None
        intent = parse_team_leader_intent(question)
        if intent is None:
            return None
        current_season = self.settings.live_season or date.today().year
        team_phrase = extract_team_phrase_from_leader_question(question)
        if not team_phrase:
            return None
        reference = resolve_team_season_reference(connection, team_phrase, season, self.live_client, current_season)
        if reference is None:
            return None
        return TeamSeasonLeaderQuery(reference=reference, intent=intent)

    def _build_live_snippet(self, query: TeamSeasonLeaderQuery) -> EvidenceSnippet | None:
        team = query.reference.live_team
        if team is None:
            return None
        roster = self.live_client.team_roster(team.team_id, season=query.reference.season)
        person_ids = [entry.get("person", {}).get("id") for entry in roster if entry.get("person", {}).get("id")]
        people = self.live_client.people_with_stats(person_ids, season=query.reference.season)
        rows = build_team_leader_rows(
            people,
            TeamRosterLeaderQuery(team=team, season=query.reference.season, intent=query.intent),
        )
        if not rows:
            return None
        leader = rows[0]
        summary = build_team_season_leader_summary(query.reference.display_name, query.intent, leader, rows[1:4], live=True)
        return EvidenceSnippet(
            source="Team Season Leaders",
            title=f"{query.reference.display_name} season leaders",
            citation="MLB Stats API active roster plus current season player stats",
            summary=summary,
            payload={
                "analysis_type": "team_season_leaderboard",
                "mode": "live",
                "season": query.reference.season,
                "team": query.reference.display_name,
                "role": query.intent.role,
                "metric": metric_label(query.intent.metric),
                "direction": query.intent.direction,
                "rows": rows[:10],
            },
        )

    def _build_historical_snippet(self, connection, query: TeamSeasonLeaderQuery) -> EvidenceSnippet | None:
        role = query.intent.role
        if role in {"hitter", "player"}:
            rows = load_historical_hitting_rows(connection, query.reference, query.intent)
        elif role in {"pitcher", "starter", "reliever"}:
            rows = load_historical_pitching_rows(connection, query.reference, query.intent)
        else:
            rows = load_historical_fielding_rows(connection, query.reference, query.intent)
        if not rows:
            return None
        leader = rows[0]
        summary = build_team_season_leader_summary(query.reference.display_name, query.intent, leader, rows[1:4], live=False)
        return EvidenceSnippet(
            source="Team Season Leaders",
            title=f"{query.reference.display_name} season leaders",
            citation="Lahman Batting, Pitching, Fielding, and People tables",
            summary=summary,
            payload={
                "analysis_type": "team_season_leaderboard",
                "mode": "historical",
                "season": query.reference.season,
                "team": query.reference.display_name,
                "role": query.intent.role,
                "metric": metric_label(query.intent.metric),
                "direction": query.intent.direction,
                "rows": rows[:10],
            },
        )


def extract_team_phrase_from_leader_question(question: str) -> str:
    text = YEAR_PATTERN.sub(" ", question)
    text = LEADER_ROLE_PATTERN.sub(" ", text)
    text = LEADER_METRIC_PATTERN.sub(" ", text)
    text = LEADER_FILLER_PATTERN.sub(" ", text)
    text = re.sub(r"[?.!,:'\"]", " ", text)
    text = re.sub(r"\s{2,}", " ", text).strip()
    return clean_team_phrase(text)


def load_historical_hitting_rows(connection, reference: TeamSeasonReference, intent: TeamLeaderIntent) -> list[dict[str, Any]]:
    if not (table_exists(connection, "lahman_batting") and table_exists(connection, "lahman_people")):
        return []
    rows = connection.execute(
        """
        SELECT
            b.playerid,
            p.namefirst,
            p.namelast,
            SUM(CAST(COALESCE(b.g, '0') AS INTEGER)) AS games,
            SUM(CAST(COALESCE(b.ab, '0') AS INTEGER)) AS at_bats,
            SUM(CAST(COALESCE(b.r, '0') AS INTEGER)) AS runs,
            SUM(CAST(COALESCE(b.h, '0') AS INTEGER)) AS hits,
            SUM(CAST(COALESCE(b.c_2b, '0') AS INTEGER)) AS doubles,
            SUM(CAST(COALESCE(b.c_3b, '0') AS INTEGER)) AS triples,
            SUM(CAST(COALESCE(b.hr, '0') AS INTEGER)) AS home_runs,
            SUM(CAST(COALESCE(b.rbi, '0') AS INTEGER)) AS rbi,
            SUM(CAST(COALESCE(b.sb, '0') AS INTEGER)) AS steals,
            SUM(CAST(COALESCE(b.cs, '0') AS INTEGER)) AS caught_stealing,
            SUM(CAST(COALESCE(b.bb, '0') AS INTEGER)) AS walks,
            SUM(CAST(COALESCE(b.so, '0') AS INTEGER)) AS strikeouts,
            SUM(CAST(COALESCE(b.hbp, '0') AS INTEGER)) AS hit_by_pitch,
            SUM(CAST(COALESCE(b.sh, '0') AS INTEGER)) AS sacrifice_hits,
            SUM(CAST(COALESCE(b.sf, '0') AS INTEGER)) AS sacrifice_flies
        FROM lahman_batting AS b
        JOIN lahman_people AS p
          ON p.playerid = b.playerid
        WHERE CAST(b.yearid AS INTEGER) = ?
          AND lower(b.teamid) = ?
        GROUP BY b.playerid, p.namefirst, p.namelast
        """,
        (reference.season, (reference.team_code or "").lower()),
    ).fetchall()

    candidates: list[dict[str, Any]] = []
    for row in rows:
        at_bats = safe_int(row["at_bats"]) or 0
        walks = safe_int(row["walks"]) or 0
        hit_by_pitch = safe_int(row["hit_by_pitch"]) or 0
        sacrifice_flies = safe_int(row["sacrifice_flies"]) or 0
        sacrifice_hits = safe_int(row["sacrifice_hits"]) or 0
        hits = safe_int(row["hits"]) or 0
        doubles = safe_int(row["doubles"]) or 0
        triples = safe_int(row["triples"]) or 0
        home_runs = safe_int(row["home_runs"]) or 0
        plate_appearances = at_bats + walks + hit_by_pitch + sacrifice_flies + sacrifice_hits
        if intent.metric in {"ops", "obp", "slg", "avg"} and plate_appearances < 25:
            continue
        if intent.metric not in {"ops", "obp", "slg", "avg"} and plate_appearances < 5:
            continue
        avg = (hits / at_bats) if at_bats else None
        obp_denom = at_bats + walks + hit_by_pitch + sacrifice_flies
        obp = ((hits + walks + hit_by_pitch) / obp_denom) if obp_denom else None
        singles = hits - doubles - triples - home_runs
        slg = ((singles + (2 * doubles) + (3 * triples) + (4 * home_runs)) / at_bats) if at_bats else None
        ops = (obp + slg) if obp is not None and slg is not None else None
        candidate = {
            "player_name": build_person_name(row["namefirst"], row["namelast"], row["playerid"]),
            "team": reference.team_code,
            "metric": metric_label(intent.metric),
            "metric_value": select_historical_hitting_metric(intent.metric, at_bats, plate_appearances, avg, obp, slg, ops, row),
            "sample_size": plate_appearances,
            "games": safe_int(row["games"]) or 0,
            "plate_appearances": plate_appearances,
            "at_bats": at_bats,
            "runs": safe_int(row["runs"]) or 0,
            "avg": avg,
            "obp": obp,
            "slg": slg,
            "ops": ops,
            "doubles": doubles,
            "triples": triples,
            "home_runs": home_runs,
            "hits": hits,
            "steals": safe_int(row["steals"]) or 0,
            "caught_stealing": safe_int(row["caught_stealing"]) or 0,
            "hit_by_pitch": hit_by_pitch,
            "runs_batted_in": safe_int(row["rbi"]) or 0,
            "walks": walks,
            "strikeouts": safe_int(row["strikeouts"]) or 0,
        }
        if candidate["metric_value"] is None:
            continue
        candidates.append(candidate)
    return rank_team_leader_rows(candidates, intent)


def load_historical_pitching_rows(connection, reference: TeamSeasonReference, intent: TeamLeaderIntent) -> list[dict[str, Any]]:
    if not (table_exists(connection, "lahman_pitching") and table_exists(connection, "lahman_people")):
        return []
    rows = connection.execute(
        """
        SELECT
            pch.playerid,
            ppl.namefirst,
            ppl.namelast,
            SUM(CAST(COALESCE(pch.w, '0') AS INTEGER)) AS wins,
            SUM(CAST(COALESCE(pch.l, '0') AS INTEGER)) AS losses,
            SUM(CAST(COALESCE(pch.g, '0') AS INTEGER)) AS games,
            SUM(CAST(COALESCE(pch.gs, '0') AS INTEGER)) AS games_started,
            SUM(CAST(COALESCE(pch.sv, '0') AS INTEGER)) AS saves,
            SUM(CAST(COALESCE(pch.ipouts, '0') AS INTEGER)) AS ipouts,
            SUM(CAST(COALESCE(pch.h, '0') AS INTEGER)) AS hits_allowed,
            SUM(CAST(COALESCE(pch.er, '0') AS INTEGER)) AS earned_runs,
            SUM(CAST(COALESCE(pch.hr, '0') AS INTEGER)) AS home_runs_allowed,
            SUM(CAST(COALESCE(pch.bb, '0') AS INTEGER)) AS walks,
            SUM(CAST(COALESCE(pch.so, '0') AS INTEGER)) AS strikeouts
        FROM lahman_pitching AS pch
        JOIN lahman_people AS ppl
          ON ppl.playerid = pch.playerid
        WHERE CAST(pch.yearid AS INTEGER) = ?
          AND lower(pch.teamid) = ?
        GROUP BY pch.playerid, ppl.namefirst, ppl.namelast
        """,
        (reference.season, (reference.team_code or "").lower()),
    ).fetchall()

    candidates: list[dict[str, Any]] = []
    for row in rows:
        games = safe_int(row["games"]) or 0
        games_started = safe_int(row["games_started"]) or 0
        if intent.role == "starter" and games_started <= 0:
            continue
        if intent.role == "reliever" and (games - games_started) <= 0:
            continue
        ipouts = safe_int(row["ipouts"]) or 0
        if intent.metric in {"era", "whip", "strikeouts_per_9"} and ipouts < 30:
            continue
        if intent.metric not in {"era", "whip", "strikeouts_per_9"} and games <= 0:
            continue
        innings = outs_to_innings_notation(ipouts)
        hits_allowed = safe_int(row["hits_allowed"]) or 0
        walks = safe_int(row["walks"]) or 0
        strikeouts = safe_int(row["strikeouts"]) or 0
        candidate = {
            "player_name": build_person_name(row["namefirst"], row["namelast"], row["playerid"]),
            "team": reference.team_code,
            "metric": metric_label(intent.metric),
            "metric_value": select_historical_pitching_metric(intent.metric, ipouts, row),
            "sample_size": ipouts,
            "games": games,
            "innings": innings,
            "games_started": games_started,
            "era": (27.0 * (safe_int(row["earned_runs"]) or 0) / ipouts) if ipouts else None,
            "whip": ((hits_allowed + walks) / (ipouts / 3.0)) if ipouts else None,
            "wins": safe_int(row["wins"]) or 0,
            "losses": safe_int(row["losses"]) or 0,
            "saves": safe_int(row["saves"]) or 0,
            "holds": None,
            "hits_allowed": hits_allowed,
            "earned_runs": safe_int(row["earned_runs"]) or 0,
            "home_runs_allowed": safe_int(row["home_runs_allowed"]) or 0,
            "walks": walks,
            "strikeouts": strikeouts,
            "strikeouts_per_9": ((27.0 * strikeouts) / ipouts) if ipouts else None,
        }
        if candidate["metric_value"] is None:
            continue
        candidates.append(candidate)
    return rank_team_leader_rows(candidates, intent)


def load_historical_fielding_rows(connection, reference: TeamSeasonReference, intent: TeamLeaderIntent) -> list[dict[str, Any]]:
    if not (table_exists(connection, "lahman_fielding") and table_exists(connection, "lahman_people")):
        return []
    rows = connection.execute(
        """
        SELECT
            fld.playerid,
            ppl.namefirst,
            ppl.namelast,
            GROUP_CONCAT(DISTINCT fld.pos) AS positions,
            SUM(CAST(COALESCE(fld.g, '0') AS INTEGER)) AS games,
            SUM(CAST(COALESCE(fld.po, '0') AS INTEGER)) AS putouts,
            SUM(CAST(COALESCE(fld.a, '0') AS INTEGER)) AS assists,
            SUM(CAST(COALESCE(fld.e, '0') AS INTEGER)) AS errors,
            SUM(CAST(COALESCE(fld.dp, '0') AS INTEGER)) AS double_plays
        FROM lahman_fielding AS fld
        JOIN lahman_people AS ppl
          ON ppl.playerid = fld.playerid
        WHERE CAST(fld.yearid AS INTEGER) = ?
          AND lower(fld.teamid) = ?
        GROUP BY fld.playerid, ppl.namefirst, ppl.namelast
        """,
        (reference.season, (reference.team_code or "").lower()),
    ).fetchall()

    candidates: list[dict[str, Any]] = []
    for row in rows:
        games = safe_int(row["games"]) or 0
        if games < 10:
            continue
        putouts = safe_int(row["putouts"]) or 0
        assists = safe_int(row["assists"]) or 0
        errors = safe_int(row["errors"]) or 0
        chances = putouts + assists + errors
        fielding_pct = ((putouts + assists) / chances) if chances else None
        candidate = {
            "player_name": build_person_name(row["namefirst"], row["namelast"], row["playerid"]),
            "team": reference.team_code,
            "metric": metric_label(intent.metric),
            "metric_value": select_historical_fielding_metric(intent.metric, fielding_pct, row),
            "sample_size": games,
            "games": games,
            "position": summarize_positions(row["positions"]),
            "fielding_pct": fielding_pct,
            "errors": errors,
            "assists": assists,
            "putouts": putouts,
            "double_plays": safe_int(row["double_plays"]) or 0,
        }
        if candidate["metric_value"] is None:
            continue
        candidates.append(candidate)
    return rank_team_leader_rows(candidates, intent)


def select_historical_hitting_metric(
    metric: str,
    at_bats: int,
    plate_appearances: int,
    avg: float | None,
    obp: float | None,
    slg: float | None,
    ops: float | None,
    row,
) -> float | None:
    singles = (safe_int(row["hits"]) or 0) - (safe_int(row["doubles"]) or 0) - (safe_int(row["triples"]) or 0) - (safe_int(row["home_runs"]) or 0)
    extra_base_hits = (safe_int(row["doubles"]) or 0) + (safe_int(row["triples"]) or 0) + (safe_int(row["home_runs"]) or 0)
    total_bases = singles + (2 * (safe_int(row["doubles"]) or 0)) + (3 * (safe_int(row["triples"]) or 0)) + (4 * (safe_int(row["home_runs"]) or 0))
    return {
        "games": safe_float(row["games"]),
        "plate_appearances": float(plate_appearances),
        "at_bats": float(at_bats),
        "runs": safe_float(row["runs"]),
        "avg": avg,
        "obp": obp,
        "slg": slg,
        "ops": ops,
        "singles": float(singles),
        "doubles": safe_float(row["doubles"]),
        "triples": safe_float(row["triples"]),
        "total_bases": float(total_bases),
        "extra_base_hits": float(extra_base_hits),
        "home_runs": safe_float(row["home_runs"]),
        "hits": safe_float(row["hits"]),
        "rbi": safe_float(row["rbi"]),
        "steals": safe_float(row["steals"]),
        "caught_stealing": safe_float(row["caught_stealing"]),
        "hit_by_pitch": safe_float(row["hit_by_pitch"]),
        "walks": safe_float(row["walks"]),
        "strikeouts": safe_float(row["strikeouts"]),
    }.get(metric)


def select_historical_pitching_metric(metric: str, ipouts: int, row) -> float | None:
    hits_allowed = safe_int(row["hits_allowed"]) or 0
    walks = safe_int(row["walks"]) or 0
    strikeouts = safe_int(row["strikeouts"]) or 0
    return {
        "games": safe_float(row["games"]),
        "games_started": safe_float(row["games_started"]),
        "era": (27.0 * (safe_int(row["earned_runs"]) or 0) / ipouts) if ipouts else None,
        "whip": ((hits_allowed + walks) / (ipouts / 3.0)) if ipouts else None,
        "wins": safe_float(row["wins"]),
        "losses": safe_float(row["losses"]),
        "saves": safe_float(row["saves"]),
        "innings": outs_to_innings_notation(ipouts),
        "hits_allowed": safe_float(row["hits_allowed"]),
        "earned_runs": safe_float(row["earned_runs"]),
        "home_runs_allowed": safe_float(row["home_runs_allowed"]),
        "walks": safe_float(row["walks"]),
        "strikeouts": safe_float(row["strikeouts"]),
        "strikeouts_per_9": ((27.0 * strikeouts) / ipouts) if ipouts else None,
        "walks_per_9": ((27.0 * walks) / ipouts) if ipouts else None,
        "hits_per_9": ((27.0 * hits_allowed) / ipouts) if ipouts else None,
        "home_runs_per_9": ((27.0 * (safe_int(row["home_runs_allowed"]) or 0)) / ipouts) if ipouts else None,
        "strikeout_to_walk": (strikeouts / walks) if walks else None,
    }.get(metric)


def select_historical_fielding_metric(metric: str, fielding_pct: float | None, row) -> float | None:
    return {
        "games": safe_float(row["games"]),
        "fielding_pct": fielding_pct,
        "errors": safe_float(row["errors"]),
        "assists": safe_float(row["assists"]),
        "putouts": safe_float(row["putouts"]),
        "double_plays": safe_float(row["double_plays"]),
    }.get(metric)


def rank_team_leader_rows(rows: list[dict[str, Any]], intent: TeamLeaderIntent) -> list[dict[str, Any]]:
    lower_is_better = metric_prefers_lower(intent.role, intent.metric)
    higher_is_better = not lower_is_better
    if intent.direction == "worst":
        higher_is_better = not higher_is_better
    rows = [row for row in rows if row.get("metric_value") is not None]
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


def build_team_season_leader_summary(
    display_name: str,
    intent: TeamLeaderIntent,
    leader: dict[str, Any],
    runners_up: list[dict[str, Any]],
    *,
    live: bool,
) -> str:
    direction_phrase = "top" if intent.direction == "best" else "bottom"
    role_phrase = {
        "hitter": "hitter",
        "pitcher": "pitcher",
        "starter": "starter",
        "reliever": "reliever",
        "fielder": "defender",
        "player": "player",
    }.get(intent.role, intent.role)
    timing_phrase = "Right now for" if live else "For"
    value_text = format_metric_value(intent.metric, leader.get("metric_value"))
    sample_text = build_sample_text(intent.role, leader)
    summary = (
        f"{timing_phrase} {display_name}, the {direction_phrase} {role_phrase} by "
        f"{metric_label(intent.metric)} is {leader.get('player_name')} at {value_text}{sample_text}."
    )
    if runners_up:
        next_text = "; ".join(
            f"{row.get('player_name')} {format_metric_value(intent.metric, row.get('metric_value'))}"
            for row in runners_up
            if row.get("metric_value") is not None
        )
        if next_text:
            summary = f"{summary} Next on the board: {next_text}."
    return summary


def build_person_name(first: Any, last: Any, fallback: Any) -> str:
    name = f"{str(first or '').strip()} {str(last or '').strip()}".strip()
    return name or str(fallback or "Unknown")


def summarize_positions(raw_positions: Any) -> str:
    text = str(raw_positions or "").strip()
    if not text:
        return ""
    parts = [part.strip() for part in text.split(",") if part.strip()]
    return "/".join(parts[:4])


def outs_to_innings_notation(ipouts: int) -> float:
    whole = ipouts // 3
    remainder = ipouts % 3
    return float(f"{whole}.{remainder}")
