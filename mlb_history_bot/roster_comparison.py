from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from statistics import mean
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .storage import table_exists
from .team_evaluator import TeamIdentity, build_roster_profile, format_float, safe_float, safe_int
from .team_season_compare import YEAR_TEAM_PATTERN, compute_ops, resolve_team_season_reference


ROSTER_COMPARE_HINTS = {"compare", "vs", "versus", "better than", "worse than", "to the"}
ROSTER_FOCUS_HINTS = {"roster", "lineup", "squad", "club"}
PITCHING_ONLY_HINTS = {"pitching roster", "pitching staff", "rotation", "bullpen"}


@dataclass(slots=True)
class RosterComparisonQuery:
    left: Any
    right: Any


@dataclass(slots=True)
class RosterSnapshot:
    display_name: str
    season: int
    scope: str
    roster_size: int
    hitter_count: int
    pitcher_count: int
    average_age: float | None
    lineup_depth_ops: float | None
    rotation_era: float | None
    bullpen_era: float | None
    top_hitters: list[dict[str, Any]]
    top_pitchers: list[dict[str, Any]]


class RosterComparisonResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        current_season = self.settings.live_season or date.today().year
        query = parse_roster_comparison_query(connection, question, self.live_client, current_season)
        if query is None:
            return None

        left_snapshot = self._build_snapshot(connection, query.left, current_season)
        right_snapshot = self._build_snapshot(connection, query.right, current_season)
        if left_snapshot is None or right_snapshot is None:
            return None

        mode = "hybrid" if query.left.live_team is not None or query.right.live_team is not None else "historical"
        summary = build_roster_comparison_summary(left_snapshot, right_snapshot)
        return EvidenceSnippet(
            source="Roster Comparison",
            title=f"{left_snapshot.display_name} vs {right_snapshot.display_name} roster comparison",
            citation="Lahman batting/pitching rosters plus MLB Stats API active-roster season snapshots",
            summary=summary,
            payload={
                "analysis_type": "roster_comparison",
                "mode": mode,
                "rows": [snapshot_row(left_snapshot), snapshot_row(right_snapshot)],
            },
        )

    def _build_snapshot(self, connection, reference, current_season: int) -> RosterSnapshot | None:
        if reference.live_team is not None and reference.season == current_season:
            return self._build_live_snapshot(reference.live_team, reference.season)
        return build_historical_roster_snapshot(connection, reference)

    def _build_live_snapshot(self, team: TeamIdentity, season: int) -> RosterSnapshot | None:
        roster = self.live_client.team_roster(team.team_id, season=season, roster_type="active")
        if not roster:
            return None
        people = self.live_client.people_with_stats(
            [entry.get("person", {}).get("id") for entry in roster],
            season=season,
        )
        if not people:
            return None
        profile = build_roster_profile(people)
        hitter_count = sum(
            1 for person in people if str(person.get("primaryPosition", {}).get("type") or "").lower() != "pitcher"
        )
        pitcher_count = sum(
            1 for person in people if str(person.get("primaryPosition", {}).get("type") or "").lower() == "pitcher"
        )
        top_hitters = [
            {
                "player": entry["name"],
                "ops": entry.get("ops"),
                "sample": f"{int(entry.get('plate_appearances') or 0)} PA",
            }
            for entry in profile.get("top_hitters") or []
        ]
        top_pitchers = [
            {
                "player": entry["name"],
                "era": entry.get("era"),
                "sample": f"{format_float(entry.get('innings'), 1)} IP",
            }
            for entry in (profile.get("top_starters") or profile.get("top_relievers") or [])
        ]
        return RosterSnapshot(
            display_name=f"{season} {team.name}",
            season=season,
            scope="active roster",
            roster_size=len(roster),
            hitter_count=hitter_count,
            pitcher_count=pitcher_count,
            average_age=safe_float(profile.get("average_age")),
            lineup_depth_ops=safe_float(profile.get("lineup_depth_ops")),
            rotation_era=safe_float(profile.get("rotation_era")),
            bullpen_era=safe_float(profile.get("bullpen_era")),
            top_hitters=top_hitters[:3],
            top_pitchers=top_pitchers[:3],
        )


def parse_roster_comparison_query(connection, question: str, live_client: LiveStatsClient, current_season: int) -> RosterComparisonQuery | None:
    lowered = question.lower()
    if not any(hint in lowered for hint in ROSTER_COMPARE_HINTS):
        return None
    if not any(hint in lowered for hint in ROSTER_FOCUS_HINTS):
        return None
    if any(hint in lowered for hint in PITCHING_ONLY_HINTS):
        return None
    matches = list(YEAR_TEAM_PATTERN.finditer(question))
    if len(matches) < 2:
        return None
    references = []
    for match in matches[:2]:
        season = int(match.group(1))
        phrase = clean_roster_team_phrase(match.group(2))
        reference = resolve_team_season_reference(connection, phrase, season, live_client, current_season)
        if reference is None:
            return None
        references.append(reference)
    return RosterComparisonQuery(left=references[0], right=references[1])


def clean_roster_team_phrase(value: str) -> str:
    cleaned = value.strip(" ?.!,'\"")
    cleaned = re.sub(r"^the\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:roster|lineup|squad|club|team)\b$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:roster|lineup|squad|club|team)\b$", "", cleaned, flags=re.IGNORECASE)
    return re.sub(r"\s{2,}", " ", cleaned).strip()


def build_historical_roster_snapshot(connection, reference) -> RosterSnapshot | None:
    if not (table_exists(connection, "lahman_batting") and table_exists(connection, "lahman_pitching") and table_exists(connection, "lahman_people")):
        return None
    team_code = (reference.team_code or "").lower()
    if not team_code:
        return None
    hitters = fetch_historical_hitters(connection, reference.season, team_code)
    pitchers = fetch_historical_pitchers(connection, reference.season, team_code)
    if not hitters and not pitchers:
        return None

    hitter_pool = [player for player in hitters if (player.get("ab") or 0) > 0]
    hitter_pool.sort(key=lambda player: (-(player.get("ab") or 0), player["player"]))
    top_hitter_pool = sorted(
        hitter_pool[:8] or hitter_pool,
        key=lambda player: (-(player.get("ops") or 0.0), -(player.get("ab") or 0), player["player"]),
    )
    top_hitters = [
        {
            "player": player["player"],
            "ops": player.get("ops"),
            "sample": f"{int(player.get('ab') or 0)} AB",
        }
        for player in top_hitter_pool[:3]
    ]
    lineup_depth_ops = mean([player["ops"] for player in hitter_pool[:6] if player.get("ops") is not None]) if hitter_pool[:6] else None

    starter_pool = [player for player in pitchers if (player.get("games_started") or 0) > 0]
    starter_pool.sort(key=lambda player: (-(player.get("games_started") or 0), -(player.get("innings") or 0.0), player["player"]))
    if not starter_pool:
        starter_pool = sorted(pitchers, key=lambda player: (-(player.get("innings") or 0.0), player["player"]))
    rotation_arms = starter_pool[:4]
    rotation_era = mean([player["era"] for player in rotation_arms if player.get("era") is not None]) if rotation_arms else None

    reliever_pool = [player for player in pitchers if (player.get("games_started") or 0) == 0 and (player.get("innings") or 0.0) > 0]
    reliever_pool.sort(key=lambda player: (-(player.get("innings") or 0.0), player["player"]))
    if not reliever_pool:
        reliever_pool = [player for player in pitchers if player not in rotation_arms]
        reliever_pool.sort(key=lambda player: (-(player.get("innings") or 0.0), player["player"]))
    bullpen_arms = reliever_pool[:4]
    bullpen_era = mean([player["era"] for player in bullpen_arms if player.get("era") is not None]) if bullpen_arms else None

    top_pitcher_pool = rotation_arms or sorted(pitchers, key=lambda player: (-(player.get("innings") or 0.0), player["player"]))
    top_pitchers = [
        {
            "player": player["player"],
            "era": player.get("era"),
            "sample": f"{format_float(player.get('innings'), 1)} IP",
        }
        for player in top_pitcher_pool[:3]
    ]
    roster_ids = {player["player_id"] for player in hitters} | {player["player_id"] for player in pitchers}
    age_values = [player["age"] for player in [*hitters, *pitchers] if player.get("age") is not None]
    return RosterSnapshot(
        display_name=reference.display_name,
        season=reference.season,
        scope="season roster",
        roster_size=len(roster_ids),
        hitter_count=len(hitter_pool),
        pitcher_count=len([player for player in pitchers if (player.get("innings") or 0.0) > 0]),
        average_age=mean(age_values) if age_values else None,
        lineup_depth_ops=lineup_depth_ops,
        rotation_era=rotation_era,
        bullpen_era=bullpen_era,
        top_hitters=top_hitters,
        top_pitchers=top_pitchers,
    )


def fetch_historical_hitters(connection, season: int, team_code: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        WITH batting AS (
            SELECT
                b.playerid,
                SUM(CAST(COALESCE(b.ab, '0') AS INTEGER)) AS ab,
                SUM(CAST(COALESCE(b.h, '0') AS INTEGER)) AS h,
                SUM(CAST(COALESCE(b.c_2b, '0') AS INTEGER)) AS d2,
                SUM(CAST(COALESCE(b.c_3b, '0') AS INTEGER)) AS d3,
                SUM(CAST(COALESCE(b.hr, '0') AS INTEGER)) AS hr,
                SUM(CAST(COALESCE(b.bb, '0') AS INTEGER)) AS bb,
                SUM(CAST(COALESCE(b.hbp, '0') AS INTEGER)) AS hbp,
                SUM(CAST(COALESCE(b.sf, '0') AS INTEGER)) AS sf
            FROM lahman_batting AS b
            WHERE b.yearid = ? AND lower(b.teamid) = ?
            GROUP BY b.playerid
        )
        SELECT
            batting.*,
            p.namefirst,
            p.namelast,
            p.birthyear,
            p.birthmonth,
            p.birthday
        FROM batting
        JOIN lahman_people AS p ON p.playerid = batting.playerid
        ORDER BY batting.ab DESC
        """,
        (season, team_code),
    ).fetchall()
    hitters: list[dict[str, Any]] = []
    for row in rows:
        hitters.append(
            {
                "player_id": row["playerid"],
                "player": f"{row['namefirst']} {row['namelast']}".strip(),
                "ab": int(row["ab"] or 0),
                "ops": compute_ops(
                    int(row["h"] or 0),
                    int(row["d2"] or 0),
                    int(row["d3"] or 0),
                    int(row["hr"] or 0),
                    int(row["ab"] or 0),
                    int(row["bb"] or 0),
                    int(row["hbp"] or 0),
                    int(row["sf"] or 0),
                ),
                "age": season_age(row["birthyear"], row["birthmonth"], row["birthday"], season),
            }
        )
    return hitters


def fetch_historical_pitchers(connection, season: int, team_code: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        WITH pitching AS (
            SELECT
                pit.playerid,
                SUM(CAST(COALESCE(pit.ipouts, '0') AS INTEGER)) AS ipouts,
                SUM(CAST(COALESCE(pit.er, '0') AS INTEGER)) AS er,
                SUM(CAST(COALESCE(pit.g, '0') AS INTEGER)) AS g,
                SUM(CAST(COALESCE(pit.gs, '0') AS INTEGER)) AS gs
            FROM lahman_pitching AS pit
            WHERE pit.yearid = ? AND lower(pit.teamid) = ?
            GROUP BY pit.playerid
        )
        SELECT
            pitching.*,
            p.namefirst,
            p.namelast,
            p.birthyear,
            p.birthmonth,
            p.birthday
        FROM pitching
        JOIN lahman_people AS p ON p.playerid = pitching.playerid
        ORDER BY pitching.ipouts DESC
        """,
        (season, team_code),
    ).fetchall()
    pitchers: list[dict[str, Any]] = []
    for row in rows:
        innings = (safe_float(row["ipouts"]) or 0.0) / 3.0
        era = ((safe_float(row["er"]) or 0.0) * 9.0 / innings) if innings else None
        pitchers.append(
            {
                "player_id": row["playerid"],
                "player": f"{row['namefirst']} {row['namelast']}".strip(),
                "innings": innings,
                "era": era,
                "games_started": int(row["gs"] or 0),
                "age": season_age(row["birthyear"], row["birthmonth"], row["birthday"], season),
            }
        )
    return pitchers


def season_age(birth_year: Any, birth_month: Any, birth_day: Any, season: int) -> float | None:
    year_value = safe_int(birth_year)
    if year_value is None:
        return None
    month_value = safe_int(birth_month) or 7
    day_value = safe_int(birth_day) or 1
    midpoint = date(season, 7, 1)
    birth_date = date(max(1, year_value), max(1, min(month_value, 12)), max(1, min(day_value, 28)))
    return round((midpoint - birth_date).days / 365.25, 1)


def snapshot_row(snapshot: RosterSnapshot) -> dict[str, Any]:
    top_hitter = snapshot.top_hitters[0] if snapshot.top_hitters else {}
    top_pitcher = snapshot.top_pitchers[0] if snapshot.top_pitchers else {}
    return {
        "team": snapshot.display_name,
        "scope": snapshot.scope,
        "roster_size": snapshot.roster_size,
        "hitters": snapshot.hitter_count,
        "pitchers": snapshot.pitcher_count,
        "avg_age": format_float(snapshot.average_age, 1),
        "lineup_depth_ops": format_float(snapshot.lineup_depth_ops, 3),
        "rotation_era": format_float(snapshot.rotation_era, 2),
        "bullpen_era": format_float(snapshot.bullpen_era, 2),
        "top_hitter": top_hitter.get("player", ""),
        "top_hitter_ops": format_float(top_hitter.get("ops"), 3),
        "top_arm": top_pitcher.get("player", ""),
        "top_arm_era": format_float(top_pitcher.get("era"), 2),
    }


def build_roster_comparison_summary(left: RosterSnapshot, right: RosterSnapshot) -> str:
    offense_edge = better_snapshot(left, right, "lineup_depth_ops", higher_is_better=True)
    rotation_edge = better_snapshot(left, right, "rotation_era", higher_is_better=False)
    bullpen_edge = better_snapshot(left, right, "bullpen_era", higher_is_better=False)

    left_score = sum(edge == "left" for edge in (offense_edge, rotation_edge, bullpen_edge))
    right_score = sum(edge == "right" for edge in (offense_edge, rotation_edge, bullpen_edge))
    if left_score > right_score:
        verdict = f"{left.display_name} look stronger on overall roster shape."
    elif right_score > left_score:
        verdict = f"{right.display_name} look stronger on overall roster shape."
    else:
        verdict = "The two rosters come out fairly even on the public comparison points available here."

    summary = (
        f"{verdict} Lineup depth leans to {edge_label(offense_edge, left, right)} "
        f"({format_float(left.lineup_depth_ops, 3)} vs {format_float(right.lineup_depth_ops, 3)} top-six OPS). "
        f"Rotation quality leans to {edge_label(rotation_edge, left, right)} "
        f"({format_float(left.rotation_era, 2)} vs {format_float(right.rotation_era, 2)} ERA). "
        f"Bullpen quality leans to {edge_label(bullpen_edge, left, right)} "
        f"({format_float(left.bullpen_era, 2)} vs {format_float(right.bullpen_era, 2)} ERA)."
    )
    summary = (
        f"{summary} {left.display_name} carry {left.roster_size} tracked players at an average age of "
        f"{format_float(left.average_age, 1)}; {right.display_name} sit at {right.roster_size} players and "
        f"{format_float(right.average_age, 1)} years."
    )
    left_bats = format_player_list(left.top_hitters, "ops")
    right_bats = format_player_list(right.top_hitters, "ops")
    left_arms = format_player_list(left.top_pitchers, "era")
    right_arms = format_player_list(right.top_pitchers, "era")
    if left_bats and right_bats:
        summary = f"{summary} Best bats: {left.display_name}: {left_bats}. {right.display_name}: {right_bats}."
    if left_arms and right_arms:
        summary = f"{summary} Main arms: {left.display_name}: {left_arms}. {right.display_name}: {right_arms}."
    if "active roster" in {left.scope, right.scope}:
        summary = (
            f"{summary} The current-season side is using the active roster and season-to-date production, "
            "so treat it as an early roster read rather than a final full-season judgment."
        )
    return summary


def better_snapshot(left: RosterSnapshot, right: RosterSnapshot, attribute: str, *, higher_is_better: bool) -> str:
    left_value = getattr(left, attribute)
    right_value = getattr(right, attribute)
    if left_value is None or right_value is None:
        return "neither side clearly"
    if abs(left_value - right_value) < 1e-9:
        return "neither side clearly"
    if higher_is_better:
        return "left" if left_value > right_value else "right"
    return "left" if left_value < right_value else "right"


def edge_label(edge: str, left: RosterSnapshot, right: RosterSnapshot) -> str:
    if edge == "left":
        return left.display_name
    if edge == "right":
        return right.display_name
    return "neither side clearly"


def format_player_list(players: list[dict[str, Any]], metric_key: str) -> str:
    items = []
    for player in players[:3]:
        metric_label = format_float(player.get(metric_key), 3 if metric_key == "ops" else 2)
        items.append(f"{player['player']} ({metric_label})")
    return "; ".join(items)
