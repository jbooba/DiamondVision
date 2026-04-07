from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .storage import table_exists
from .team_evaluator import format_float, safe_float
from .team_season_compare import (
    YEAR_TEAM_PATTERN,
    clean_team_phrase,
    resolve_team_season_reference,
)


PITCHING_COMPARISON_HINTS = ("pitching roster", "pitching rosters", "pitching staff", "rotation", "bullpen")


@dataclass(slots=True)
class PitchingStaffSnapshot:
    display_name: str
    era: float | None
    whip: float | None
    strikeouts_per_9: float | None
    walks_per_9: float | None
    innings: float | None
    top_pitchers: list[dict[str, Any]]


class PitchingStaffComparisonResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        lowered = question.lower()
        if "compare" not in lowered or not any(hint in lowered for hint in PITCHING_COMPARISON_HINTS):
            return None
        current_season = self.settings.live_season or date.today().year
        matches = list(YEAR_TEAM_PATTERN.finditer(question))
        if len(matches) < 2:
            return None
        left = resolve_team_season_reference(
            connection,
            clean_team_phrase(matches[0].group(2)),
            int(matches[0].group(1)),
            self.live_client,
            current_season,
        )
        right = resolve_team_season_reference(
            connection,
            clean_team_phrase(matches[1].group(2)),
            int(matches[1].group(1)),
            self.live_client,
            current_season,
        )
        if left is None or right is None:
            return None

        left_snapshot = self._build_snapshot(connection, left)
        right_snapshot = self._build_snapshot(connection, right)
        if left_snapshot is None or right_snapshot is None:
            return None

        better = choose_better_staff(left_snapshot, right_snapshot)
        summary = (
            f"{left_snapshot.display_name} carried a {format_float(left_snapshot.era, 2)} ERA, "
            f"{format_float(left_snapshot.whip, 2)} WHIP, and {format_float(left_snapshot.strikeouts_per_9, 1)} K/9 "
            f"over {format_float(left_snapshot.innings, 1)} innings. "
            f"{right_snapshot.display_name} are at {format_float(right_snapshot.era, 2)} ERA, "
            f"{format_float(right_snapshot.whip, 2)} WHIP, and {format_float(right_snapshot.strikeouts_per_9, 1)} K/9 "
            f"over {format_float(right_snapshot.innings, 1)} innings. "
            f"The stronger run-prevention profile belongs to {better}. "
            f"Top arms: {describe_top_pitchers(left_snapshot)} | {describe_top_pitchers(right_snapshot)}."
        )
        return EvidenceSnippet(
            source="Pitching Staff Comparison",
            title=f"{left.display_name} vs {right.display_name} pitching staffs",
            citation="Lahman Pitching and MLB Stats API current roster pitching lines",
            summary=summary,
            payload={
                "analysis_type": "pitching_staff_comparison",
                "mode": "hybrid" if left.live_team is not None or right.live_team is not None else "historical",
                "rows": [
                    overview_row(left_snapshot),
                    overview_row(right_snapshot),
                ],
            },
        )

    def _build_snapshot(self, connection, reference) -> PitchingStaffSnapshot | None:
        current_season = self.settings.live_season or date.today().year
        if reference.live_team is not None and reference.season == current_season:
            return self._build_live_snapshot(reference)
        return self._build_historical_snapshot(connection, reference)

    def _build_live_snapshot(self, reference) -> PitchingStaffSnapshot | None:
        roster = self.live_client.team_roster(reference.live_team.team_id, season=reference.season)
        people = self.live_client.people_with_stats(
            [entry.get("person", {}).get("id") for entry in roster],
            season=reference.season,
            groups=("pitching",),
        )
        pitchers: list[dict[str, Any]] = []
        innings_total = 0.0
        earned_runs_total = 0.0
        walks_total = 0.0
        hits_total = 0.0
        strikeouts_total = 0.0
        for person in people:
            stat = first_group_stat(person, "pitching")
            innings = innings_text_to_float(stat.get("inningsPitched"))
            if innings <= 0:
                continue
            pitchers.append(
                {
                    "name": str(person.get("fullName") or ""),
                    "innings": innings,
                    "era": safe_float(stat.get("era")),
                    "strikeouts": safe_float(stat.get("strikeOuts")) or 0.0,
                }
            )
            innings_total += innings
            earned_runs_total += safe_float(stat.get("earnedRuns")) or 0.0
            walks_total += safe_float(stat.get("baseOnBalls")) or 0.0
            hits_total += safe_float(stat.get("hits")) or 0.0
            strikeouts_total += safe_float(stat.get("strikeOuts")) or 0.0
        if not pitchers:
            return None
        pitchers.sort(key=lambda pitcher: (-(pitcher["innings"] or 0.0), pitcher["era"] if pitcher["era"] is not None else 999.0))
        return PitchingStaffSnapshot(
            display_name=reference.display_name,
            era=((earned_runs_total * 9.0) / innings_total) if innings_total else None,
            whip=((walks_total + hits_total) / innings_total) if innings_total else None,
            strikeouts_per_9=((strikeouts_total * 9.0) / innings_total) if innings_total else None,
            walks_per_9=((walks_total * 9.0) / innings_total) if innings_total else None,
            innings=innings_total,
            top_pitchers=pitchers[:4],
        )

    def _build_historical_snapshot(self, connection, reference) -> PitchingStaffSnapshot | None:
        if not table_exists(connection, "lahman_pitching") or not table_exists(connection, "lahman_teams"):
            return None
        team_row = connection.execute(
            """
            SELECT teamid
            FROM lahman_teams
            WHERE yearid = ?
              AND (
                lower(name) = ?
                OR lower(teamidretro) = ?
              )
            LIMIT 1
            """,
            (
                reference.season,
                reference.display_name.split(" ", 1)[1].lower(),
                (reference.team_code or "").lower(),
            ),
        ).fetchone()
        if team_row is None:
            return None
        team_id = str(team_row["teamid"] or "")
        rows = connection.execute(
            """
            SELECT playerid, ipouts, er, h, bb, so, era
            FROM lahman_pitching
            WHERE yearid = ? AND lower(teamid) = ?
            """,
            (reference.season, team_id.lower()),
        ).fetchall()
        if not rows:
            return None
        people = {
            row["playerid"]: f"{row['namefirst']} {row['namelast']}".strip()
            for row in connection.execute(
                """
                SELECT playerid, namefirst, namelast
                FROM lahman_people
                WHERE playerid IN (
                    SELECT playerid
                    FROM lahman_pitching
                    WHERE yearid = ? AND lower(teamid) = ?
                )
                """,
                (reference.season, team_id.lower()),
            ).fetchall()
        }
        innings_total = sum((safe_float(row["ipouts"]) or 0.0) / 3.0 for row in rows)
        earned_runs_total = sum(safe_float(row["er"]) or 0.0 for row in rows)
        hits_total = sum(safe_float(row["h"]) or 0.0 for row in rows)
        walks_total = sum(safe_float(row["bb"]) or 0.0 for row in rows)
        strikeouts_total = sum(safe_float(row["so"]) or 0.0 for row in rows)
        pitchers = [
            {
                "name": people.get(str(row["playerid"]), str(row["playerid"])),
                "innings": (safe_float(row["ipouts"]) or 0.0) / 3.0,
                "era": safe_float(row["era"]),
                "strikeouts": safe_float(row["so"]) or 0.0,
            }
            for row in rows
            if (safe_float(row["ipouts"]) or 0.0) > 0
        ]
        pitchers.sort(key=lambda pitcher: (-(pitcher["innings"] or 0.0), pitcher["era"] if pitcher["era"] is not None else 999.0))
        return PitchingStaffSnapshot(
            display_name=reference.display_name,
            era=((earned_runs_total * 9.0) / innings_total) if innings_total else None,
            whip=((walks_total + hits_total) / innings_total) if innings_total else None,
            strikeouts_per_9=((strikeouts_total * 9.0) / innings_total) if innings_total else None,
            walks_per_9=((walks_total * 9.0) / innings_total) if innings_total else None,
            innings=innings_total,
            top_pitchers=pitchers[:4],
        )


def first_group_stat(person: dict[str, Any], group_name: str) -> dict[str, Any]:
    for stats_group in person.get("stats", []):
        if str(stats_group.get("group", {}).get("displayName") or "").lower() != group_name:
            continue
        splits = stats_group.get("splits") or []
        if splits:
            return splits[0].get("stat", {})
    return {}


def innings_text_to_float(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    if "." not in text:
        return safe_float(text) or 0.0
    whole, fraction = text.split(".", 1)
    return (safe_float(whole) or 0.0) + ((safe_float(fraction[:1]) or 0.0) / 3.0)


def choose_better_staff(left: PitchingStaffSnapshot, right: PitchingStaffSnapshot) -> str:
    left_score = staff_score(left)
    right_score = staff_score(right)
    return left.display_name if left_score <= right_score else right.display_name


def staff_score(snapshot: PitchingStaffSnapshot) -> float:
    return (snapshot.era or 99.0) + (snapshot.whip or 9.0) + ((9.5 - (snapshot.strikeouts_per_9 or 0.0)) / 4.0)


def describe_top_pitchers(snapshot: PitchingStaffSnapshot) -> str:
    parts = [
        f"{pitcher['name']} ({format_float(pitcher['era'], 2)} ERA, {format_float(pitcher['innings'], 1)} IP)"
        for pitcher in snapshot.top_pitchers[:3]
    ]
    return f"{snapshot.display_name}: {'; '.join(parts)}"


def overview_row(snapshot: PitchingStaffSnapshot) -> dict[str, Any]:
    return {
        "team": snapshot.display_name,
        "innings": format_float(snapshot.innings, 1),
        "era": format_float(snapshot.era, 2),
        "whip": format_float(snapshot.whip, 2),
        "strikeouts_per_9": format_float(snapshot.strikeouts_per_9, 1),
        "walks_per_9": format_float(snapshot.walks_per_9, 1),
    }
