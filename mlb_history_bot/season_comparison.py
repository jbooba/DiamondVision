from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .storage import table_exists
from .team_evaluator import (
    TeamIdentity,
    find_team_split,
    format_float,
    infer_focus,
    per_game_value,
    resolve_team_from_question,
    safe_float,
    safe_int,
)


LAST_YEAR_HINTS = {
    "compared to last year",
    "compare to last year",
    "than last year",
    "vs last year",
    "versus last year",
    "relative to last year",
    "compared to last season",
    "compare to last season",
    "than last season",
    "vs last season",
    "versus last season",
    "relative to last season",
}


@dataclass(slots=True)
class SeasonComparisonQuery:
    team: TeamIdentity
    current_season: int
    previous_season: int
    focus: str


class SeasonComparisonResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_season_comparison_query(question, self.live_client, self.settings.live_season or date.today().year)
        if query is None:
            return None

        current = self._team_snapshot(query.team, query.current_season)
        previous = self._team_snapshot(query.team, query.previous_season)
        if current is None or previous is None:
            return None

        same_point = self._same_point_previous_season(connection, query.team, query.previous_season, current["games_played"])
        summary = build_season_comparison_summary(query, current=current, previous=previous, same_point=same_point)
        return EvidenceSnippet(
            source="Season Comparison",
            title=f"{query.team.name} {query.current_season} vs {query.previous_season}",
            citation="MLB Stats API team season stats plus Retrosheet same-point team game logs when available",
            summary=summary,
            payload={
                "analysis_type": "season_comparison",
                "mode": "hybrid",
                "team": {"id": query.team.team_id, "name": query.team.name, "abbreviation": query.team.abbreviation},
                "current_season": query.current_season,
                "previous_season": query.previous_season,
                "focus": query.focus,
                "current": current,
                "previous": previous,
                "same_point_previous": same_point,
                "clips": [],
            },
        )

    def _team_snapshot(self, team: TeamIdentity, season: int) -> dict[str, Any] | None:
        hitting = find_team_split(self.live_client.all_team_group_stats("hitting", season), team.team_id)
        pitching = find_team_split(self.live_client.all_team_group_stats("pitching", season), team.team_id)
        fielding = find_team_split(self.live_client.all_team_group_stats("fielding", season), team.team_id)
        standings_row = self._find_standings_row(self.live_client.standings(season).get("standings", []), team)
        if hitting is None or pitching is None or fielding is None:
            return None
        games_played = safe_int(hitting["stat"].get("gamesPlayed")) or 0
        wins = safe_int(standings_row.get("wins")) if standings_row else None
        losses = safe_int(standings_row.get("losses")) if standings_row else None
        return {
            "season": season,
            "games_played": games_played,
            "wins": wins,
            "losses": losses,
            "win_pct": safe_float(standings_row.get("pct")) if standings_row else None,
            "runs_per_game": per_game_value(hitting["stat"].get("runs"), hitting["stat"].get("gamesPlayed")),
            "ops": safe_float(hitting["stat"].get("ops")),
            "home_runs_per_game": per_game_value(hitting["stat"].get("homeRuns"), hitting["stat"].get("gamesPlayed")),
            "era": safe_float(pitching["stat"].get("era")),
            "whip": safe_float(pitching["stat"].get("whip")),
            "runs_allowed_per_game": per_game_value(pitching["stat"].get("runs"), pitching["stat"].get("gamesPlayed")),
            "fielding": safe_float(fielding["stat"].get("fielding")),
            "errors_per_game": per_game_value(fielding["stat"].get("errors"), fielding["stat"].get("gamesPlayed")),
        }

    def _find_standings_row(self, rows: list[dict[str, Any]], team: TeamIdentity) -> dict[str, Any] | None:
        for row in rows:
            team_label = str(row.get("team") or "").strip().lower()
            if team_label in {team.name.lower(), team.short_name.lower(), team.club_name.lower(), team.franchise_name.lower()}:
                return row
        return None

    def _same_point_previous_season(
        self,
        connection,
        team: TeamIdentity,
        season: int,
        games_played: int,
    ) -> dict[str, Any] | None:
        if games_played <= 0 or not table_exists(connection, "retrosheet_teamstats") or not table_exists(connection, "lahman_teams"):
            return None
        row = connection.execute(
            """
            SELECT teamidretro
            FROM lahman_teams
            WHERE CAST(yearid AS INTEGER) = ?
              AND (lower(name) = ? OR lower(name) LIKE ?)
            LIMIT 1
            """,
            (season, team.name.lower(), f"%{team.club_name.lower()}%"),
        ).fetchone()
        if row is None or not row["teamidretro"]:
            return None
        rows = connection.execute(
            """
            SELECT b_r, b_hr, p_r, p_er, p_ipouts, d_e, win, loss
            FROM retrosheet_teamstats
            WHERE stattype = 'value'
              AND team = ?
              AND substr(date, 1, 4) = ?
              AND gametype = 'regular'
            ORDER BY date, CAST(number AS INTEGER), gid
            LIMIT ?
            """,
            (row["teamidretro"], str(season), games_played),
        ).fetchall()
        if not rows:
            return None
        wins = sum(int(candidate["win"] or 0) for candidate in rows)
        losses = sum(int(candidate["loss"] or 0) for candidate in rows)
        runs = sum(int(candidate["b_r"] or 0) for candidate in rows)
        runs_allowed = sum(int(candidate["p_r"] or 0) for candidate in rows)
        home_runs = sum(int(candidate["b_hr"] or 0) for candidate in rows)
        errors = sum(int(candidate["d_e"] or 0) for candidate in rows)
        earned_runs = sum(int(candidate["p_er"] or 0) for candidate in rows)
        outs = sum(int(candidate["p_ipouts"] or 0) for candidate in rows)
        return {
            "season": season,
            "games_played": len(rows),
            "wins": wins,
            "losses": losses,
            "win_pct": wins / len(rows) if rows else None,
            "runs_per_game": runs / len(rows),
            "home_runs_per_game": home_runs / len(rows),
            "runs_allowed_per_game": runs_allowed / len(rows),
            "era": (27.0 * earned_runs / outs) if outs else None,
            "errors_per_game": errors / len(rows),
        }


def parse_season_comparison_query(
    question: str,
    live_client: LiveStatsClient,
    current_season: int,
) -> SeasonComparisonQuery | None:
    lowered = question.lower()
    if not any(hint in lowered for hint in LAST_YEAR_HINTS):
        return None
    team = resolve_team_from_question(question, live_client.teams(current_season))
    if team is None:
        return None
    return SeasonComparisonQuery(
        team=team,
        current_season=current_season,
        previous_season=current_season - 1,
        focus=infer_focus(question),
    )


def build_season_comparison_summary(
    query: SeasonComparisonQuery,
    *,
    current: dict[str, Any],
    previous: dict[str, Any],
    same_point: dict[str, Any] | None,
) -> str:
    current_record = f"{current['wins']}-{current['losses']}" if current.get("wins") is not None else "unknown"
    previous_record = f"{previous['wins']}-{previous['losses']}" if previous.get("wins") is not None else "unknown"
    summary = (
        f"Through {current['games_played']} game(s), {query.team.name} are {current_record} in {query.current_season}, "
        f"compared with {previous_record} over the full {query.previous_season} season."
    )
    if same_point is not None:
        summary = (
            f"Through {current['games_played']} game(s), {query.team.name} are {current_record} in {query.current_season}. "
            f"At the same point in {query.previous_season}, they were {same_point['wins']}-{same_point['losses']}."
        )
    summary = (
        f"{summary} Offensively they are at {format_float(current['runs_per_game'], 2)} runs per game and "
        f"{format_float(current['ops'], 3)} OPS, versus {format_float(previous['runs_per_game'], 2)} and "
        f"{format_float(previous['ops'], 3)} last year."
    )
    summary = (
        f"{summary} Their pitching line is {format_float(current['era'], 2)} ERA and "
        f"{format_float(current['runs_allowed_per_game'], 2)} runs allowed per game, versus "
        f"{format_float(previous['era'], 2)} and {format_float(previous['runs_allowed_per_game'], 2)} last year."
    )
    if same_point is not None:
        summary = (
            f"{summary} Against the same-point {query.previous_season} start, the biggest swing is "
            f"{describe_swing(current, same_point)}."
        )
    return summary


def describe_swing(current: dict[str, Any], previous: dict[str, Any]) -> str:
    offense_delta = current["runs_per_game"] - previous["runs_per_game"]
    prevention_delta = previous["runs_allowed_per_game"] - current["runs_allowed_per_game"]
    if abs(offense_delta) >= abs(prevention_delta):
        direction = "up" if offense_delta >= 0 else "down"
        return f"offense, with runs per game {direction} by {abs(offense_delta):.2f}"
    direction = "better" if prevention_delta >= 0 else "worse"
    return f"run prevention, with runs allowed per game {direction} by {abs(prevention_delta):.2f}"
