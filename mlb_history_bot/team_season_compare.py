from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .query_utils import extract_first_n_games
from .storage import table_exists
from .team_evaluator import (
    TeamIdentity,
    build_team_aliases,
    find_team_split,
    format_float,
    normalize_team_identity,
    safe_float,
    safe_int,
)


COMPARISON_HINTS = {"compare", "vs", "versus", "worse than", "better than", "matched up against", "to the"}
YEAR_TEAM_PATTERN = re.compile(
    r"\b(18\d{2}|19\d{2}|20\d{2})\s+([A-Za-z][A-Za-z .'-]{1,60}?)(?=(?:\s+(?:to|vs|versus|and|than|through|over|across|in|of|their|seasons?|season)\b|[?.!,]|$))",
    re.IGNORECASE,
)


@dataclass(slots=True)
class TeamSeasonReference:
    season: int
    phrase: str
    display_name: str
    team_code: str | None
    live_team: TeamIdentity | None


@dataclass(slots=True)
class TeamSeasonComparisonQuery:
    left: TeamSeasonReference
    right: TeamSeasonReference
    first_n_games: int | None
    comparator: str


@dataclass(slots=True)
class TeamSeasonSnapshot:
    display_name: str
    season: int
    scope_label: str
    games_played: int
    wins: int | None
    losses: int | None
    win_pct: float | None
    runs_per_game: float | None
    runs_allowed_per_game: float | None
    run_diff_per_game: float | None
    home_runs_per_game: float | None
    ops: float | None
    era: float | None
    fielding_pct: float | None
    exact_window: bool

    @property
    def record(self) -> str:
        if self.wins is None or self.losses is None:
            return "unknown"
        return f"{self.wins}-{self.losses}"


class TeamSeasonComparisonResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        current_season = self.settings.live_season or date.today().year
        query = parse_team_season_comparison_query(connection, question, self.live_client, current_season)
        if query is None:
            return None

        left_snapshot = self._build_snapshot(connection, query.left, query.first_n_games, current_season)
        right_snapshot = self._build_snapshot(connection, query.right, query.first_n_games, current_season)
        if left_snapshot is None or right_snapshot is None:
            return None

        mode = "hybrid" if query.left.live_team is not None or query.right.live_team is not None else "historical"
        summary = build_team_season_comparison_summary(query, left_snapshot, right_snapshot)
        return EvidenceSnippet(
            source="Team Season Comparison",
            title=f"{query.left.display_name} vs {query.right.display_name}",
            citation="Lahman and Retrosheet team history plus MLB Stats API team game logs for current seasons",
            summary=summary,
            payload={
                "analysis_type": "team_season_comparison",
                "mode": mode,
                "first_n_games": query.first_n_games,
                "comparator": query.comparator,
                "rows": [
                    snapshot_row(left_snapshot),
                    snapshot_row(right_snapshot),
                ],
            },
        )

    def _build_snapshot(
        self,
        connection,
        reference: TeamSeasonReference,
        first_n_games: int | None,
        current_season: int,
    ) -> TeamSeasonSnapshot | None:
        if reference.live_team is not None and reference.season == current_season:
            if first_n_games is not None:
                return self._live_window_snapshot(reference.live_team, reference.season, first_n_games)
            return self._live_season_to_date_snapshot(reference.live_team, reference.season)
        if first_n_games is not None:
            return self._historical_window_snapshot(connection, reference, first_n_games)
        return self._historical_full_snapshot(connection, reference)

    def _live_window_snapshot(self, team: TeamIdentity, season: int, first_n_games: int) -> TeamSeasonSnapshot | None:
        hitting_logs = self.live_client.team_game_logs(team.team_id, "hitting", season)
        pitching_logs = self.live_client.team_game_logs(team.team_id, "pitching", season)
        fielding_logs = self.live_client.team_game_logs(team.team_id, "fielding", season)
        if not hitting_logs or not pitching_logs or not fielding_logs:
            return None

        hitting_rows = sorted(hitting_logs, key=game_log_sort_key)[:first_n_games]
        pitching_rows = sorted(pitching_logs, key=game_log_sort_key)[:first_n_games]
        fielding_rows = sorted(fielding_logs, key=game_log_sort_key)[:first_n_games]
        exact_window = len(hitting_rows) >= first_n_games and len(pitching_rows) >= first_n_games and len(fielding_rows) >= first_n_games
        if not hitting_rows or not pitching_rows:
            return None

        games_played = min(len(hitting_rows), len(pitching_rows), len(fielding_rows))
        hitting_rows = hitting_rows[:games_played]
        pitching_rows = pitching_rows[:games_played]
        fielding_rows = fielding_rows[:games_played]

        wins = sum(1 for row in hitting_rows if bool(row.get("isWin")))
        losses = sum(1 for row in hitting_rows if row.get("isWin") is False)
        runs = sum(safe_int(row.get("stat", {}).get("runs")) or 0 for row in hitting_rows)
        home_runs = sum(safe_int(row.get("stat", {}).get("homeRuns")) or 0 for row in hitting_rows)
        hits = sum(safe_int(row.get("stat", {}).get("hits")) or 0 for row in hitting_rows)
        at_bats = sum(safe_int(row.get("stat", {}).get("atBats")) or 0 for row in hitting_rows)
        walks = sum(safe_int(row.get("stat", {}).get("baseOnBalls")) or 0 for row in hitting_rows)
        hbp = sum(safe_int(row.get("stat", {}).get("hitByPitch")) or 0 for row in hitting_rows)
        sacrifice_flies = sum(safe_int(row.get("stat", {}).get("sacFlies")) or 0 for row in hitting_rows)
        doubles = sum(safe_int(row.get("stat", {}).get("doubles")) or 0 for row in hitting_rows)
        triples = sum(safe_int(row.get("stat", {}).get("triples")) or 0 for row in hitting_rows)
        runs_allowed = sum(safe_int(row.get("stat", {}).get("runs")) or 0 for row in pitching_rows)
        earned_runs = sum(safe_int(row.get("stat", {}).get("earnedRuns")) or 0 for row in pitching_rows)
        pitching_hits = sum(safe_int(row.get("stat", {}).get("hits")) or 0 for row in pitching_rows)
        pitching_walks = sum(safe_int(row.get("stat", {}).get("baseOnBalls")) or 0 for row in pitching_rows)
        innings_outs = sum(parse_innings_to_outs(row.get("stat", {}).get("inningsPitched")) for row in pitching_rows)
        chances = sum(safe_int(row.get("stat", {}).get("chances")) or 0 for row in fielding_rows)
        errors = sum(safe_int(row.get("stat", {}).get("errors")) or 0 for row in fielding_rows)

        return TeamSeasonSnapshot(
            display_name=f"{season} {team.name}",
            season=season,
            scope_label=f"first {first_n_games} games" if exact_window else f"first {games_played} of requested {first_n_games} games",
            games_played=games_played,
            wins=wins,
            losses=losses,
            win_pct=(wins / games_played) if games_played else None,
            runs_per_game=(runs / games_played) if games_played else None,
            runs_allowed_per_game=(runs_allowed / games_played) if games_played else None,
            run_diff_per_game=((runs - runs_allowed) / games_played) if games_played else None,
            home_runs_per_game=(home_runs / games_played) if games_played else None,
            ops=compute_ops(hits, doubles, triples, home_runs, at_bats, walks, hbp, sacrifice_flies),
            era=(27.0 * earned_runs / innings_outs) if innings_outs else None,
            fielding_pct=((chances - errors) / chances) if chances else None,
            exact_window=exact_window,
        )

    def _live_season_to_date_snapshot(self, team: TeamIdentity, season: int) -> TeamSeasonSnapshot | None:
        hitting = find_team_split(self.live_client.all_team_group_stats("hitting", season), team.team_id)
        pitching = find_team_split(self.live_client.all_team_group_stats("pitching", season), team.team_id)
        fielding = find_team_split(self.live_client.all_team_group_stats("fielding", season), team.team_id)
        standings_rows = self.live_client.standings(season).get("standings", [])
        aliases = {
            team.name.lower(),
            team.short_name.lower(),
            team.club_name.lower(),
            team.franchise_name.lower(),
            f"{team.location_name} {team.club_name}".strip().lower(),
        }
        standings_row = next((row for row in standings_rows if str(row.get("team") or "").strip().lower() in aliases), None)
        if hitting is None or pitching is None or fielding is None:
            return None
        games_played = safe_int(hitting["stat"].get("gamesPlayed")) or 0
        wins = safe_int(standings_row.get("wins")) if standings_row else None
        losses = safe_int(standings_row.get("losses")) if standings_row else None
        win_pct = safe_float(standings_row.get("pct")) if standings_row else (wins / games_played if wins is not None and games_played else None)
        runs = safe_int(hitting["stat"].get("runs")) or 0
        runs_allowed = safe_int(pitching["stat"].get("runs")) or 0
        return TeamSeasonSnapshot(
            display_name=f"{season} {team.name}",
            season=season,
            scope_label="season to date",
            games_played=games_played,
            wins=wins,
            losses=losses,
            win_pct=win_pct,
            runs_per_game=(runs / games_played) if games_played else None,
            runs_allowed_per_game=(runs_allowed / games_played) if games_played else None,
            run_diff_per_game=((runs - runs_allowed) / games_played) if games_played else None,
            home_runs_per_game=((safe_int(hitting["stat"].get("homeRuns")) or 0) / games_played) if games_played else None,
            ops=safe_float(hitting["stat"].get("ops")),
            era=safe_float(pitching["stat"].get("era")),
            fielding_pct=safe_float(fielding["stat"].get("fielding")),
            exact_window=True,
        )

    def _historical_window_snapshot(self, connection, reference: TeamSeasonReference, first_n_games: int) -> TeamSeasonSnapshot | None:
        if not reference.team_code or not table_exists(connection, "retrosheet_teamstats"):
            return None
        row = connection.execute(
            """
            WITH ordered_games AS (
                SELECT
                    gid,
                    date,
                    CAST(COALESCE(b_r, '0') AS INTEGER) AS b_r,
                    CAST(COALESCE(b_hr, '0') AS INTEGER) AS b_hr,
                    CAST(COALESCE(b_ab, '0') AS INTEGER) AS b_ab,
                    CAST(COALESCE(b_h, '0') AS INTEGER) AS b_h,
                    CAST(COALESCE(b_d, '0') AS INTEGER) AS b_2b,
                    CAST(COALESCE(b_t, '0') AS INTEGER) AS b_3b,
                    CAST(COALESCE(b_w, '0') AS INTEGER) AS b_bb,
                    CAST(COALESCE(b_hbp, '0') AS INTEGER) AS b_hbp,
                    CAST(COALESCE(b_sf, '0') AS INTEGER) AS b_sf,
                    CAST(COALESCE(p_r, '0') AS INTEGER) AS p_r,
                    CAST(COALESCE(p_er, '0') AS INTEGER) AS p_er,
                    CAST(COALESCE(p_h, '0') AS INTEGER) AS p_h,
                    CAST(COALESCE(p_w, '0') AS INTEGER) AS p_bb,
                    CAST(COALESCE(p_ipouts, '0') AS INTEGER) AS p_ipouts,
                    CAST(COALESCE(d_po, '0') AS INTEGER) AS d_po,
                    CAST(COALESCE(d_a, '0') AS INTEGER) AS d_a,
                    CAST(COALESCE(d_e, '0') AS INTEGER) AS d_e,
                    CAST(COALESCE(win, '0') AS INTEGER) AS win,
                    CAST(COALESCE(loss, '0') AS INTEGER) AS loss,
                    ROW_NUMBER() OVER (
                        ORDER BY date, CAST(COALESCE(number, '0') AS INTEGER), gid
                    ) AS game_number
                FROM retrosheet_teamstats
                WHERE stattype = 'value'
                  AND gametype = 'regular'
                  AND team = ?
                  AND substr(date, 1, 4) = ?
            )
            SELECT
                COUNT(*) AS games_played,
                SUM(b_r) AS runs,
                SUM(b_hr) AS home_runs,
                SUM(b_ab) AS at_bats,
                SUM(b_h) AS hits,
                SUM(b_2b) AS doubles,
                SUM(b_3b) AS triples,
                SUM(b_bb) AS walks,
                SUM(b_hbp) AS hbp,
                SUM(b_sf) AS sf,
                SUM(p_r) AS runs_allowed,
                SUM(p_er) AS earned_runs,
                SUM(p_h) AS pitching_hits,
                SUM(p_bb) AS pitching_walks,
                SUM(p_ipouts) AS ipouts,
                SUM(d_po) AS putouts,
                SUM(d_a) AS assists,
                SUM(d_e) AS errors,
                SUM(win) AS wins,
                SUM(loss) AS losses
            FROM ordered_games
            WHERE game_number <= ?
            """,
            (reference.team_code, str(reference.season), first_n_games),
        ).fetchone()
        if row is None:
            return None
        games_played = int(row["games_played"] or 0)
        if games_played == 0:
            return None
        exact_window = games_played >= first_n_games
        chances = int(row["putouts"] or 0) + int(row["assists"] or 0) + int(row["errors"] or 0)
        wins = int(row["wins"] or 0)
        losses = int(row["losses"] or 0)
        return TeamSeasonSnapshot(
            display_name=reference.display_name,
            season=reference.season,
            scope_label=f"first {first_n_games} games" if exact_window else f"first {games_played} of requested {first_n_games} games",
            games_played=games_played,
            wins=wins,
            losses=losses,
            win_pct=(wins / games_played) if games_played else None,
            runs_per_game=(int(row["runs"] or 0) / games_played) if games_played else None,
            runs_allowed_per_game=(int(row["runs_allowed"] or 0) / games_played) if games_played else None,
            run_diff_per_game=((int(row["runs"] or 0) - int(row["runs_allowed"] or 0)) / games_played) if games_played else None,
            home_runs_per_game=(int(row["home_runs"] or 0) / games_played) if games_played else None,
            ops=compute_ops(
                int(row["hits"] or 0),
                int(row["doubles"] or 0),
                int(row["triples"] or 0),
                int(row["home_runs"] or 0),
                int(row["at_bats"] or 0),
                int(row["walks"] or 0),
                int(row["hbp"] or 0),
                int(row["sf"] or 0),
            ),
            era=(27.0 * int(row["earned_runs"] or 0) / int(row["ipouts"] or 0)) if int(row["ipouts"] or 0) else None,
            fielding_pct=((chances - int(row["errors"] or 0)) / chances) if chances else None,
            exact_window=exact_window,
        )

    def _historical_full_snapshot(self, connection, reference: TeamSeasonReference) -> TeamSeasonSnapshot | None:
        if not table_exists(connection, "lahman_teams"):
            return None
        row = connection.execute(
            """
            SELECT *
            FROM lahman_teams
            WHERE yearid = ?
              AND lower(name) = ?
            LIMIT 1
            """,
            (reference.season, reference.display_name.split(" ", 1)[1].lower()),
        ).fetchone()
        if row is None and reference.team_code:
            row = connection.execute(
                """
                SELECT *
                FROM lahman_teams
                WHERE yearid = ?
                  AND lower(teamidretro) = ?
                LIMIT 1
                """,
                (reference.season, reference.team_code.lower()),
            ).fetchone()
        if row is None:
            return None
        games_played = int(row["g"] or 0)
        wins = int(row["w"] or 0)
        losses = int(row["l"] or 0)
        return TeamSeasonSnapshot(
            display_name=reference.display_name,
            season=reference.season,
            scope_label="full season",
            games_played=games_played,
            wins=wins,
            losses=losses,
            win_pct=(wins / games_played) if games_played else None,
            runs_per_game=(int(row["r"] or 0) / games_played) if games_played else None,
            runs_allowed_per_game=(int(row["ra"] or 0) / games_played) if games_played else None,
            run_diff_per_game=((int(row["r"] or 0) - int(row["ra"] or 0)) / games_played) if games_played else None,
            home_runs_per_game=(int(row["hr"] or 0) / games_played) if games_played else None,
            ops=compute_ops(
                int(row["h"] or 0),
                int(row["c_2b"] or 0),
                int(row["c_3b"] or 0),
                int(row["hr"] or 0),
                int(row["ab"] or 0),
                int(row["bb"] or 0),
                int(row["hbp"] or 0),
                int(row["sf"] or 0),
            ),
            era=safe_float(row["era"]),
            fielding_pct=safe_float(row["fp"]),
            exact_window=True,
        )


def parse_team_season_comparison_query(
    connection,
    question: str,
    live_client: LiveStatsClient,
    current_season: int,
) -> TeamSeasonComparisonQuery | None:
    lowered = question.lower()
    if not any(hint in lowered for hint in COMPARISON_HINTS):
        return None
    matches = list(YEAR_TEAM_PATTERN.finditer(question))
    if len(matches) < 2:
        return None
    references: list[TeamSeasonReference] = []
    for match in matches[:2]:
        season = int(match.group(1))
        phrase = clean_team_phrase(match.group(2))
        reference = resolve_team_season_reference(connection, phrase, season, live_client, current_season)
        if reference is not None:
            references.append(reference)
    if len(references) < 2:
        return None
    comparator = "worse" if "worse than" in lowered else "better" if "better than" in lowered else "compare"
    return TeamSeasonComparisonQuery(
        left=references[0],
        right=references[1],
        first_n_games=extract_first_n_games(question),
        comparator=comparator,
    )


def resolve_team_season_reference(
    connection,
    phrase: str,
    season: int,
    live_client: LiveStatsClient,
    current_season: int,
) -> TeamSeasonReference | None:
    if season == current_season:
        live_match = resolve_live_team_phrase(phrase, live_client.teams(season))
        if live_match is not None:
            return TeamSeasonReference(
                season=season,
                phrase=phrase,
                display_name=f"{season} {live_match.name}",
                team_code=None,
                live_team=live_match,
            )
    if not table_exists(connection, "lahman_teams"):
        return None
    lowered = phrase.lower()
    row = connection.execute(
        """
        SELECT yearid, name, teamidretro
        FROM lahman_teams
        WHERE yearid = ?
          AND (
            lower(name) = ?
            OR lower(name) LIKE ?
            OR lower(teamidretro) = ?
            OR lower(teamid) = ?
            OR lower(franchid) = ?
          )
        ORDER BY
            CASE
                WHEN lower(name) = ? THEN 0
                WHEN lower(name) LIKE ? THEN 1
                ELSE 2
            END,
            w DESC,
            l ASC
        LIMIT 1
        """,
        (season, lowered, f"%{lowered}%", lowered, lowered, lowered, lowered, f"%{lowered}%"),
    ).fetchone()
    if row is None:
        return None
    return TeamSeasonReference(
        season=int(row["yearid"]),
        phrase=phrase,
        display_name=f"{row['yearid']} {row['name']}",
        team_code=str(row["teamidretro"] or ""),
        live_team=None,
    )


def resolve_live_team_phrase(phrase: str, teams: list[dict[str, Any]]) -> TeamIdentity | None:
    lowered = phrase.lower().strip()
    matches: list[tuple[int, TeamIdentity]] = []
    for team in teams:
        aliases = build_team_aliases(team)
        best_alias_length = 0
        for alias in aliases:
            alias_lower = alias.lower().strip()
            if not alias_lower:
                continue
            if lowered == alias_lower or lowered in alias_lower:
                best_alias_length = max(best_alias_length, len(alias_lower))
        if best_alias_length:
            matches.append((best_alias_length, normalize_team_identity(team)))
    if not matches:
        return None
    matches.sort(key=lambda item: item[0], reverse=True)
    if len(matches) > 1 and matches[0][0] == matches[1][0] and matches[0][1].name != matches[1][1].name:
        return None
    return matches[0][1]


def clean_team_phrase(value: str) -> str:
    cleaned = value.strip(" ?.!,'\"")
    cleaned = re.sub(r"^the\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(worse|better|compare|compared)\b$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\b(?:pitching\s+staff|pitching\s+roster|rotation|bullpen|staff|roster|lineup|squad|club|team)\b$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    return cleaned.strip()


def compute_ops(
    hits: int,
    doubles: int,
    triples: int,
    home_runs: int,
    at_bats: int,
    walks: int,
    hit_by_pitch: int,
    sacrifice_flies: int,
) -> float | None:
    if at_bats <= 0:
        return None
    singles = hits - doubles - triples - home_runs
    obp_denom = at_bats + walks + hit_by_pitch + sacrifice_flies
    obp = ((hits + walks + hit_by_pitch) / obp_denom) if obp_denom else None
    slg = (singles + (2 * doubles) + (3 * triples) + (4 * home_runs)) / at_bats
    if obp is None:
        return None
    return obp + slg


def parse_innings_to_outs(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    if "." not in text:
        try:
            return int(float(text) * 3)
        except ValueError:
            return 0
    whole, fraction = text.split(".", 1)
    try:
        whole_outs = int(whole) * 3
        extra_outs = int(fraction[:1] or "0")
    except ValueError:
        return 0
    return whole_outs + extra_outs


def game_log_sort_key(row: dict[str, Any]) -> tuple[str, int, int]:
    game = row.get("game", {}) if isinstance(row.get("game"), dict) else {}
    return (
        str(row.get("date") or ""),
        int(game.get("gameNumber") or 0),
        int(game.get("gamePk") or 0),
    )


def snapshot_row(snapshot: TeamSeasonSnapshot) -> dict[str, Any]:
    return {
        "team": snapshot.display_name,
        "scope": snapshot.scope_label,
        "games": snapshot.games_played,
        "record": snapshot.record,
        "win_pct": format_float(snapshot.win_pct, 3),
        "runs_per_game": format_float(snapshot.runs_per_game, 2),
        "runs_allowed_per_game": format_float(snapshot.runs_allowed_per_game, 2),
        "run_diff_per_game": format_float(snapshot.run_diff_per_game, 2),
        "ops": format_float(snapshot.ops, 3),
        "era": format_float(snapshot.era, 2),
        "fielding_pct": format_float(snapshot.fielding_pct, 3),
    }


def build_team_season_comparison_summary(
    query: TeamSeasonComparisonQuery,
    left: TeamSeasonSnapshot,
    right: TeamSeasonSnapshot,
) -> str:
    comparison_points = compare_snapshot_quality(left, right)
    scope_text = left.scope_label if left.scope_label == right.scope_label else f"{left.scope_label} vs {right.scope_label}"
    if query.comparator == "worse":
        verdict = "Yes." if comparison_points < 0 else "No."
    elif query.comparator == "better":
        verdict = "Yes." if comparison_points > 0 else "No."
    else:
        verdict = ""

    summary = (
        f"{verdict} Comparing {left.display_name} and {right.display_name} over {scope_text}, "
        f"{left.display_name} were {left.record} with {format_float(left.runs_per_game, 2)} runs scored per game, "
        f"{format_float(left.runs_allowed_per_game, 2)} allowed, {format_float(left.ops, 3)} OPS, and {format_float(left.era, 2)} ERA. "
        f"{right.display_name} were {right.record} with {format_float(right.runs_per_game, 2)} runs scored per game, "
        f"{format_float(right.runs_allowed_per_game, 2)} allowed, {format_float(right.ops, 3)} OPS, and {format_float(right.era, 2)} ERA."
    )
    if query.comparator == "compare":
        summary = f"{summary} The clearer edge belongs to {left.display_name if comparison_points > 0 else right.display_name if comparison_points < 0 else 'neither side decisively'}."
    elif query.comparator == "worse":
        summary = f"{summary} The main swing is {describe_biggest_gap(left, right)}."
    elif query.comparator == "better":
        summary = f"{summary} The main swing is {describe_biggest_gap(left, right)}."
    if not left.exact_window or not right.exact_window:
        summary = (
            f"{summary} This comparison is provisional because at least one current-season side has not yet completed the requested window."
        )
    return summary.strip()


def compare_snapshot_quality(left: TeamSeasonSnapshot, right: TeamSeasonSnapshot) -> int:
    axes = [
        compare_axis(left.win_pct, right.win_pct, higher_is_better=True),
        compare_axis(left.run_diff_per_game, right.run_diff_per_game, higher_is_better=True),
        compare_axis(left.ops, right.ops, higher_is_better=True),
        compare_axis(left.era, right.era, higher_is_better=False),
    ]
    return sum(axis for axis in axes if axis is not None)


def compare_axis(left_value: float | None, right_value: float | None, *, higher_is_better: bool) -> int | None:
    if left_value is None or right_value is None:
        return None
    if abs(left_value - right_value) < 1e-9:
        return 0
    left_better = left_value > right_value if higher_is_better else left_value < right_value
    return 1 if left_better else -1


def describe_biggest_gap(left: TeamSeasonSnapshot, right: TeamSeasonSnapshot) -> str:
    gaps = [
        ("record", absolute_gap(left.win_pct, right.win_pct), left.display_name if compare_axis(left.win_pct, right.win_pct, higher_is_better=True) == 1 else right.display_name),
        ("run differential", absolute_gap(left.run_diff_per_game, right.run_diff_per_game), left.display_name if compare_axis(left.run_diff_per_game, right.run_diff_per_game, higher_is_better=True) == 1 else right.display_name),
        ("OPS", absolute_gap(left.ops, right.ops), left.display_name if compare_axis(left.ops, right.ops, higher_is_better=True) == 1 else right.display_name),
        ("ERA", absolute_gap(left.era, right.era), left.display_name if compare_axis(left.era, right.era, higher_is_better=False) == 1 else right.display_name),
    ]
    filtered = [gap for gap in gaps if gap[1] is not None and gap[2]]
    if not filtered:
        return "too close to call from the available metrics"
    label, amount, winner = max(filtered, key=lambda item: item[1] or 0)
    return f"{label}, where {winner} had the stronger mark"


def absolute_gap(left_value: float | None, right_value: float | None) -> float | None:
    if left_value is None or right_value is None:
        return None
    return abs(left_value - right_value)
