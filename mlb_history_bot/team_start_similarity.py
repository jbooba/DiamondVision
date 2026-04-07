from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .query_utils import extract_first_n_games
from .storage import table_exists
from .team_evaluator import find_team_split, format_float, safe_float, safe_int
from .team_season_compare import (
    TeamSeasonComparisonResearcher,
    TeamSeasonReference,
    YEAR_TEAM_PATTERN,
    clean_team_phrase,
    compute_ops,
    resolve_team_season_reference,
)


SIMILARITY_HINTS = (
    "comparable season start",
    "comparable season starts",
    "similar season start",
    "similar season starts",
    "comparable start",
    "comparable starts",
    "similar start",
    "similar starts",
)


@dataclass(slots=True)
class TeamStartSimilarityQuery:
    reference: TeamSeasonReference
    first_n_games: int | None


@dataclass(slots=True)
class TeamStartSnapshot:
    season: int
    team_code: str
    team_name: str
    games_played: int
    wins: int
    losses: int
    win_pct: float | None
    runs_per_game: float | None
    runs_allowed_per_game: float | None
    run_diff_per_game: float | None
    ops: float | None
    era: float | None

    @property
    def record(self) -> str:
        return f"{self.wins}-{self.losses}"


class TeamStartSimilarityResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)
        self.comparison = TeamSeasonComparisonResearcher(settings)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_team_start_similarity_query(connection, question, self.live_client, self.settings.live_season or date.today().year)
        if query is None or not table_exists(connection, "retrosheet_teamstats"):
            return None

        target_snapshot = self._build_target_snapshot(connection, query)
        if target_snapshot is None or target_snapshot.games_played <= 0:
            return None

        comparable_rows = fetch_historical_similar_starts(connection, target_snapshot)
        if not comparable_rows:
            return None

        lines = [
            f"{index}. {row['team']} ({row['record']}, similarity {row['similarity_score']})"
            for index, row in enumerate(comparable_rows[:5], start=1)
        ]
        summary = (
            f"The closest historical season starts to the {target_snapshot.season} {target_snapshot.team_name} through "
            f"{target_snapshot.games_played} game(s) are {' '.join(lines)}."
        )
        return EvidenceSnippet(
            source="Team Start Similarity",
            title=f"{target_snapshot.season} {target_snapshot.team_name} comparable starts",
            citation="Retrosheet first-N-games windows plus MLB Stats API current season game logs",
            summary=summary,
            payload={
                "analysis_type": "team_start_similarity",
                "mode": "hybrid" if query.reference.live_team is not None else "historical",
                "target": snapshot_row(target_snapshot),
                "rows": comparable_rows,
            },
        )

    def _build_target_snapshot(self, connection, query: TeamStartSimilarityQuery) -> TeamStartSnapshot | None:
        current_season = self.settings.live_season or date.today().year
        reference = query.reference
        if reference.live_team is not None and reference.season == current_season:
            games_played = query.first_n_games
            if games_played is None:
                hitting_split = find_team_split(self.live_client.all_team_group_stats("hitting", current_season), reference.live_team.team_id)
                games_played = safe_int(hitting_split.get("stat", {}).get("gamesPlayed")) if hitting_split else None
            if not games_played:
                return None
            snapshot = self.comparison._live_window_snapshot(reference.live_team, reference.season, games_played)
            if snapshot is None:
                return None
            return TeamStartSnapshot(
                season=reference.season,
                team_code=reference.live_team.abbreviation or reference.live_team.club_name,
                team_name=reference.live_team.name,
                games_played=snapshot.games_played,
                wins=snapshot.wins or 0,
                losses=snapshot.losses or 0,
                win_pct=snapshot.win_pct,
                runs_per_game=snapshot.runs_per_game,
                runs_allowed_per_game=snapshot.runs_allowed_per_game,
                run_diff_per_game=snapshot.run_diff_per_game,
                ops=snapshot.ops,
                era=snapshot.era,
            )

        games_played = query.first_n_games or 10
        snapshot = self.comparison._historical_window_snapshot(connection, reference, games_played)
        if snapshot is None:
            return None
        return TeamStartSnapshot(
            season=reference.season,
            team_code=reference.team_code or reference.display_name.split(" ", 1)[1],
            team_name=reference.display_name.split(" ", 1)[1],
            games_played=snapshot.games_played,
            wins=snapshot.wins or 0,
            losses=snapshot.losses or 0,
            win_pct=snapshot.win_pct,
            runs_per_game=snapshot.runs_per_game,
            runs_allowed_per_game=snapshot.runs_allowed_per_game,
            run_diff_per_game=snapshot.run_diff_per_game,
            ops=snapshot.ops,
            era=snapshot.era,
        )


def parse_team_start_similarity_query(
    connection,
    question: str,
    live_client: LiveStatsClient,
    current_season: int,
) -> TeamStartSimilarityQuery | None:
    lowered = question.lower()
    if not any(hint in lowered for hint in SIMILARITY_HINTS):
        return None
    match = YEAR_TEAM_PATTERN.search(question)
    if match is None:
        return None
    reference = resolve_team_season_reference(
        connection,
        clean_team_phrase(match.group(2)),
        int(match.group(1)),
        live_client,
        current_season,
    )
    if reference is None:
        return None
    return TeamStartSimilarityQuery(reference=reference, first_n_games=extract_first_n_games(question))


def fetch_historical_similar_starts(connection, target: TeamStartSnapshot) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        WITH ordered_games AS (
            SELECT
                team,
                substr(date, 1, 4) AS season,
                CAST(COALESCE(b_r, '0') AS INTEGER) AS runs,
                CAST(COALESCE(b_hr, '0') AS INTEGER) AS home_runs,
                CAST(COALESCE(b_ab, '0') AS INTEGER) AS at_bats,
                CAST(COALESCE(b_h, '0') AS INTEGER) AS hits,
                CAST(COALESCE(b_d, '0') AS INTEGER) AS doubles,
                CAST(COALESCE(b_t, '0') AS INTEGER) AS triples,
                CAST(COALESCE(b_w, '0') AS INTEGER) AS walks,
                CAST(COALESCE(b_hbp, '0') AS INTEGER) AS hbp,
                CAST(COALESCE(b_sf, '0') AS INTEGER) AS sf,
                CAST(COALESCE(p_r, '0') AS INTEGER) AS runs_allowed,
                CAST(COALESCE(p_er, '0') AS INTEGER) AS earned_runs,
                CAST(COALESCE(p_ipouts, '0') AS INTEGER) AS ipouts,
                CAST(COALESCE(win, '0') AS INTEGER) AS wins,
                CAST(COALESCE(loss, '0') AS INTEGER) AS losses,
                ROW_NUMBER() OVER (
                    PARTITION BY team, substr(date, 1, 4)
                    ORDER BY date, CAST(COALESCE(number, '0') AS INTEGER), gid
                ) AS game_number
            FROM retrosheet_teamstats
            WHERE stattype = 'value'
              AND gametype = 'regular'
        )
        SELECT
            ordered_games.team,
            ordered_games.season,
            COUNT(*) AS games_played,
            SUM(ordered_games.runs) AS runs,
            SUM(ordered_games.home_runs) AS home_runs,
            SUM(ordered_games.at_bats) AS at_bats,
            SUM(ordered_games.hits) AS hits,
            SUM(ordered_games.doubles) AS doubles,
            SUM(ordered_games.triples) AS triples,
            SUM(ordered_games.walks) AS walks,
            SUM(ordered_games.hbp) AS hbp,
            SUM(ordered_games.sf) AS sf,
            SUM(ordered_games.runs_allowed) AS runs_allowed,
            SUM(ordered_games.earned_runs) AS earned_runs,
            SUM(ordered_games.ipouts) AS ipouts,
            SUM(ordered_games.wins) AS wins,
            SUM(ordered_games.losses) AS losses,
            MIN(teams.name) AS team_name
        FROM ordered_games
        LEFT JOIN lahman_teams AS teams
            ON CAST(teams.yearid AS TEXT) = ordered_games.season
           AND lower(teams.teamidretro) = lower(ordered_games.team)
        WHERE ordered_games.game_number <= ?
        GROUP BY ordered_games.team, ordered_games.season
        HAVING COUNT(*) = ?
        """,
        (target.games_played, target.games_played),
    ).fetchall()

    similar_rows: list[dict[str, Any]] = []
    for row in rows:
        season = int(row["season"])
        team_code = str(row["team"] or "")
        team_name = str(row["team_name"] or team_code).strip()
        if season == target.season and team_name.lower() == target.team_name.lower():
            continue
        games_played = int(row["games_played"] or 0)
        wins = int(row["wins"] or 0)
        losses = int(row["losses"] or 0)
        runs = int(row["runs"] or 0)
        runs_allowed = int(row["runs_allowed"] or 0)
        candidate = TeamStartSnapshot(
            season=season,
            team_code=team_code,
            team_name=team_name,
            games_played=games_played,
            wins=wins,
            losses=losses,
            win_pct=(wins / games_played) if games_played else None,
            runs_per_game=(runs / games_played) if games_played else None,
            runs_allowed_per_game=(runs_allowed / games_played) if games_played else None,
            run_diff_per_game=((runs - runs_allowed) / games_played) if games_played else None,
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
        )
        score = similarity_score(target, candidate)
        similar_rows.append(
            {
                **snapshot_row(candidate),
                "similarity_score": format_float(score, 3),
            }
        )
    similar_rows.sort(key=lambda row: (safe_float(row["similarity_score"]) or 999.0, row["season"], row["team"]))
    return similar_rows[:8]


def similarity_score(target: TeamStartSnapshot, candidate: TeamStartSnapshot) -> float:
    score = 0.0
    score += abs((target.win_pct or 0.0) - (candidate.win_pct or 0.0)) * 5.0
    score += abs((target.run_diff_per_game or 0.0) - (candidate.run_diff_per_game or 0.0)) * 1.2
    score += abs((target.ops or 0.0) - (candidate.ops or 0.0)) * 4.0
    score += abs((target.era or 0.0) - (candidate.era or 0.0)) * 0.7
    return score


def snapshot_row(snapshot: TeamStartSnapshot) -> dict[str, Any]:
    return {
        "team": f"{snapshot.season} {snapshot.team_name}",
        "season": snapshot.season,
        "games": snapshot.games_played,
        "record": snapshot.record,
        "win_pct": format_float(snapshot.win_pct, 3),
        "runs_per_game": format_float(snapshot.runs_per_game, 2),
        "runs_allowed_per_game": format_float(snapshot.runs_allowed_per_game, 2),
        "run_diff_per_game": format_float(snapshot.run_diff_per_game, 2),
        "ops": format_float(snapshot.ops, 3),
        "era": format_float(snapshot.era, 2),
    }
