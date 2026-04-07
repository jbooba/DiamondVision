from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .query_utils import question_mentions_explicit_year
from .storage import table_exists
from .team_evaluator import build_historical_percentiles, format_float, safe_float
from .team_season_compare import YEAR_TEAM_PATTERN, clean_team_phrase, compute_ops, resolve_team_season_reference


HISTORICAL_TEAM_ANALYSIS_HINTS = (
    "analyze",
    "analysis",
    "assess",
    "evaluate",
    "how good",
    "how bad",
    "how strong",
    "how weak",
    "what were",
)


@dataclass(slots=True)
class HistoricalTeamSnapshot:
    display_name: str
    season: int
    games: int
    wins: int
    losses: int
    runs: int
    runs_allowed: int
    home_runs: int
    era: float | None
    fielding_pct: float | None
    ops: float | None
    top_hitters: list[dict[str, Any]]
    top_pitchers: list[dict[str, Any]]
    historical_context: dict[str, Any]


class HistoricalTeamAnalysisResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        lowered = question.lower()
        if "compare" in lowered or " vs " in lowered or " versus " in lowered:
            return None
        if not question_mentions_explicit_year(question):
            return None
        if not any(hint in lowered for hint in HISTORICAL_TEAM_ANALYSIS_HINTS):
            return None
        current_season = self.settings.live_season or date.today().year
        match = YEAR_TEAM_PATTERN.search(question)
        if match is None:
            return None
        reference = resolve_team_season_reference(
            connection,
            clean_team_phrase(match.group(2)),
            int(match.group(1)),
            self.live_client,
            current_season,
        )
        if reference is None or reference.live_team is not None:
            return None
        snapshot = build_historical_team_snapshot(connection, reference)
        if snapshot is None:
            return None
        summary = build_historical_team_summary(snapshot)
        return EvidenceSnippet(
            source="Historical Team Analysis",
            title=f"{snapshot.display_name} season analysis",
            citation="Lahman team/batting/pitching tables with Retrosheet-backed historical season availability",
            summary=summary,
            payload={
                "analysis_type": "historical_team_analysis",
                "mode": "historical",
                "season": snapshot.season,
                "team": snapshot.display_name,
                "rows": [
                    {
                        "team": snapshot.display_name,
                        "games": snapshot.games,
                        "record": f"{snapshot.wins}-{snapshot.losses}",
                        "win_pct": format_float(snapshot.wins / snapshot.games if snapshot.games else None, 3),
                        "runs_per_game": format_float(snapshot.runs / snapshot.games if snapshot.games else None, 2),
                        "runs_allowed_per_game": format_float(snapshot.runs_allowed / snapshot.games if snapshot.games else None, 2),
                        "run_diff_per_game": format_float((snapshot.runs - snapshot.runs_allowed) / snapshot.games if snapshot.games else None, 2),
                        "ops": format_float(snapshot.ops, 3),
                        "era": format_float(snapshot.era, 2),
                        "fielding_pct": format_float(snapshot.fielding_pct, 3),
                    }
                ],
                "top_hitters": snapshot.top_hitters,
                "top_pitchers": snapshot.top_pitchers,
                "historical_context": snapshot.historical_context,
            },
        )


def build_historical_team_snapshot(connection, reference) -> HistoricalTeamSnapshot | None:
    if not (table_exists(connection, "lahman_teams") and table_exists(connection, "lahman_batting") and table_exists(connection, "lahman_pitching")):
        return None
    team_row = connection.execute(
        """
        SELECT *
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
    top_hitters = fetch_top_hitters(connection, reference.season, team_id)
    top_pitchers = fetch_top_pitchers(connection, reference.season, team_id)
    all_rows = connection.execute(
        """
        SELECT g, w, r, ra, hr, era, fp
        FROM lahman_teams
        WHERE CAST(g AS INTEGER) > 0
        """
    ).fetchall()
    historical_context = build_historical_percentiles(
        all_rows,
        win_pct=(safe_float(team_row["w"]) or 0.0) / (safe_float(team_row["g"]) or 1.0),
        runs_per_game=(safe_float(team_row["r"]) or 0.0) / (safe_float(team_row["g"]) or 1.0),
        runs_allowed_per_game=(safe_float(team_row["ra"]) or 0.0) / (safe_float(team_row["g"]) or 1.0),
        home_runs_per_game=(safe_float(team_row["hr"]) or 0.0) / (safe_float(team_row["g"]) or 1.0),
        era=safe_float(team_row["era"]),
        fielding_pct=safe_float(team_row["fp"]),
        run_diff_per_game=((safe_float(team_row["r"]) or 0.0) - (safe_float(team_row["ra"]) or 0.0)) / (safe_float(team_row["g"]) or 1.0),
    )
    return HistoricalTeamSnapshot(
        display_name=reference.display_name,
        season=reference.season,
        games=int(safe_float(team_row["g"]) or 0),
        wins=int(safe_float(team_row["w"]) or 0),
        losses=int(safe_float(team_row["l"]) or 0),
        runs=int(safe_float(team_row["r"]) or 0),
        runs_allowed=int(safe_float(team_row["ra"]) or 0),
        home_runs=int(safe_float(team_row["hr"]) or 0),
        era=safe_float(team_row["era"]),
        fielding_pct=safe_float(team_row["fp"]),
        ops=compute_ops(
            int(safe_float(team_row["h"]) or 0),
            int(safe_float(team_row["c_2b"]) or 0),
            int(safe_float(team_row["c_3b"]) or 0),
            int(safe_float(team_row["hr"]) or 0),
            int(safe_float(team_row["ab"]) or 0),
            int(safe_float(team_row["bb"]) or 0),
            int(safe_float(team_row["hbp"]) or 0),
            int(safe_float(team_row["sf"]) or 0),
        ),
        top_hitters=top_hitters,
        top_pitchers=top_pitchers,
        historical_context=historical_context,
    )


def fetch_top_hitters(connection, season: int, team_id: str) -> list[dict[str, Any]]:
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
                SUM(CAST(COALESCE(b.sf, '0') AS INTEGER)) AS sf,
                SUM(CAST(COALESCE(b.rbi, '0') AS INTEGER)) AS rbi
            FROM lahman_batting AS b
            WHERE b.yearid = ? AND lower(b.teamid) = ?
            GROUP BY b.playerid
        )
        SELECT
            batting.*,
            p.namefirst,
            p.namelast
        FROM batting
        JOIN lahman_people AS p ON p.playerid = batting.playerid
        ORDER BY batting.ab DESC
        """,
        (season, team_id.lower()),
    ).fetchall()
    hitters: list[dict[str, Any]] = []
    for row in rows:
        ops = compute_ops(
            int(row["h"] or 0),
            int(row["d2"] or 0),
            int(row["d3"] or 0),
            int(row["hr"] or 0),
            int(row["ab"] or 0),
            int(row["bb"] or 0),
            int(row["hbp"] or 0),
            int(row["sf"] or 0),
        )
        hitters.append(
            {
                "player": f"{row['namefirst']} {row['namelast']}".strip(),
                "ops": ops,
                "ab": int(row["ab"] or 0),
                "hr": int(row["hr"] or 0),
                "rbi": int(row["rbi"] or 0),
            }
        )
    hitters.sort(key=lambda item: (-(item["ops"] or 0.0), -item["ab"], item["player"]))
    return hitters[:3]


def fetch_top_pitchers(connection, season: int, team_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        WITH pitching AS (
            SELECT
                pit.playerid,
                SUM(CAST(COALESCE(pit.ipouts, '0') AS INTEGER)) AS ipouts,
                SUM(CAST(COALESCE(pit.er, '0') AS INTEGER)) AS er,
                SUM(CAST(COALESCE(pit.so, '0') AS INTEGER)) AS so
            FROM lahman_pitching AS pit
            WHERE pit.yearid = ? AND lower(pit.teamid) = ?
            GROUP BY pit.playerid
        )
        SELECT
            pitching.*,
            p.namefirst,
            p.namelast
        FROM pitching
        JOIN lahman_people AS p ON p.playerid = pitching.playerid
        ORDER BY pitching.ipouts DESC
        """,
        (season, team_id.lower()),
    ).fetchall()
    pitchers: list[dict[str, Any]] = []
    for row in rows:
        innings = (safe_float(row["ipouts"]) or 0.0) / 3.0
        era = ((safe_float(row["er"]) or 0.0) * 9.0 / innings) if innings else None
        pitchers.append(
            {
                "player": f"{row['namefirst']} {row['namelast']}".strip(),
                "innings": innings,
                "era": era,
                "so": int(row["so"] or 0),
            }
        )
    pitchers.sort(key=lambda item: (-(item["innings"] or 0.0), item["era"] if item["era"] is not None else 999.0))
    return pitchers[:3]


def build_historical_team_summary(snapshot: HistoricalTeamSnapshot) -> str:
    win_pct = snapshot.wins / snapshot.games if snapshot.games else 0.0
    runs_per_game = snapshot.runs / snapshot.games if snapshot.games else 0.0
    runs_allowed_per_game = snapshot.runs_allowed / snapshot.games if snapshot.games else 0.0
    run_diff_per_game = (snapshot.runs - snapshot.runs_allowed) / snapshot.games if snapshot.games else 0.0
    mlb_history = snapshot.historical_context.get("mlb_history") or snapshot.historical_context
    record_pct = percentile_text(mlb_history.get("win_pct_percentile"))
    offense_pct = percentile_text(mlb_history.get("runs_per_game_percentile"))
    prevention_pct = percentile_text(mlb_history.get("runs_allowed_per_game_percentile"), lower_is_better=True)
    summary = (
        f"{snapshot.display_name} finished {snapshot.wins}-{snapshot.losses} ({win_pct:.3f}) over {snapshot.games} games, "
        f"scoring {snapshot.runs} runs ({runs_per_game:.2f}/G) and allowing {snapshot.runs_allowed} ({runs_allowed_per_game:.2f}/G) "
        f"for a {run_diff_per_game:+.2f} run differential per game. They hit {snapshot.home_runs} home runs, posted a "
        f"{format_float(snapshot.ops, 3)} OPS, a {format_float(snapshot.era, 2)} ERA, and a {format_float(snapshot.fielding_pct, 3)} fielding percentage."
    )
    summary = (
        f"{summary} Relative to MLB history, that looks {record_pct} by record, {offense_pct} on offense, and {prevention_pct} at run prevention."
    )
    if snapshot.top_hitters:
        hitters_text = "; ".join(
            f"{hitter['player']} ({format_float(hitter['ops'], 3)} OPS, {hitter['hr']} HR, {hitter['rbi']} RBI)"
            for hitter in snapshot.top_hitters
        )
        summary = f"{summary} Best bats: {hitters_text}."
    if snapshot.top_pitchers:
        pitchers_text = "; ".join(
            f"{pitcher['player']} ({format_float(pitcher['era'], 2)} ERA in {format_float(pitcher['innings'], 1)} IP)"
            for pitcher in snapshot.top_pitchers
        )
        summary = f"{summary} Main arms: {pitchers_text}."
    return summary


def percentile_text(value: Any, *, lower_is_better: bool = False) -> str:
    percentile = safe_float(value)
    if percentile is None:
        return "unclear"
    if percentile >= 80:
        return "excellent"
    if percentile >= 65:
        return "well above average"
    if percentile >= 55:
        return "a bit above average"
    if percentile >= 45:
        return "roughly average"
    if percentile >= 30:
        return "below average"
    return "poor"
