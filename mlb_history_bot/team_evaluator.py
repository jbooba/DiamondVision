from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from statistics import mean
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .query_utils import question_mentions_explicit_year
from .storage import table_exists


NEGATIVE_HINTS = {
    "how bad",
    "bad",
    "awful",
    "terrible",
    "brutal",
    "cooked",
    "weak",
    "mess",
    "problem",
    "problems",
    "sucks",
}
POSITIVE_HINTS = {
    "how good",
    "good",
    "great",
    "strong",
    "elite",
    "stacked",
    "legit",
    "contender",
}
ANALYSIS_HINTS = {
    "roster",
    "team",
    "lineup",
    "rotation",
    "bullpen",
    "offense",
    "offensive",
    "hitting",
    "pitching",
    "defense",
    "defensive",
    "analyze",
    "analysis",
    "assess",
    "evaluate",
    "break down",
    "where are",
    "what's wrong",
    "what is wrong",
    "how does",
}
CURRENT_HINTS = {"current", "right now", "this season", "roster"}
FOCUS_KEYWORDS = {
    "rotation": ("rotation", "starting pitching", "starter", "starters"),
    "bullpen": ("bullpen", "relief", "relievers", "closer"),
    "pitching": ("pitching", "staff"),
    "offense": ("offense", "offensive", "lineup", "hitting"),
    "defense": ("defense", "defensive", "fielding", "glove"),
}


@dataclass(slots=True)
class TeamIdentity:
    team_id: int
    name: str
    abbreviation: str
    short_name: str
    club_name: str
    franchise_name: str
    location_name: str
    league: str
    division: str


@dataclass(slots=True)
class TeamEvaluationQuery:
    team: TeamIdentity
    season: int
    tone: str
    focus: str


class TeamEvaluator:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = self.parse_question(question)
        if query is None:
            return None

        standings = self.live_client.standings(query.season)
        standings_row = self._find_standings_row(standings.get("standings", []), query.team)
        hitting_splits = self.live_client.all_team_group_stats("hitting", query.season)
        pitching_splits = self.live_client.all_team_group_stats("pitching", query.season)
        fielding_splits = self.live_client.all_team_group_stats("fielding", query.season)
        hitting = find_team_split(hitting_splits, query.team.team_id)
        pitching = find_team_split(pitching_splits, query.team.team_id)
        fielding = find_team_split(fielding_splits, query.team.team_id)
        if hitting is None or pitching is None or fielding is None:
            return None

        games_played = safe_int(hitting["stat"].get("gamesPlayed")) or 0
        wins = safe_int(standings_row.get("wins")) if standings_row else None
        losses = safe_int(standings_row.get("losses")) if standings_row else None
        win_pct = safe_float(standings_row.get("pct")) if standings_row else None
        if win_pct is None and wins is not None and losses is not None and wins + losses:
            win_pct = wins / (wins + losses)
        run_diff = (safe_float(hitting["stat"].get("runs")) or 0.0) - (safe_float(pitching["stat"].get("runs")) or 0.0)
        run_diff_pg = run_diff / games_played if games_played else None

        offense_metrics = [
            metric_snapshot(hitting_splits, query.team.team_id, "runs", higher_is_better=True, per_game=True),
            metric_snapshot(hitting_splits, query.team.team_id, "ops", higher_is_better=True),
            metric_snapshot(hitting_splits, query.team.team_id, "obp", higher_is_better=True),
            metric_snapshot(hitting_splits, query.team.team_id, "homeRuns", higher_is_better=True, per_game=True),
        ]
        pitching_metrics = [
            metric_snapshot(pitching_splits, query.team.team_id, "era", higher_is_better=False),
            metric_snapshot(pitching_splits, query.team.team_id, "whip", higher_is_better=False),
            metric_snapshot(pitching_splits, query.team.team_id, "strikeoutWalkRatio", higher_is_better=True),
            metric_snapshot(pitching_splits, query.team.team_id, "homeRunsPer9", higher_is_better=False),
            metric_snapshot(pitching_splits, query.team.team_id, "runsScoredPer9", higher_is_better=False),
        ]
        defense_metrics = [
            metric_snapshot(fielding_splits, query.team.team_id, "fielding", higher_is_better=True),
            metric_snapshot(fielding_splits, query.team.team_id, "errors", higher_is_better=False, per_game=True),
        ]
        drs_snapshot = self._current_team_drs(connection, query.team, query.season)
        if drs_snapshot is not None:
            defense_metrics.append(drs_snapshot)

        offense_score = composite_score(offense_metrics)
        pitching_score = composite_score(pitching_metrics)
        defense_score = composite_score(defense_metrics)
        record_score = score_from_value(win_pct, [safe_float(row.get("pct")) for row in standings.get("standings", [])], True)
        run_diff_score = score_from_value(
            run_diff_pg,
            build_values_from_team_splits(hitting_splits, pitching_splits, "runs", "runs"),
            True,
        )
        component_scores = {
            "offense": offense_score,
            "pitching": pitching_score,
            "defense": defense_score,
            "record": average_scores(record_score, run_diff_score),
        }
        overall_score = mean(component_scores.values())

        roster = self.live_client.team_roster(query.team.team_id, season=query.season)
        roster_people = self.live_client.people_with_stats(
            [entry.get("person", {}).get("id") for entry in roster],
            season=query.season,
        )
        roster_profile = build_roster_profile(roster_people)
        historical_context = self._historical_context(
            connection,
            query.team,
            win_pct=win_pct,
            runs_per_game=per_game_value(hitting["stat"].get("runs"), hitting["stat"].get("gamesPlayed")),
            runs_allowed_per_game=per_game_value(pitching["stat"].get("runs"), pitching["stat"].get("gamesPlayed")),
            home_runs_per_game=per_game_value(hitting["stat"].get("homeRuns"), hitting["stat"].get("gamesPlayed")),
            era=safe_float(pitching["stat"].get("era")),
            fielding_pct=safe_float(fielding["stat"].get("fielding")),
            run_diff_per_game=run_diff_pg,
        )

        summary = build_team_evaluation_summary(
            query,
            games_played=games_played,
            wins=wins,
            losses=losses,
            win_pct=win_pct,
            hitting=hitting["stat"],
            pitching=pitching["stat"],
            fielding=fielding["stat"],
            overall_score=overall_score,
            component_scores=component_scores,
            historical_context=historical_context,
            roster_profile=roster_profile,
            drs_snapshot=drs_snapshot,
        )
        return EvidenceSnippet(
            source="Team Evaluator",
            title=f"{query.team.name} {query.season} evaluation",
            citation="MLB Stats API current team stats/roster plus Lahman historical team baselines",
            summary=summary,
            payload={
                "analysis_type": "team_evaluation",
                "mode": "live",
                "team": {
                    "id": query.team.team_id,
                    "name": query.team.name,
                    "abbreviation": query.team.abbreviation,
                    "club_name": query.team.club_name,
                },
                "season": query.season,
                "focus": query.focus,
                "tone": query.tone,
                "games_played": games_played,
                "wins": wins,
                "losses": losses,
                "win_pct": win_pct,
                "run_diff": run_diff,
                "overall_score": round(overall_score, 1),
                "assessment": assessment_label(overall_score),
                "component_scores": {key: round(value, 1) for key, value in component_scores.items()},
                "historical_context": historical_context,
                "roster_profile": roster_profile,
                "team_metrics": {
                    "hitting": dict(hitting["stat"]),
                    "pitching": dict(pitching["stat"]),
                    "fielding": dict(fielding["stat"]),
                },
                "defense_drs": drs_snapshot,
            },
        )

    def parse_question(self, question: str) -> TeamEvaluationQuery | None:
        lowered = question.lower()
        if ("compare" in lowered or " versus " in lowered or " vs " in lowered) and question_mentions_explicit_year(question):
            return None
        if not (
            any(hint in lowered for hint in NEGATIVE_HINTS | POSITIVE_HINTS | ANALYSIS_HINTS)
            and (any(hint in lowered for hint in CURRENT_HINTS) or not question_mentions_explicit_year(question))
        ):
            return None
        season = self.settings.live_season or date.today().year
        teams = self.live_client.teams(season)
        team = resolve_team_from_question(question, teams)
        if team is None:
            return None
        return TeamEvaluationQuery(
            team=team,
            season=season,
            tone=infer_tone(question),
            focus=infer_focus(question),
        )

    def _find_standings_row(self, rows: list[dict[str, Any]], team: TeamIdentity) -> dict[str, Any] | None:
        for row in rows:
            label = str(row.get("team") or "").strip().lower()
            if label in {
                team.short_name.lower(),
                team.club_name.lower(),
                team.name.lower(),
                team.franchise_name.lower(),
            }:
                return row
        return None

    def _historical_context(
        self,
        connection,
        team: TeamIdentity,
        *,
        win_pct: float | None,
        runs_per_game: float | None,
        runs_allowed_per_game: float | None,
        home_runs_per_game: float | None,
        era: float | None,
        fielding_pct: float | None,
        run_diff_per_game: float | None,
    ) -> dict[str, Any]:
        if not table_exists(connection, "lahman_teams"):
            return {}
        row = connection.execute(
            """
            SELECT franchid
            FROM lahman_teams
            WHERE lower(name) = ?
            ORDER BY CAST(yearid AS INTEGER) DESC
            LIMIT 1
            """,
            (team.name.lower(),),
        ).fetchone()
        if row is None:
            row = connection.execute(
                """
                SELECT franchid
                FROM lahman_teams
                WHERE lower(name) LIKE ?
                ORDER BY CAST(yearid AS INTEGER) DESC
                LIMIT 1
                """,
                (f"%{team.club_name.lower()}%",),
            ).fetchone()
        franchid = row["franchid"] if row is not None else None
        all_rows = connection.execute(
            """
            SELECT franchid, g, w, r, ra, hr, era, fp
            FROM lahman_teams
            WHERE CAST(g AS INTEGER) > 0
            """
        ).fetchall()
        if not all_rows:
            return {}
        franchise_rows = [candidate for candidate in all_rows if franchid and candidate["franchid"] == franchid]
        return {
            "franchid": franchid,
            "mlb_history": build_historical_percentiles(
                all_rows,
                win_pct=win_pct,
                runs_per_game=runs_per_game,
                runs_allowed_per_game=runs_allowed_per_game,
                home_runs_per_game=home_runs_per_game,
                era=era,
                fielding_pct=fielding_pct,
                run_diff_per_game=run_diff_per_game,
            ),
            "franchise_history": build_historical_percentiles(
                franchise_rows,
                win_pct=win_pct,
                runs_per_game=runs_per_game,
                runs_allowed_per_game=runs_allowed_per_game,
                home_runs_per_game=home_runs_per_game,
                era=era,
                fielding_pct=fielding_pct,
                run_diff_per_game=run_diff_per_game,
            ),
        }

    def _current_team_drs(self, connection, team: TeamIdentity, season: int) -> dict[str, Any] | None:
        if not table_exists(connection, "fielding_bible_team_drs"):
            return None
        latest_snapshot = connection.execute(
            """
            SELECT snapshot_at
            FROM fielding_bible_team_drs
            WHERE season = ?
            ORDER BY snapshot_at DESC
            LIMIT 1
            """,
            (season,),
        ).fetchone()
        snapshot_at = latest_snapshot["snapshot_at"] if latest_snapshot else ""
        row = connection.execute(
            """
            SELECT team_id, nickname, total
            FROM fielding_bible_team_drs
            WHERE season = ?
              AND snapshot_at = ?
              AND (team_id = ? OR lower(nickname) = ?)
            LIMIT 1
            """,
            (season, snapshot_at, team.team_id, team.club_name.lower()),
        ).fetchone()
        if row is None:
            return None
        all_rows = connection.execute(
            """
            SELECT total
            FROM fielding_bible_team_drs
            WHERE season = ? AND snapshot_at = ?
            """,
            (season, snapshot_at),
        ).fetchall()
        totals = [safe_float(candidate["total"]) for candidate in all_rows]
        filtered_totals = [value for value in totals if value is not None]
        return {
            "label": "team DRS",
            "value": safe_float(row["total"]),
            "rank": rank_from_values(safe_float(row["total"]), filtered_totals, True),
            "team_count": len(filtered_totals),
            "league_average": mean(filtered_totals) if filtered_totals else None,
            "percentile": percentile_from_values(safe_float(row["total"]), filtered_totals, True),
            "higher_is_better": True,
            "source": "Fielding Bible / SIS team DRS",
        }


def looks_like_team_evaluation(question: str) -> bool:
    lowered = question.lower()
    return any(hint in lowered for hint in NEGATIVE_HINTS | POSITIVE_HINTS | ANALYSIS_HINTS)


def resolve_team_from_question(question: str, teams: list[dict[str, Any]]) -> TeamIdentity | None:
    lowered = question.lower()
    matches: list[tuple[int, TeamIdentity]] = []
    for team in teams:
        aliases = build_team_aliases(team)
        best_alias_length = 0
        for alias in aliases:
            alias_lower = alias.lower().strip()
            if not alias_lower:
                continue
            if contains_alias(lowered, alias_lower):
                best_alias_length = max(best_alias_length, len(alias_lower))
        if best_alias_length:
            matches.append((best_alias_length, normalize_team_identity(team)))
    if not matches:
        return None
    matches.sort(key=lambda item: item[0], reverse=True)
    if len(matches) > 1 and matches[0][0] == matches[1][0] and matches[0][1].name != matches[1][1].name:
        return None
    return matches[0][1]


def build_team_aliases(team: dict[str, Any]) -> set[str]:
    location = str(team.get("locationName") or "").strip()
    club_name = str(team.get("clubName") or team.get("teamName") or "").strip()
    franchise_name = str(team.get("franchiseName") or "").strip()
    name = str(team.get("name") or "").strip()
    short_name = str(team.get("shortName") or "").strip()
    abbreviation = str(team.get("abbreviation") or "").strip()
    file_code = str(team.get("fileCode") or "").strip()
    team_code = str(team.get("teamCode") or "").strip()
    aliases = {
        name,
        short_name,
        club_name,
        franchise_name,
        f"{location} {club_name}".strip(),
        abbreviation,
        file_code,
        team_code,
    }
    return {alias for alias in aliases if alias}


def normalize_team_identity(team: dict[str, Any]) -> TeamIdentity:
    return TeamIdentity(
        team_id=int(team.get("id") or 0),
        name=str(team.get("name") or "").strip(),
        abbreviation=str(team.get("abbreviation") or "").strip(),
        short_name=str(team.get("shortName") or "").strip(),
        club_name=str(team.get("clubName") or team.get("teamName") or "").strip(),
        franchise_name=str(team.get("franchiseName") or "").strip(),
        location_name=str(team.get("locationName") or "").strip(),
        league=str(team.get("league", {}).get("name") or "").strip(),
        division=str(team.get("division", {}).get("name") or "").strip(),
    )


def contains_alias(question: str, alias: str) -> bool:
    question_tokens = f" {question} "
    alias_tokens = f" {alias} "
    return alias_tokens in question_tokens


def infer_tone(question: str) -> str:
    lowered = question.lower()
    if any(hint in lowered for hint in NEGATIVE_HINTS):
        return "negative"
    if any(hint in lowered for hint in POSITIVE_HINTS):
        return "positive"
    return "neutral"


def infer_focus(question: str) -> str:
    lowered = question.lower()
    for focus, keywords in FOCUS_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return focus
    return "overall"


def find_team_split(splits: list[dict[str, Any]], team_id: int) -> dict[str, Any] | None:
    for split in splits:
        if int(split.get("team", {}).get("id") or 0) == team_id:
            return split
    return None


def metric_snapshot(
    splits: list[dict[str, Any]],
    team_id: int,
    metric_key: str,
    *,
    higher_is_better: bool,
    per_game: bool = False,
    label: str | None = None,
) -> dict[str, Any] | None:
    target_value = None
    values: list[float | None] = []
    for split in splits:
        stat = split.get("stat", {})
        value = safe_float(stat.get(metric_key))
        if per_game:
            games = safe_float(stat.get("gamesPlayed") or stat.get("games"))
            value = value / games if value is not None and games else None
        values.append(value)
        if int(split.get("team", {}).get("id") or 0) == team_id:
            target_value = value
    filtered_values = [value for value in values if value is not None]
    if target_value is None or not filtered_values:
        return None
    return {
        "label": label or metric_key,
        "value": target_value,
        "rank": rank_from_values(target_value, filtered_values, higher_is_better),
        "team_count": len(filtered_values),
        "league_average": mean(filtered_values),
        "percentile": percentile_from_values(target_value, filtered_values, higher_is_better),
        "higher_is_better": higher_is_better,
    }


def build_values_from_team_splits(
    hitting_splits: list[dict[str, Any]],
    pitching_splits: list[dict[str, Any]],
    hitting_key: str,
    pitching_key: str,
) -> list[float]:
    values: list[float] = []
    pitching_by_team = {int(split.get("team", {}).get("id") or 0): split for split in pitching_splits}
    for hitting in hitting_splits:
        team_id = int(hitting.get("team", {}).get("id") or 0)
        pitching = pitching_by_team.get(team_id)
        if pitching is None:
            continue
        games = safe_float(hitting.get("stat", {}).get("gamesPlayed"))
        runs_scored = safe_float(hitting.get("stat", {}).get(hitting_key))
        runs_allowed = safe_float(pitching.get("stat", {}).get(pitching_key))
        if not games or runs_scored is None or runs_allowed is None:
            continue
        values.append((runs_scored - runs_allowed) / games)
    return values


def composite_score(metrics: list[dict[str, Any] | None]) -> float:
    scores = [normalized_rank_score(metric["rank"], metric["team_count"]) for metric in metrics if metric]
    return mean(scores) if scores else 50.0


def average_scores(*scores: float | None) -> float:
    valid_scores = [score for score in scores if score is not None]
    return mean(valid_scores) if valid_scores else 50.0


def score_from_value(target: float | None, values: list[float | None], higher_is_better: bool) -> float | None:
    filtered_values = [value for value in values if value is not None]
    if target is None or not filtered_values:
        return None
    rank = rank_from_values(target, filtered_values, higher_is_better)
    return normalized_rank_score(rank, len(filtered_values))


def rank_from_values(target: float | None, values: list[float | None], higher_is_better: bool) -> int:
    filtered_values = [value for value in values if value is not None]
    if target is None or not filtered_values:
        return len(filtered_values)
    better = sum(1 for value in filtered_values if value > target) if higher_is_better else sum(
        1 for value in filtered_values if value < target
    )
    return min(len(filtered_values), better + 1)


def percentile_from_values(target: float | None, values: list[float | None], higher_is_better: bool) -> float | None:
    filtered_values = [value for value in values if value is not None]
    if target is None or len(filtered_values) <= 1:
        return None
    rank = rank_from_values(target, filtered_values, higher_is_better)
    return round(100.0 * (len(filtered_values) - rank) / (len(filtered_values) - 1), 1)


def normalized_rank_score(rank: int, team_count: int) -> float:
    if team_count <= 1:
        return 50.0
    return round(100.0 * (team_count - rank) / (team_count - 1), 1)


def assessment_label(score: float) -> str:
    if score >= 80:
        return "elite"
    if score >= 65:
        return "strong"
    if score >= 55:
        return "above average"
    if score >= 45:
        return "roughly average"
    if score >= 35:
        return "below average"
    if score >= 20:
        return "bad"
    return "brutal"


def build_roster_profile(people: list[dict[str, Any]]) -> dict[str, Any]:
    hitters: list[dict[str, Any]] = []
    starters: list[dict[str, Any]] = []
    relievers: list[dict[str, Any]] = []
    ages: list[int] = []
    for person in people:
        current_age = safe_int(person.get("currentAge"))
        if current_age is not None:
            ages.append(current_age)
        position_type = str(person.get("primaryPosition", {}).get("type") or "").lower()
        hitting = extract_primary_stat_line(person, "hitting")
        pitching = extract_primary_stat_line(person, "pitching")
        if hitting and position_type != "pitcher":
            hitters.append(
                {
                    "name": person.get("fullName"),
                    "age": current_age,
                    "plate_appearances": safe_float(hitting.get("plateAppearances")) or 0.0,
                    "ops": safe_float(hitting.get("ops")),
                    "obp": safe_float(hitting.get("obp")),
                    "slg": safe_float(hitting.get("slg")),
                    "home_runs": safe_int(hitting.get("homeRuns")) or 0,
                    "rbi": safe_int(hitting.get("rbi")) or 0,
                }
            )
        if pitching:
            entry = {
                "name": person.get("fullName"),
                "age": current_age,
                "innings": safe_float(pitching.get("inningsPitched")) or 0.0,
                "games_started": safe_int(pitching.get("gamesStarted")) or 0,
                "era": safe_float(pitching.get("era")),
                "whip": safe_float(pitching.get("whip")),
                "strikeouts_per_9": safe_float(pitching.get("strikeoutsPer9Inn")),
                "holds": safe_int(pitching.get("holds")) or 0,
                "saves": safe_int(pitching.get("saves")) or 0,
            }
            if entry["games_started"] > 0:
                starters.append(entry)
            elif entry["innings"] > 0:
                relievers.append(entry)

    hitters.sort(key=lambda player: (-(player["ops"] or -1.0), -player["plate_appearances"], str(player["name"] or "")))
    starters.sort(key=lambda player: (-(player["innings"] or 0.0), player["era"] if player["era"] is not None else 999.0))
    relievers.sort(key=lambda player: (player["era"] if player["era"] is not None else 999.0, -(player["innings"] or 0.0)))
    qualified_hitters = [player for player in hitters if player["plate_appearances"] >= 10] or hitters
    qualified_starters = [player for player in starters if player["innings"] >= 5.0] or starters
    qualified_relievers = [player for player in relievers if player["innings"] >= 3.0] or relievers
    return {
        "average_age": round(mean(ages), 1) if ages else None,
        "lineup_depth_ops": round(mean(player["ops"] for player in qualified_hitters[:6] if player["ops"] is not None), 3)
        if qualified_hitters
        else None,
        "rotation_era": round(mean(player["era"] for player in qualified_starters[:4] if player["era"] is not None), 2)
        if qualified_starters
        else None,
        "bullpen_era": round(mean(player["era"] for player in qualified_relievers[:4] if player["era"] is not None), 2)
        if qualified_relievers
        else None,
        "top_hitters": qualified_hitters[:3],
        "top_starters": qualified_starters[:3],
        "top_relievers": qualified_relievers[:3],
    }


def extract_primary_stat_line(person: dict[str, Any], group_name: str) -> dict[str, Any] | None:
    for stats_group in person.get("stats", []):
        if str(stats_group.get("group", {}).get("displayName") or "").lower() != group_name:
            continue
        splits = stats_group.get("splits", [])
        if not splits:
            return None
        if group_name != "fielding":
            return splits[0].get("stat", {})
        best_split = max(splits, key=lambda split: safe_float(split.get("stat", {}).get("gamesPlayed")) or 0.0)
        return best_split.get("stat", {})
    return None


def build_historical_percentiles(
    rows: list[Any],
    *,
    win_pct: float | None,
    runs_per_game: float | None,
    runs_allowed_per_game: float | None,
    home_runs_per_game: float | None,
    era: float | None,
    fielding_pct: float | None,
    run_diff_per_game: float | None,
) -> dict[str, Any]:
    if not rows:
        return {}
    win_pcts = [safe_ratio(row["w"], row["g"]) for row in rows]
    runs_pg = [safe_ratio(row["r"], row["g"]) for row in rows]
    runs_allowed_pg = [safe_ratio(row["ra"], row["g"]) for row in rows]
    home_runs_pg = [safe_ratio(row["hr"], row["g"]) for row in rows]
    eras = [safe_float(row["era"]) for row in rows]
    fielding_pcts = [safe_float(row["fp"]) for row in rows]
    run_diff_pg_values = [
        (safe_float(row["r"]) - safe_float(row["ra"])) / safe_float(row["g"])
        for row in rows
        if safe_float(row["r"]) is not None and safe_float(row["ra"]) is not None and safe_float(row["g"])
    ]
    return {
        "sample_size": len(rows),
        "win_pct_percentile": percentile_from_values(win_pct, win_pcts, True),
        "runs_per_game_percentile": percentile_from_values(runs_per_game, runs_pg, True),
        "runs_allowed_per_game_percentile": percentile_from_values(runs_allowed_per_game, runs_allowed_pg, False),
        "home_runs_per_game_percentile": percentile_from_values(home_runs_per_game, home_runs_pg, True),
        "era_percentile": percentile_from_values(era, eras, False),
        "fielding_pct_percentile": percentile_from_values(fielding_pct, fielding_pcts, True),
        "run_diff_per_game_percentile": percentile_from_values(run_diff_per_game, run_diff_pg_values, True),
    }


def build_team_evaluation_summary(
    query: TeamEvaluationQuery,
    *,
    games_played: int,
    wins: int | None,
    losses: int | None,
    win_pct: float | None,
    hitting: dict[str, Any],
    pitching: dict[str, Any],
    fielding: dict[str, Any],
    overall_score: float,
    component_scores: dict[str, float],
    historical_context: dict[str, Any],
    roster_profile: dict[str, Any],
    drs_snapshot: dict[str, Any] | None,
) -> str:
    record_text = f"{wins}-{losses}" if wins is not None and losses is not None else "an unknown record"
    record_sentence = (
        f"{query.team.name} look {assessment_phrase(assessment_label(overall_score), query.tone)} so far. "
        f"They are {record_text} through {games_played} game(s)"
    )
    if win_pct is not None:
        record_sentence = f"{record_sentence} with a {win_pct:.3f} winning percentage."
    else:
        record_sentence = f"{record_sentence}."

    offense_sentence = (
        f"Offensively they have scored {safe_int(hitting.get('runs')) or 0} runs "
        f"({per_game_value(hitting.get('runs'), hitting.get('gamesPlayed')):.2f} per game), "
        f"with a {string_or_placeholder(hitting.get('ops'))} OPS and {safe_int(hitting.get('homeRuns')) or 0} home runs."
    )
    pitching_sentence = (
        f"On the mound they own a {string_or_placeholder(pitching.get('era'))} ERA, "
        f"{string_or_placeholder(pitching.get('whip'))} WHIP, and "
        f"{string_or_placeholder(pitching.get('strikeoutsPer9Inn'))} strikeouts per 9."
    )
    defense_sentence = (
        f"Defensively they are at {string_or_placeholder(fielding.get('fielding'))} fielding percentage "
        f"with {safe_int(fielding.get('errors')) or 0} errors."
    )
    if drs_snapshot and drs_snapshot.get("value") is not None:
        defense_sentence = (
            f"{defense_sentence} Their current team DRS is {format_float(drs_snapshot['value'], 1)}, "
            f"which ranks {drs_snapshot['rank']} of {drs_snapshot['team_count']} teams."
        )
    strength_area = max(component_scores, key=component_scores.get)
    weakness_area = min(component_scores, key=component_scores.get)
    angle_sentence = f"The strongest area right now is {strength_area}, while the biggest concern is {weakness_area}."
    roster_sentence = build_roster_sentence(roster_profile, query.focus)
    history_sentence = build_history_sentence(historical_context, query.focus)
    sample_sentence = (
        " The sample is still small, so treat this as an early-season read rather than a full-season verdict."
        if games_played < 25
        else ""
    )

    focus_sentences = {
        "offense": [record_sentence, offense_sentence, roster_sentence, history_sentence],
        "pitching": [record_sentence, pitching_sentence, roster_sentence, history_sentence],
        "rotation": [record_sentence, pitching_sentence, roster_sentence, history_sentence],
        "bullpen": [record_sentence, pitching_sentence, roster_sentence, history_sentence],
        "defense": [record_sentence, defense_sentence, roster_sentence, history_sentence],
    }
    sentences = focus_sentences.get(
        query.focus,
        [record_sentence, offense_sentence, pitching_sentence, defense_sentence, angle_sentence, roster_sentence, history_sentence],
    )
    return " ".join(sentence for sentence in sentences if sentence) + sample_sentence


def assessment_phrase(assessment: str, tone: str) -> str:
    if tone == "negative":
        if assessment in {"elite", "strong", "above average"}:
            return f"better than the question suggests; they have been {assessment}"
        return assessment
    if tone == "positive":
        if assessment in {"bad", "brutal", "below average"}:
            return f"weaker than a positive framing would suggest; they have been {assessment}"
        return assessment
    return assessment


def build_roster_sentence(roster_profile: dict[str, Any], focus: str) -> str:
    parts: list[str] = []
    top_hitters = roster_profile.get("top_hitters") or []
    top_starters = roster_profile.get("top_starters") or []
    top_relievers = roster_profile.get("top_relievers") or []
    average_age = roster_profile.get("average_age")

    if focus in {"overall", "offense"} and top_hitters:
        hitters_text = "; ".join(
            f"{player['name']} ({format_float(player['ops'], 3)} OPS, {player['plate_appearances']:.0f} PA)"
            for player in top_hitters
            if player.get("ops") is not None
        )
        if hitters_text:
            parts.append(f"Best lineup pieces so far: {hitters_text}.")
    if focus in {"overall", "pitching", "rotation"} and top_starters:
        starters_text = "; ".join(
            f"{player['name']} ({format_float(player['era'], 2)} ERA in {format_float(player['innings'], 1)} IP)"
            for player in top_starters
            if player.get("era") is not None
        )
        if starters_text:
            parts.append(f"Rotation anchors: {starters_text}.")
    if focus in {"overall", "pitching", "bullpen"} and top_relievers:
        relievers_text = "; ".join(
            f"{player['name']} ({format_float(player['era'], 2)} ERA in {format_float(player['innings'], 1)} IP)"
            for player in top_relievers[:2]
            if player.get("era") is not None
        )
        if relievers_text:
            parts.append(f"Best relief work so far: {relievers_text}.")
    if average_age is not None and focus == "overall":
        parts.append(f"The active roster's average age is {average_age:.1f}.")
    return " ".join(parts)


def build_history_sentence(historical_context: dict[str, Any], focus: str) -> str:
    mlb_history = historical_context.get("mlb_history") or {}
    franchise_history = historical_context.get("franchise_history") or {}
    if focus == "offense":
        percentile = mlb_history.get("runs_per_game_percentile")
        if percentile is not None:
            return f"By runs scored per game, this offense sits around the {ordinal_percentile(percentile)} percentile of imported MLB team-seasons."
        return ""
    if focus in {"pitching", "rotation", "bullpen"}:
        percentile = mlb_history.get("era_percentile")
        if percentile is not None:
            return f"By ERA, this staff sits around the {ordinal_percentile(percentile)} percentile of imported MLB team-seasons, with lower ERA treated as better."
        return ""
    if focus == "defense":
        percentile = mlb_history.get("fielding_pct_percentile")
        if percentile is not None:
            return f"By fielding percentage, this defense sits around the {ordinal_percentile(percentile)} percentile of imported MLB team-seasons."
        return ""

    parts: list[str] = []
    run_diff_percentile = mlb_history.get("run_diff_per_game_percentile")
    franchise_win_pct = franchise_history.get("win_pct_percentile")
    if run_diff_percentile is not None:
        parts.append(
            f"Their run-differential pace is around the {ordinal_percentile(run_diff_percentile)} percentile of imported MLB team-seasons."
        )
    if franchise_win_pct is not None:
        parts.append(
            f"Inside franchise history, their current winning-percentage pace is around the {ordinal_percentile(franchise_win_pct)} percentile."
        )
    return " ".join(parts)


def ordinal_percentile(value: float) -> str:
    rounded = int(round(value))
    if rounded <= 0:
        return "bottom"
    if 10 <= rounded % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(rounded % 10, "th")
    return f"{rounded}{suffix}"


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text in {".---", "-.--", "---"}:
        return None
    if text.startswith("."):
        text = f"0{text}"
    try:
        return float(text)
    except ValueError:
        return None


def safe_int(value: Any) -> int | None:
    converted = safe_float(value)
    return int(round(converted)) if converted is not None else None


def safe_ratio(numerator: Any, denominator: Any) -> float | None:
    numerator_value = safe_float(numerator)
    denominator_value = safe_float(denominator)
    if numerator_value is None or denominator_value in {None, 0.0}:
        return None
    return numerator_value / denominator_value


def per_game_value(value: Any, games: Any) -> float:
    return safe_ratio(value, games) or 0.0


def string_or_placeholder(value: Any) -> str:
    return str(value) if value not in {None, ""} else "unknown"


def format_float(value: Any, digits: int) -> str:
    converted = safe_float(value)
    if converted is None:
        return "unknown"
    return f"{converted:.{digits}f}"
