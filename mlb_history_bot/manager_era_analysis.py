from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .models import EvidenceSnippet
from .pybaseball_adapter import load_batting_stats, load_fielding_stats
from .storage import table_exists


OFFENSE_HINTS = ("offensive", "offense", "hitting", "hitter", "batting")
DEFENSE_HINTS = ("defensive", "defense", "fielding", "fielder", "glove")
TEAM_MANAGER_PATTERNS = (
    re.compile(
        r"(?:for|on)\s+(?:the\s+)?(?P<team>[A-Za-z .'-]{2,60}?)\s+under\s+(?P<manager>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+)+)",
        re.IGNORECASE,
    ),
    re.compile(
        r"under\s+(?P<manager>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+)+)\s+(?:for|on)\s+(?:the\s+)?(?P<team>[A-Za-z .'-]{2,60})",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:for|on)\s+(?:the\s+)?(?P<team>[A-Za-z .'-]{2,60}?)\s+during\s+(?P<manager>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+)+)'s\s+tenure",
        re.IGNORECASE,
    ),
    re.compile(
        r"while\s+(?P<manager>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+)+)\s+managed\s+(?:the\s+)?(?P<team>[A-Za-z .'-]{2,60})",
        re.IGNORECASE,
    ),
)


@dataclass(slots=True)
class ManagerEraQuery:
    team_phrase: str
    manager_name: str
    focus: str


@dataclass(slots=True)
class ManagerSeason:
    season: int
    team_name: str
    team_code_bref: str
    wins: int
    losses: int


class ManagerEraAnalysisResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_manager_era_query(question)
        if query is None:
            return None
        manager_seasons = fetch_manager_seasons(connection, query)
        if not manager_seasons:
            return None

        if query.focus == "offense":
            leaders = build_offense_leaderboard(manager_seasons)
            if not leaders:
                return None
            summary = build_offense_summary(query, manager_seasons, leaders)
            analysis_type = "manager_era_offense"
        else:
            leaders = build_defense_leaderboard(manager_seasons)
            if not leaders:
                return None
            summary = build_defense_summary(query, manager_seasons, leaders)
            analysis_type = "manager_era_defense"

        return EvidenceSnippet(
            source="Manager Era Analysis",
            title=f"{query.team_phrase} under {query.manager_name}",
            citation="Lahman managers/team history plus pybaseball FanGraphs batting/fielding leaderboards",
            summary=summary,
            payload={
                "analysis_type": analysis_type,
                "mode": "historical",
                "team": manager_seasons[0].team_name,
                "manager": query.manager_name,
                "seasons": [season.season for season in manager_seasons],
                "rows": leaders,
            },
        )


def parse_manager_era_query(question: str) -> ManagerEraQuery | None:
    lowered = question.lower()
    focus = "offense" if any(token in lowered for token in OFFENSE_HINTS) else "defense" if any(token in lowered for token in DEFENSE_HINTS) else None
    if focus is None or not any(token in lowered for token in ("under ", " during ", " while ")):
        return None
    for pattern in TEAM_MANAGER_PATTERNS:
        match = pattern.search(question)
        if match is None:
            continue
        team_phrase = clean_phrase(match.group("team"))
        manager_name = clean_phrase(match.group("manager"))
        if team_phrase and manager_name:
            return ManagerEraQuery(team_phrase=team_phrase, manager_name=manager_name, focus=focus)
    return None


def fetch_manager_seasons(connection, query: ManagerEraQuery) -> list[ManagerSeason]:
    if not (table_exists(connection, "lahman_managers") and table_exists(connection, "lahman_people") and table_exists(connection, "lahman_teams")):
        return []
    rows = connection.execute(
        """
        SELECT
            CAST(m.yearid AS INTEGER) AS season,
            t.name AS team_name,
            t.teamidbr AS team_code_bref,
            CAST(COALESCE(m.w, '0') AS INTEGER) AS wins,
            CAST(COALESCE(m.l, '0') AS INTEGER) AS losses
        FROM lahman_managers AS m
        JOIN lahman_people AS p
          ON p.playerid = m.playerid
        JOIN lahman_teams AS t
          ON t.yearid = m.yearid
         AND t.teamid = m.teamid
        WHERE lower(trim(coalesce(p.namefirst, '') || ' ' || coalesce(p.namelast, ''))) = ?
          AND (
            lower(t.name) = ?
            OR lower(t.name) LIKE ?
            OR lower(t.teamidbr) = ?
            OR lower(t.teamidretro) = ?
            OR lower(t.franchid) = ?
          )
        ORDER BY CAST(m.yearid AS INTEGER)
        """,
        (
            query.manager_name.lower(),
            query.team_phrase.lower(),
            f"%{query.team_phrase.lower()}%",
            query.team_phrase.lower(),
            query.team_phrase.lower(),
            query.team_phrase.lower(),
        ),
    ).fetchall()
    return [
        ManagerSeason(
            season=int(row["season"]),
            team_name=str(row["team_name"]),
            team_code_bref=str(row["team_code_bref"] or ""),
            wins=int(row["wins"]),
            losses=int(row["losses"]),
        )
        for row in rows
        if str(row["team_code_bref"] or "").strip()
    ]


def build_offense_leaderboard(manager_seasons: list[ManagerSeason]) -> list[dict[str, Any]]:
    aggregates: dict[str, dict[str, Any]] = {}
    for season in manager_seasons:
        rows = load_batting_stats(season.season, season.season)
        for row in rows:
            if str(row.get("Team") or "").strip() != season.team_code_bref:
                continue
            name = str(row.get("Name") or "").strip()
            if not name:
                continue
            aggregate = aggregates.setdefault(
                name,
                {
                    "player": name,
                    "games": 0,
                    "plate_appearances": 0,
                    "at_bats": 0,
                    "hits": 0,
                    "doubles": 0,
                    "triples": 0,
                    "home_runs": 0,
                    "runs": 0,
                    "runs_batted_in": 0,
                    "walks": 0,
                    "hit_by_pitch": 0,
                    "sacrifice_flies": 0,
                    "wrc_plus_weight": 0.0,
                    "war": 0.0,
                },
            )
            pa = safe_int(row.get("PA")) or 0
            aggregate["games"] += safe_int(row.get("G")) or 0
            aggregate["plate_appearances"] += pa
            aggregate["at_bats"] += safe_int(row.get("AB")) or 0
            aggregate["hits"] += safe_int(row.get("H")) or 0
            aggregate["doubles"] += safe_int(row.get("2B")) or 0
            aggregate["triples"] += safe_int(row.get("3B")) or 0
            aggregate["home_runs"] += safe_int(row.get("HR")) or 0
            aggregate["runs"] += safe_int(row.get("R")) or 0
            aggregate["runs_batted_in"] += safe_int(row.get("RBI")) or 0
            aggregate["walks"] += safe_int(row.get("BB")) or 0
            aggregate["hit_by_pitch"] += safe_int(row.get("HBP")) or 0
            aggregate["sacrifice_flies"] += safe_int(row.get("SF")) or 0
            aggregate["wrc_plus_weight"] += (safe_float(row.get("wRC+")) or 0.0) * pa
            aggregate["war"] += safe_float(row.get("WAR")) or 0.0
    leaders: list[dict[str, Any]] = []
    for aggregate in aggregates.values():
        if aggregate["plate_appearances"] < 100:
            continue
        ops = compute_ops(
            aggregate["hits"],
            aggregate["doubles"],
            aggregate["triples"],
            aggregate["home_runs"],
            aggregate["at_bats"],
            aggregate["walks"],
            aggregate["hit_by_pitch"],
            aggregate["sacrifice_flies"],
        )
        leaders.append(
            {
                "player": aggregate["player"],
                "games": aggregate["games"],
                "plate_appearances": aggregate["plate_appearances"],
                "hits": aggregate["hits"],
                "home_runs": aggregate["home_runs"],
                "runs": aggregate["runs"],
                "runs_batted_in": aggregate["runs_batted_in"],
                "ops": round(ops, 3) if ops is not None else None,
                "wrc_plus": round(aggregate["wrc_plus_weight"] / aggregate["plate_appearances"], 1)
                if aggregate["plate_appearances"]
                else None,
                "war": round(aggregate["war"], 1),
            }
        )
    leaders.sort(key=lambda item: ((item["wrc_plus"] or 0.0), (item["ops"] or 0.0), item["plate_appearances"]), reverse=True)
    return leaders[:5]


def build_defense_leaderboard(manager_seasons: list[ManagerSeason]) -> list[dict[str, Any]]:
    aggregates: dict[str, dict[str, Any]] = {}
    for season in manager_seasons:
        rows = load_fielding_stats(season.season, season.season)
        for row in rows:
            if str(row.get("Team") or "").strip() != season.team_code_bref:
                continue
            name = str(row.get("Name") or "").strip()
            if not name:
                continue
            aggregate = aggregates.setdefault(
                name,
                {
                    "player": name,
                    "games": 0.0,
                    "innings": 0.0,
                    "drs": 0.0,
                    "def": 0.0,
                    "oaa": 0.0,
                    "positions": set(),
                },
            )
            aggregate["games"] += safe_float(row.get("G")) or 0.0
            aggregate["innings"] += safe_float(row.get("Inn")) or 0.0
            aggregate["drs"] += safe_float(row.get("DRS")) or 0.0
            aggregate["def"] += safe_float(row.get("Def")) or 0.0
            aggregate["oaa"] += safe_float(row.get("OAA")) or 0.0
            if row.get("Pos"):
                aggregate["positions"].add(str(row.get("Pos")))
    leaders: list[dict[str, Any]] = []
    for aggregate in aggregates.values():
        if aggregate["games"] < 20:
            continue
        leaders.append(
            {
                "player": aggregate["player"],
                "positions": ", ".join(sorted(aggregate["positions"])),
                "games": int(round(aggregate["games"])),
                "innings": round(aggregate["innings"], 1),
                "drs": round(aggregate["drs"], 1),
                "def": round(aggregate["def"], 1),
                "oaa": round(aggregate["oaa"], 1),
            }
        )
    leaders.sort(key=lambda item: ((item["drs"] or 0.0), (item["def"] or 0.0), (item["oaa"] or 0.0), item["innings"]), reverse=True)
    return leaders[:5]


def build_offense_summary(query: ManagerEraQuery, manager_seasons: list[ManagerSeason], leaders: list[dict[str, Any]]) -> str:
    leader = leaders[0]
    span = format_manager_span(manager_seasons)
    trailing = "; ".join(
        f"{row['player']} ({row['wrc_plus']} wRC+, {row['ops']} OPS)"
        for row in leaders[1:4]
    )
    summary = (
        f"Using public batting production across {span}, {leader['player']} was the strongest offensive player for "
        f"the {manager_seasons[0].team_name} under {query.manager_name}. "
        f"They put up a {leader['wrc_plus']} wRC+ and {leader['ops']} OPS over {leader['plate_appearances']} PA, "
        f"with {leader['home_runs']} HR and {leader['runs_batted_in']} RBI."
    )
    if trailing:
        summary = f"{summary} Next on the board: {trailing}."
    return summary


def build_defense_summary(query: ManagerEraQuery, manager_seasons: list[ManagerSeason], leaders: list[dict[str, Any]]) -> str:
    leader = leaders[0]
    span = format_manager_span(manager_seasons)
    trailing = "; ".join(
        f"{row['player']} ({row['drs']} DRS, {row['def']} Def)"
        for row in leaders[1:4]
    )
    summary = (
        f"Using public FanGraphs/Statcast fielding measures across {span}, {leader['player']} rated as the strongest "
        f"defensive player for the {manager_seasons[0].team_name} under {query.manager_name}. "
        f"They logged {leader['drs']} DRS, {leader['def']} Def, and {leader['oaa']} OAA in {leader['games']} games."
    )
    if trailing:
        summary = f"{summary} Next on the board: {trailing}."
    return summary


def format_manager_span(manager_seasons: list[ManagerSeason]) -> str:
    seasons = [season.season for season in manager_seasons]
    if len(seasons) == 1:
        return str(seasons[0])
    return f"{seasons[0]}-{seasons[-1]}"


def clean_phrase(value: str) -> str:
    cleaned = value.strip(" ?.!,'\"")
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> int | None:
    numeric = safe_float(value)
    return int(numeric) if numeric is not None else None


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
    obp_denominator = at_bats + walks + hit_by_pitch + sacrifice_flies
    if obp_denominator <= 0:
        return None
    obp = (hits + walks + hit_by_pitch) / obp_denominator
    slg = (singles + 2 * doubles + 3 * triples + 4 * home_runs) / at_bats
    return obp + slg
