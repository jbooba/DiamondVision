from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .pybaseball_adapter import load_lahman_managers, load_lahman_people, load_lahman_teams_core
from .query_utils import extract_explicit_year, normalize_person_name
from .storage import table_exists
from .team_evaluator import safe_int
from .team_season_compare import clean_team_phrase, resolve_team_season_reference


MANAGER_TERMS = ("manager", "managed", "skipper")
WINS_TERMS = ("wins", "won", "win total")
LOSSES_TERMS = ("losses", "lost", "loss total")
RECORD_TERMS = ("record",)
WIN_PCT_TERMS = ("winning percentage", "win percentage", "win pct", "winning pct")
QUESTION_FILLER_PATTERN = re.compile(
    r"\b(?:who|was|were|is|are|the|of|for|in|during|did|do|does|current|historically|historical)\b",
    re.IGNORECASE,
)
MANAGER_FILLER_PATTERN = re.compile(
    r"\b(?:manager|managed|skipper|head\s+coach|coach)\b",
    re.IGNORECASE,
)
TEAM_FACT_FILLER_PATTERN = re.compile(
    r"\b(?:wins?|won|loss(?:es)?|lost|record|winning|percentage|pct|total|have|had|did|how|many|what|were|was|the|of|for|in|during)\b",
    re.IGNORECASE,
)
YEAR_PATTERN = re.compile(r"\b(18\d{2}|19\d{2}|20\d{2})\b")


@dataclass(slots=True)
class HistoricalManagerQuery:
    season: int
    team_phrase: str


@dataclass(slots=True)
class HistoricalTeamFactQuery:
    season: int
    team_phrase: str
    fact: str


class HistoricalTeamFactsResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        manager_query = parse_historical_manager_query(question)
        if manager_query is not None:
            return self._build_manager_snippet(connection, manager_query)

        fact_query = parse_historical_team_fact_query(question)
        if fact_query is not None:
            return self._build_team_fact_snippet(connection, fact_query)
        return None

    def _resolve_reference(self, connection, team_phrase: str, season: int):
        current_season = self.settings.live_season or date.today().year
        reference = resolve_team_season_reference(
            connection,
            team_phrase,
            season,
            self.live_client,
            current_season,
        )
        if reference is None or reference.live_team is not None:
            return None
        return reference

    def _build_manager_snippet(self, connection, query: HistoricalManagerQuery) -> EvidenceSnippet | None:
        reference = self._resolve_reference(connection, query.team_phrase, query.season)
        if reference is None:
            return None
        manager_rows = load_local_manager_rows(connection, reference.season, reference.display_name, reference.team_code)
        citation = "Lahman Managers + People + Teams tables"
        if not manager_rows:
            manager_rows = load_pybaseball_manager_rows(reference.season, reference.display_name, reference.team_code)
            citation = "pybaseball Lahman managers()/people()/teams_core() helpers"
        if not manager_rows:
            return None

        summary = build_manager_summary(reference.display_name, manager_rows)
        return EvidenceSnippet(
            source="Historical Team Facts",
            title=f"{reference.display_name} manager lookup",
            citation=citation,
            summary=summary,
            payload={
                "analysis_type": "historical_manager_lookup",
                "mode": "historical",
                "season": reference.season,
                "team": reference.display_name,
                "rows": manager_rows,
            },
        )

    def _build_team_fact_snippet(self, connection, query: HistoricalTeamFactQuery) -> EvidenceSnippet | None:
        reference = self._resolve_reference(connection, query.team_phrase, query.season)
        if reference is None:
            return None
        team_row = load_local_team_fact_row(connection, reference.season, reference.display_name, reference.team_code)
        if not team_row:
            return None
        summary = build_team_fact_summary(reference.display_name, team_row, query.fact)
        return EvidenceSnippet(
            source="Historical Team Facts",
            title=f"{reference.display_name} team fact lookup",
            citation="Lahman Teams table",
            summary=summary,
            payload={
                "analysis_type": "historical_team_fact_lookup",
                "mode": "historical",
                "season": reference.season,
                "team": reference.display_name,
                "fact": query.fact,
                "rows": [team_row],
            },
        )


def parse_historical_manager_query(question: str) -> HistoricalManagerQuery | None:
    lowered = question.lower()
    if not any(term in lowered for term in MANAGER_TERMS):
        return None
    season = extract_explicit_year(question)
    if season is None:
        return None
    team_phrase = extract_team_phrase_from_manager_question(question, season)
    if not team_phrase:
        return None
    return HistoricalManagerQuery(season=season, team_phrase=team_phrase)


def parse_historical_team_fact_query(question: str) -> HistoricalTeamFactQuery | None:
    lowered = question.lower()
    season = extract_explicit_year(question)
    if season is None:
        return None
    fact = detect_team_fact(lowered)
    if fact is None:
        return None
    team_phrase = extract_team_phrase_from_fact_question(question, season)
    if not team_phrase:
        return None
    return HistoricalTeamFactQuery(season=season, team_phrase=team_phrase, fact=fact)


def detect_team_fact(lowered_question: str) -> str | None:
    if any(term in lowered_question for term in WIN_PCT_TERMS):
        return "win_pct"
    if any(term in lowered_question for term in RECORD_TERMS):
        return "record"
    if any(term in lowered_question for term in WINS_TERMS):
        return "wins"
    if any(term in lowered_question for term in LOSSES_TERMS):
        return "losses"
    return None


def extract_team_phrase_from_manager_question(question: str, season: int) -> str:
    cleaned = YEAR_PATTERN.sub(" ", question)
    cleaned = MANAGER_FILLER_PATTERN.sub(" ", cleaned)
    cleaned = QUESTION_FILLER_PATTERN.sub(" ", cleaned)
    cleaned = re.sub(r"[?.!,:'\"]", " ", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return clean_team_phrase(cleaned)


def extract_team_phrase_from_fact_question(question: str, season: int) -> str:
    cleaned = YEAR_PATTERN.sub(" ", question)
    cleaned = TEAM_FACT_FILLER_PATTERN.sub(" ", cleaned)
    cleaned = re.sub(r"[?.!,:'\"]", " ", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return clean_team_phrase(cleaned)


def load_local_manager_rows(connection, season: int, display_name: str, team_code: str | None) -> list[dict[str, Any]]:
    if not (table_exists(connection, "lahman_managers") and table_exists(connection, "lahman_people") and table_exists(connection, "lahman_teams")):
        return []
    team_name = display_name.split(" ", 1)[1] if " " in display_name else display_name
    rows = connection.execute(
        """
        SELECT
            p.namefirst,
            p.namelast,
            m.playerid,
            m.inseason,
            m.g,
            m.w,
            m.l,
            m.rank,
            m.plyrmgr,
            t.name
        FROM lahman_managers AS m
        JOIN lahman_people AS p
          ON p.playerid = m.playerid
        JOIN lahman_teams AS t
          ON t.yearid = m.yearid
         AND t.teamid = m.teamid
        WHERE CAST(m.yearid AS INTEGER) = ?
          AND (
            lower(t.name) = ?
            OR lower(t.teamidretro) = ?
          )
        ORDER BY CAST(COALESCE(m.inseason, '1') AS INTEGER), CAST(COALESCE(m.g, '0') AS INTEGER) DESC
        """,
        (season, team_name.lower(), (team_code or "").lower()),
    ).fetchall()
    return [
        {
            "manager": f"{row['namefirst']} {row['namelast']}".strip(),
            "games": safe_int(row["g"]) or 0,
            "wins": safe_int(row["w"]) or 0,
            "losses": safe_int(row["l"]) or 0,
            "finish": safe_int(row["rank"]),
            "inseason": safe_int(row["inseason"]) or 1,
            "player_manager": "Yes" if str(row["plyrmgr"] or "").upper() == "Y" else "No",
        }
        for row in rows
    ]


def load_local_team_fact_row(connection, season: int, display_name: str, team_code: str | None) -> dict[str, Any] | None:
    if not table_exists(connection, "lahman_teams"):
        return None
    team_name = display_name.split(" ", 1)[1] if " " in display_name else display_name
    row = connection.execute(
        """
        SELECT
            yearid,
            name,
            w,
            l,
            g,
            rank,
            divwin,
            wcwin,
            lgwin,
            wswin
        FROM lahman_teams
        WHERE CAST(yearid AS INTEGER) = ?
          AND (
            lower(name) = ?
            OR lower(teamidretro) = ?
            OR lower(teamid) = ?
            OR lower(franchid) = ?
          )
        ORDER BY
            CASE WHEN lower(name) = ? THEN 0 ELSE 1 END,
            CAST(COALESCE(w, '0') AS INTEGER) DESC,
            CAST(COALESCE(l, '0') AS INTEGER) ASC
        LIMIT 1
        """,
        (
            season,
            team_name.lower(),
            (team_code or "").lower(),
            (team_code or "").lower(),
            (team_code or "").lower(),
            team_name.lower(),
        ),
    ).fetchone()
    if row is None:
        return None
    wins = safe_int(row["w"]) or 0
    losses = safe_int(row["l"]) or 0
    games = safe_int(row["g"]) or (wins + losses)
    win_pct = (wins / (wins + losses)) if (wins + losses) else None
    return {
        "team": str(row["name"]),
        "season": safe_int(row["yearid"]) or season,
        "wins": wins,
        "losses": losses,
        "games": games,
        "win_pct": round(win_pct, 3) if win_pct is not None else None,
        "finish": safe_int(row["rank"]),
        "won_league": str(row["lgwin"] or "").upper() == "Y",
        "won_world_series": str(row["wswin"] or "").upper() == "Y",
    }


def load_pybaseball_manager_rows(season: int, display_name: str, team_code: str | None) -> list[dict[str, Any]]:
    managers = load_lahman_managers()
    people = load_lahman_people()
    teams = load_lahman_teams_core()
    if not managers or not people or not teams:
        return []

    people_by_id = {
        normalize_dict_keyed_value(row, "playerid", "playerID"): row
        for row in people
        if normalize_dict_keyed_value(row, "playerid", "playerID")
    }
    matching_team_ids: set[str] = set()
    target_team_name = display_name.split(" ", 1)[1] if " " in display_name else display_name
    for row in teams:
        row_season = safe_int(normalize_dict_keyed_value(row, "yearid", "yearID"))
        if row_season != season:
            continue
        aliases = {
            str(normalize_dict_keyed_value(row, "name") or "").strip().lower(),
            str(normalize_dict_keyed_value(row, "teamid", "teamID") or "").strip().lower(),
            str(normalize_dict_keyed_value(row, "teamidretro", "teamIDretro") or "").strip().lower(),
            str(normalize_dict_keyed_value(row, "franchid", "franchID") or "").strip().lower(),
        }
        if target_team_name.lower() in aliases or (team_code or "").lower() in aliases:
            value = str(normalize_dict_keyed_value(row, "teamid", "teamID") or "").strip()
            if value:
                matching_team_ids.add(value.casefold())
    if not matching_team_ids:
        return []

    matched_rows: list[dict[str, Any]] = []
    for row in managers:
        row_season = safe_int(normalize_dict_keyed_value(row, "yearid", "yearID"))
        row_team_id = str(normalize_dict_keyed_value(row, "teamid", "teamID") or "").strip().casefold()
        if row_season != season or row_team_id not in matching_team_ids:
            continue
        player_id = normalize_dict_keyed_value(row, "playerid", "playerID")
        person = people_by_id.get(player_id) if player_id else None
        manager_name = " ".join(
            part
            for part in (
                str(normalize_dict_keyed_value(person or {}, "namefirst", "nameFirst") or "").strip(),
                str(normalize_dict_keyed_value(person or {}, "namelast", "nameLast") or "").strip(),
            )
            if part
        ).strip()
        matched_rows.append(
            {
                "manager": manager_name or str(player_id or "").strip(),
                "games": safe_int(normalize_dict_keyed_value(row, "g", "G")) or 0,
                "wins": safe_int(normalize_dict_keyed_value(row, "w", "W")) or 0,
                "losses": safe_int(normalize_dict_keyed_value(row, "l", "L")) or 0,
                "finish": safe_int(normalize_dict_keyed_value(row, "rank", "Rank")),
                "inseason": safe_int(normalize_dict_keyed_value(row, "inseason", "inSeason")) or 1,
                "player_manager": "Yes"
                if str(normalize_dict_keyed_value(row, "plyrmgr", "plyrMgr") or "").upper() == "Y"
                else "No",
            }
        )
    matched_rows.sort(key=lambda row: (row.get("inseason") or 1, -(row.get("games") or 0)))
    return matched_rows


def normalize_dict_keyed_value(row: dict[str, Any], *keys: str) -> Any:
    if not row:
        return None
    lowered = {str(key).casefold(): value for key, value in row.items()}
    for key in keys:
        lookup = lowered.get(key.casefold())
        if lookup not in (None, ""):
            return lookup
    return None


def build_manager_summary(display_name: str, manager_rows: list[dict[str, Any]]) -> str:
    if len(manager_rows) == 1:
        row = manager_rows[0]
        rank_text = f" and finished {ordinal_suffix(row['finish'])}" if row.get("finish") else ""
        player_manager_text = " as a player-manager" if row.get("player_manager") == "Yes" else ""
        return (
            f"{row['manager']} managed the {display_name}{player_manager_text}. "
            f"They went {row['wins']}-{row['losses']} over {row['games']} games{rank_text}."
        )

    parts = []
    for row in manager_rows:
        descriptor = f"{row['wins']}-{row['losses']} in {row['games']} G"
        if row.get("player_manager") == "Yes":
            descriptor = f"{descriptor}, player-manager"
        parts.append(f"{row['manager']} ({descriptor})")
    return f"{display_name} used {len(manager_rows)} managers: " + "; ".join(parts) + "."


def build_team_fact_summary(display_name: str, team_row: dict[str, Any], fact: str) -> str:
    wins = team_row["wins"]
    losses = team_row["losses"]
    win_pct = team_row.get("win_pct")
    finish = team_row.get("finish")
    finish_text = f", finishing {ordinal_suffix(finish)}" if finish else ""
    postseason_bits = []
    if team_row.get("won_league"):
        postseason_bits.append("won the pennant")
    if team_row.get("won_world_series"):
        postseason_bits.append("won the World Series")
    postseason_text = f" They also {', and '.join(postseason_bits)}." if postseason_bits else ""
    if fact == "wins":
        return (
            f"The {display_name} won {wins} games. They finished {wins}-{losses}"
            f" ({format_win_pct(win_pct)}){finish_text}.{postseason_text}"
        )
    if fact == "losses":
        return (
            f"The {display_name} lost {losses} games. They finished {wins}-{losses}"
            f" ({format_win_pct(win_pct)}){finish_text}.{postseason_text}"
        )
    if fact == "win_pct":
        return (
            f"The {display_name} posted a {format_win_pct(win_pct)} winning percentage"
            f" with a {wins}-{losses} record{finish_text}.{postseason_text}"
        )
    return f"The {display_name} went {wins}-{losses} ({format_win_pct(win_pct)}){finish_text}.{postseason_text}"


def ordinal_suffix(value: int | None) -> str:
    if value is None:
        return ""
    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def format_win_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.3f}"
