from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .config import Settings
from .models import EvidenceSnippet
from .query_utils import extract_name_candidates
from .storage import table_exists


CAREER_EARNINGS_TERMS = (
    "career earnings",
    "earned the most",
    "highest career salary",
    "highest career earnings",
    "highest-paid",
    "highest paid",
    "made the most money",
)
SALARY_TERMS = ("salary", "salaries", "contract", "contracts", "earnings", "money", "paid")
OFFENSE_TERMS = (
    "offensive",
    "offense",
    "production",
    "hits",
    "runs scored",
    "home runs",
    "homers",
    "rbi",
    "per hit",
    "per run",
    "per game",
    "per home run",
    "per homer",
    "per rbi",
)

COUNTRY_ALIASES = {
    "dominican": ("D.R.", "Dominican Republic"),
    "dominican born": ("D.R.", "Dominican Republic"),
    "dominican-born": ("D.R.", "Dominican Republic"),
    "puerto rican": ("P.R.", "Puerto Rico"),
    "puerto-rican": ("P.R.", "Puerto Rico"),
    "venezuelan": ("Venezuela",),
    "cuban": ("Cuba",),
    "mexican": ("México", "Mexico"),
    "japanese": ("Japan",),
    "korean": ("South Korea",),
}


@dataclass(slots=True)
class CareerEarningsQuery:
    country_filter: tuple[str, ...] | None
    country_label: str | None


@dataclass(slots=True)
class PlayerSalaryQuery:
    player_name: str


class SalaryRelationshipResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        earnings_query = parse_career_earnings_query(question)
        if earnings_query is not None:
            rows = fetch_career_earnings_rows(connection, earnings_query)
            if rows:
                summary = build_career_earnings_summary(rows, earnings_query)
                return EvidenceSnippet(
                    source="Salary Analysis",
                    title=f"{earnings_query.country_label or 'career'} earnings leaderboard",
                    citation="Lahman Salaries + People tables",
                    summary=summary,
                    payload={
                        "analysis_type": "career_earnings_leaderboard",
                        "mode": "historical",
                        "rows": rows,
                    },
                )

        player_query = parse_player_salary_query(question)
        if player_query is None:
            return None
        rows = fetch_player_salary_rows(connection, player_query.player_name)
        if not rows:
            return None
        summary, career_row = build_player_salary_summary(player_query.player_name, rows)
        return EvidenceSnippet(
            source="Salary Analysis",
            title=f"{player_query.player_name} salary vs offense",
            citation="Lahman Salaries + Batting tables",
            summary=summary,
            payload={
                "analysis_type": "player_salary_analysis",
                "mode": "historical",
                "player": player_query.player_name,
                "career": career_row,
                "rows": rows,
            },
        )


def parse_career_earnings_query(question: str) -> CareerEarningsQuery | None:
    lowered = question.lower()
    if not any(term in lowered for term in CAREER_EARNINGS_TERMS):
        return None
    for alias, values in COUNTRY_ALIASES.items():
        if alias in lowered:
            return CareerEarningsQuery(country_filter=values, country_label=alias.title())
    if "born player" in lowered or "born" in lowered:
        match = re.search(r"which\s+(.+?)\s+born\s+player", lowered)
        if match:
            label = match.group(1).strip().title()
            return CareerEarningsQuery(country_filter=(label,), country_label=label)
    return CareerEarningsQuery(country_filter=None, country_label=None)


def parse_player_salary_query(question: str) -> PlayerSalaryQuery | None:
    lowered = question.lower()
    if not any(term in lowered for term in SALARY_TERMS):
        return None
    if not any(term in lowered for term in OFFENSE_TERMS):
        return None
    direct_match = re.search(
        r"(?:use|analyze|break down)\s+([A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+)+)(?:'s)?\s+(?:contract|salary|earnings)",
        question,
        re.IGNORECASE,
    )
    if direct_match is not None:
        player_name = re.sub(r"^(?:use|analyze|break down)\s+", "", direct_match.group(1).strip(), flags=re.IGNORECASE)
        player_name = re.sub(r"'s\b", "", player_name, flags=re.IGNORECASE)
        return PlayerSalaryQuery(player_name=player_name)
    names = extract_name_candidates(question)
    if not names:
        return None
    return PlayerSalaryQuery(player_name=names[0])


def fetch_career_earnings_rows(connection, query: CareerEarningsQuery) -> list[dict[str, Any]]:
    if not (table_exists(connection, "lahman_salaries") and table_exists(connection, "lahman_people")):
        return []
    sql = """
        SELECT
            trim(coalesce(p.namefirst, '') || ' ' || coalesce(p.namelast, '')) AS player_name,
            p.birthcountry AS birthcountry,
            SUM(CAST(COALESCE(s.salary, '0') AS INTEGER)) AS career_earnings,
            MIN(CAST(COALESCE(s.yearid, '0') AS INTEGER)) AS first_year,
            MAX(CAST(COALESCE(s.yearid, '0') AS INTEGER)) AS last_year
        FROM lahman_salaries AS s
        JOIN lahman_people AS p
          ON p.playerid = s.playerid
    """
    parameters: list[Any] = []
    if query.country_filter:
        placeholders = ",".join("?" for _ in query.country_filter)
        sql += f" WHERE p.birthcountry IN ({placeholders})"
        parameters.extend(query.country_filter)
    sql += """
        GROUP BY s.playerid
        ORDER BY career_earnings DESC
        LIMIT 5
    """
    rows = connection.execute(sql, parameters).fetchall()
    return [
        {
            "player": str(row["player_name"]),
            "birthcountry": str(row["birthcountry"] or ""),
            "career_earnings": int(row["career_earnings"] or 0),
            "first_year": int(row["first_year"] or 0),
            "last_year": int(row["last_year"] or 0),
        }
        for row in rows
    ]


def fetch_player_salary_rows(connection, player_name: str) -> list[dict[str, Any]]:
    if not (table_exists(connection, "lahman_salaries") and table_exists(connection, "lahman_people") and table_exists(connection, "lahman_batting")):
        return []
    rows = connection.execute(
        """
        WITH target_player AS (
            SELECT playerid
            FROM lahman_people
            WHERE lower(trim(coalesce(namefirst, '') || ' ' || coalesce(namelast, ''))) = ?
            LIMIT 1
        ),
        yearly_salary AS (
            SELECT
                yearid,
                SUM(CAST(COALESCE(salary, '0') AS INTEGER)) AS salary
            FROM lahman_salaries
            WHERE playerid = (SELECT playerid FROM target_player)
            GROUP BY yearid
        ),
        yearly_batting AS (
            SELECT
                yearid,
                SUM(CAST(COALESCE(g, '0') AS INTEGER)) AS games,
                SUM(CAST(COALESCE(ab, '0') AS INTEGER)) AS at_bats,
                SUM(CAST(COALESCE(h, '0') AS INTEGER)) AS hits,
                SUM(CAST(COALESCE(c_2b, '0') AS INTEGER)) AS doubles,
                SUM(CAST(COALESCE(c_3b, '0') AS INTEGER)) AS triples,
                SUM(CAST(COALESCE(hr, '0') AS INTEGER)) AS home_runs,
                SUM(CAST(COALESCE(r, '0') AS INTEGER)) AS runs,
                SUM(CAST(COALESCE(rbi, '0') AS INTEGER)) AS runs_batted_in,
                SUM(CAST(COALESCE(bb, '0') AS INTEGER)) AS walks,
                SUM(CAST(COALESCE(hbp, '0') AS INTEGER)) AS hit_by_pitch,
                SUM(CAST(COALESCE(sf, '0') AS INTEGER)) AS sacrifice_flies
            FROM lahman_batting
            WHERE playerid = (SELECT playerid FROM target_player)
            GROUP BY yearid
        )
        SELECT
            CAST(s.yearid AS INTEGER) AS season,
            CAST(s.salary AS INTEGER) AS salary,
            CAST(COALESCE(b.games, 0) AS INTEGER) AS games,
            CAST(COALESCE(b.at_bats, 0) AS INTEGER) AS at_bats,
            CAST(COALESCE(b.hits, 0) AS INTEGER) AS hits,
            CAST(COALESCE(b.doubles, 0) AS INTEGER) AS doubles,
            CAST(COALESCE(b.triples, 0) AS INTEGER) AS triples,
            CAST(COALESCE(b.home_runs, 0) AS INTEGER) AS home_runs,
            CAST(COALESCE(b.runs, 0) AS INTEGER) AS runs,
            CAST(COALESCE(b.runs_batted_in, 0) AS INTEGER) AS runs_batted_in,
            CAST(COALESCE(b.walks, 0) AS INTEGER) AS walks,
            CAST(COALESCE(b.hit_by_pitch, 0) AS INTEGER) AS hit_by_pitch,
            CAST(COALESCE(b.sacrifice_flies, 0) AS INTEGER) AS sacrifice_flies
        FROM yearly_salary AS s
        LEFT JOIN yearly_batting AS b
          ON b.yearid = s.yearid
        ORDER BY season
        """,
        (player_name.lower(),),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        ops = compute_ops(
            int(row["hits"] or 0),
            int(row["doubles"] or 0),
            int(row["triples"] or 0),
            int(row["home_runs"] or 0),
            int(row["at_bats"] or 0),
            int(row["walks"] or 0),
            int(row["hit_by_pitch"] or 0),
            int(row["sacrifice_flies"] or 0),
        )
        salary = int(row["salary"] or 0)
        games = int(row["games"] or 0)
        hits = int(row["hits"] or 0)
        runs = int(row["runs"] or 0)
        result.append(
            {
                "season": int(row["season"] or 0),
                "salary": salary,
                "games": games,
                "hits": hits,
                "runs": runs,
                "home_runs": int(row["home_runs"] or 0),
                "runs_batted_in": int(row["runs_batted_in"] or 0),
                "ops": round(ops, 3) if ops is not None else None,
                "salary_per_game": round(salary / games, 2) if games else None,
                "salary_per_hit": round(salary / hits, 2) if hits else None,
                "salary_per_run": round(salary / runs, 2) if runs else None,
            }
        )
    return result


def build_career_earnings_summary(rows: list[dict[str, Any]], query: CareerEarningsQuery) -> str:
    leader = rows[0]
    label = f"{query.country_label} born" if query.country_label else "all tracked"
    trailing = "; ".join(f"{row['player']} ${row['career_earnings']:,}" for row in rows[1:4])
    summary = (
        f"Among {label} players in the loaded salary history, {leader['player']} has the highest career earnings "
        f"at ${leader['career_earnings']:,}, spanning {leader['first_year']}-{leader['last_year']}."
    )
    if trailing:
        summary = f"{summary} Next on the board: {trailing}."
    return summary


def build_player_salary_summary(player_name: str, rows: list[dict[str, Any]]) -> tuple[str, dict[str, Any]]:
    total_salary = sum(int(row["salary"] or 0) for row in rows)
    total_games = sum(int(row["games"] or 0) for row in rows)
    total_hits = sum(int(row["hits"] or 0) for row in rows)
    total_runs = sum(int(row["runs"] or 0) for row in rows)
    best_ops_season = max(rows, key=lambda row: row["ops"] if row["ops"] is not None else -1)
    highest_salary_season = max(rows, key=lambda row: row["salary"])
    career_row = {
        "salary": total_salary,
        "games": total_games,
        "hits": total_hits,
        "runs": total_runs,
        "salary_per_game": round(total_salary / total_games, 2) if total_games else None,
        "salary_per_hit": round(total_salary / total_hits, 2) if total_hits else None,
        "salary_per_run": round(total_salary / total_runs, 2) if total_runs else None,
    }
    summary = (
        f"Using public salary history as a contract proxy, {player_name} has earned ${total_salary:,} across the loaded seasons. "
        f"That works out to about {format_currency(career_row['salary_per_game'])} per game, "
        f"{format_currency(career_row['salary_per_hit'])} per hit, and {format_currency(career_row['salary_per_run'])} per run scored."
    )
    if best_ops_season.get("ops") is not None:
        summary = (
            f"{summary} Their best OPS season in the salary file was {best_ops_season['season']} at {best_ops_season['ops']}, "
            f"while their highest salary season was {highest_salary_season['season']} at ${highest_salary_season['salary']:,}."
        )
    return summary, career_row


def format_currency(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.0f}"


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
