from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .manager_era_analysis import ManagerSeason, ManagerEraQuery, fetch_manager_seasons
from .pybaseball_adapter import load_all_star_full, load_all_star_game_logs
from .salary_relationships import COUNTRY_ALIASES
from .storage import table_exists


TEAM_MANAGER_PATTERNS = (
    re.compile(
        r"(?:for|on)\s+(?:the\s+)?(?P<team>[A-Za-z .'-]{2,60}?)\s+under\s+(?P<manager>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+)*)",
        re.IGNORECASE,
    ),
    re.compile(
        r"under\s+(?P<manager>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+)*)\s+(?:for|on)\s+(?:the\s+)?(?P<team>[A-Za-z .'-]{2,60})",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:for|on)\s+(?:the\s+)?(?P<team>[A-Za-z .'-]{2,60}?)\s+during\s+(?P<manager>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+)*)'s\s+tenure",
        re.IGNORECASE,
    ),
    re.compile(
        r"while\s+(?P<manager>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+)*)\s+managed\s+(?:the\s+)?(?P<team>[A-Za-z .'-]{2,60})",
        re.IGNORECASE,
    ),
)
COUNTRY_BORN_PATTERN = re.compile(r"\b([A-Za-z][A-Za-z -]{1,40})-born\b", re.IGNORECASE)
LEFT_HANDED_HITTER_PATTERN = re.compile(r"\bleft[- ]handed\s+(?:hitters?|batters?|sluggers?)\b|\blefties?\s+at\s+the\s+plate\b", re.IGNORECASE)
RIGHT_HANDED_HITTER_PATTERN = re.compile(r"\bright[- ]handed\s+(?:hitters?|batters?|sluggers?)\b|\brighties?\s+at\s+the\s+plate\b", re.IGNORECASE)
SWITCH_HITTER_PATTERN = re.compile(r"\bswitch[- ]hitters?\b", re.IGNORECASE)
LEFT_HANDED_PITCHER_PATTERN = re.compile(r"\bleft[- ]handed\s+(?:pitchers?|starters?|relievers?)\b|\blefties?\s+on\s+the\s+mound\b", re.IGNORECASE)
RIGHT_HANDED_PITCHER_PATTERN = re.compile(r"\bright[- ]handed\s+(?:pitchers?|starters?|relievers?)\b|\brighties?\s+on\s+the\s+mound\b", re.IGNORECASE)
HALL_OF_FAME_PATTERN = re.compile(r"\bhall(?:\s+of\s+fame|[- ]of[- ]fame)?\s+(?:players?|hitters?|pitchers?|fielders?|famers?)\b|\bhall[- ]of[- ]famers?\b", re.IGNORECASE)
ALL_STAR_PATTERN = re.compile(
    r"\ball[- ]stars?\b|\ball[- ]star(?:\s+(?:team|teams|selection|selections|game|games))?\b",
    re.IGNORECASE,
)


@dataclass(slots=True, frozen=True)
class CohortFilter:
    kind: str
    label: str
    team_phrase: str | None = None
    manager_name: str | None = None
    country_filter: tuple[str, ...] | None = None
    bats_filter: tuple[str, ...] | None = None
    throws_filter: tuple[str, ...] | None = None


@dataclass(slots=True)
class ResolvedCohort:
    kind: str
    label: str
    seasons: tuple[int, ...]
    team_code: str | None = None
    team_name: str | None = None
    player_ids: set[str] | None = None
    player_names: set[str] | None = None
    country_filter: tuple[str, ...] | None = None
    bats_filter: tuple[str, ...] | None = None
    throws_filter: tuple[str, ...] | None = None


def parse_cohort_filter(question: str) -> CohortFilter | None:
    manager_filter = parse_manager_era_filter(question)
    if manager_filter is not None:
        return manager_filter
    for parser in (
        parse_birth_country_filter,
        parse_handedness_filter,
        parse_all_star_filter,
        parse_hall_of_fame_filter,
    ):
        cohort = parser(question)
        if cohort is not None:
            return cohort
    return None


def parse_manager_era_filter(question: str) -> CohortFilter | None:
    for pattern in TEAM_MANAGER_PATTERNS:
        match = pattern.search(question)
        if match is None:
            continue
        team_phrase = clean_phrase(strip_trailing_query_terms(match.group("team")))
        manager_name = clean_phrase(strip_trailing_query_terms(match.group("manager")))
        if team_phrase and manager_name:
            return CohortFilter(
                kind="manager_era",
                label=f"{team_phrase} under {manager_name}",
                team_phrase=team_phrase,
                manager_name=manager_name,
            )
    return None


def parse_birth_country_filter(question: str) -> CohortFilter | None:
    lowered = question.lower()
    for alias, values in COUNTRY_ALIASES.items():
        if alias in lowered:
            return CohortFilter(
                kind="birth_country",
                label=f"{alias.title()} players",
                country_filter=values,
            )
    match = COUNTRY_BORN_PATTERN.search(question)
    if match is None:
        return None
    label = clean_phrase(match.group(1))
    if not label:
        return None
    return CohortFilter(
        kind="birth_country",
        label=f"{label.title()}-born players",
        country_filter=(label.title(),),
    )


def parse_handedness_filter(question: str) -> CohortFilter | None:
    if SWITCH_HITTER_PATTERN.search(question):
        # Lahman encodes switch-hitters as B ("both"), but some external sources use S.
        return CohortFilter(kind="bat_handedness", label="switch-hitters", bats_filter=("B", "S"))
    if LEFT_HANDED_HITTER_PATTERN.search(question):
        return CohortFilter(kind="bat_handedness", label="left-handed hitters", bats_filter=("L",))
    if RIGHT_HANDED_HITTER_PATTERN.search(question):
        return CohortFilter(kind="bat_handedness", label="right-handed hitters", bats_filter=("R",))
    if LEFT_HANDED_PITCHER_PATTERN.search(question):
        return CohortFilter(kind="throw_handedness", label="left-handed pitchers", throws_filter=("L",))
    if RIGHT_HANDED_PITCHER_PATTERN.search(question):
        return CohortFilter(kind="throw_handedness", label="right-handed pitchers", throws_filter=("R",))
    return None


def parse_hall_of_fame_filter(question: str) -> CohortFilter | None:
    if HALL_OF_FAME_PATTERN.search(question) is None:
        return None
    return CohortFilter(kind="hall_of_fame", label="Hall of Famers")


def parse_all_star_filter(question: str) -> CohortFilter | None:
    if ALL_STAR_PATTERN.search(question) is None:
        return None
    return CohortFilter(kind="all_star", label="All-Stars")


def resolve_cohort_filter(connection, cohort: CohortFilter) -> ResolvedCohort | None:
    if cohort.kind == "manager_era":
        manager_query = ManagerEraQuery(
            team_phrase=cohort.team_phrase or "",
            manager_name=cohort.manager_name or "",
            focus="offense",
        )
        seasons = fetch_manager_seasons(connection, manager_query)
        if not seasons:
            return None
        season_values = tuple(sorted(season.season for season in seasons))
        return ResolvedCohort(
            kind="manager_era",
            label=cohort.label,
            seasons=season_values,
            team_code=seasons[0].team_id,
            team_name=seasons[0].team_name,
        )
    if cohort.kind == "birth_country":
        player_ids, player_names = load_birth_country_people(connection, cohort.country_filter or tuple())
        if not player_ids:
            return None
        return ResolvedCohort(
            kind="birth_country",
            label=cohort.label,
            seasons=tuple(),
            player_ids=player_ids,
            player_names=player_names,
            country_filter=cohort.country_filter,
        )
    if cohort.kind == "bat_handedness":
        player_ids, player_names = load_people_filter_identities(connection, bats_filter=cohort.bats_filter)
        if not player_ids:
            return None
        return ResolvedCohort(
            kind="bat_handedness",
            label=cohort.label,
            seasons=tuple(),
            player_ids=player_ids,
            player_names=player_names,
            bats_filter=cohort.bats_filter,
        )
    if cohort.kind == "throw_handedness":
        player_ids, player_names = load_people_filter_identities(connection, throws_filter=cohort.throws_filter)
        if not player_ids:
            return None
        return ResolvedCohort(
            kind="throw_handedness",
            label=cohort.label,
            seasons=tuple(),
            player_ids=player_ids,
            player_names=player_names,
            throws_filter=cohort.throws_filter,
        )
    if cohort.kind == "hall_of_fame":
        player_ids, player_names = load_hall_of_fame_identities(connection)
        if not player_ids:
            return None
        return ResolvedCohort(
            kind="hall_of_fame",
            label=cohort.label,
            seasons=tuple(),
            player_ids=player_ids,
            player_names=player_names,
        )
    if cohort.kind == "all_star":
        player_ids, player_names = load_all_star_identities(connection)
        if not player_ids and not player_names:
            return None
        return ResolvedCohort(
            kind="all_star",
            label=cohort.label,
            seasons=tuple(),
            player_ids=player_ids,
            player_names=player_names,
        )
    return None


def load_birth_country_people(connection, country_filter: tuple[str, ...]) -> tuple[set[str], set[str]]:
    if not country_filter or not table_exists(connection, "lahman_people"):
        return set(), set()
    placeholders = ",".join("?" for _ in country_filter)
    rows = connection.execute(
        f"""
        SELECT playerid, namefirst, namelast
        FROM lahman_people
        WHERE COALESCE(playerid, '') <> ''
          AND birthcountry IN ({placeholders})
        """,
        tuple(country_filter),
    ).fetchall()
    return extract_identity_sets(rows)


def load_people_filter_identities(
    connection,
    *,
    bats_filter: tuple[str, ...] | None = None,
    throws_filter: tuple[str, ...] | None = None,
) -> tuple[set[str], set[str]]:
    if not table_exists(connection, "lahman_people"):
        return set(), set()
    clauses = ["COALESCE(playerid, '') <> ''"]
    parameters: list[str] = []
    if bats_filter:
        placeholders = ",".join("?" for _ in bats_filter)
        clauses.append(f"bats IN ({placeholders})")
        parameters.extend(bats_filter)
    if throws_filter:
        placeholders = ",".join("?" for _ in throws_filter)
        clauses.append(f"throws IN ({placeholders})")
        parameters.extend(throws_filter)
    rows = connection.execute(
        f"""
        SELECT playerid, namefirst, namelast
        FROM lahman_people
        WHERE {' AND '.join(clauses)}
        """,
        tuple(parameters),
    ).fetchall()
    return extract_identity_sets(rows)


def load_hall_of_fame_identities(connection) -> tuple[set[str], set[str]]:
    if not (table_exists(connection, "lahman_halloffame") and table_exists(connection, "lahman_people")):
        return set(), set()
    rows = connection.execute(
        """
        SELECT DISTINCT hof.playerid, ppl.namefirst, ppl.namelast
        FROM lahman_halloffame AS hof
        JOIN lahman_people AS ppl
          ON ppl.playerid = hof.playerid
        WHERE lower(COALESCE(hof.inducted, '')) = 'y'
          AND COALESCE(hof.playerid, '') <> ''
        """
    ).fetchall()
    return extract_identity_sets(rows)


def load_all_star_identities(connection) -> tuple[set[str], set[str]]:
    if table_exists(connection, "lahman_allstarfull") and table_exists(connection, "lahman_people"):
        rows = connection.execute(
            """
            SELECT DISTINCT ast.playerid, ppl.namefirst, ppl.namelast
            FROM lahman_allstarfull AS ast
            JOIN lahman_people AS ppl
              ON ppl.playerid = ast.playerid
            WHERE COALESCE(ast.playerid, '') <> ''
            """
        ).fetchall()
        player_ids, player_names = extract_identity_sets(rows)
        if player_ids or player_names:
            return player_ids, player_names

    full_rows = load_all_star_full()
    player_ids, player_names = extract_all_star_full_identities(full_rows)
    if player_ids or player_names:
        resolved_ids, resolved_names = lookup_people_identities_by_names(connection, player_names)
        player_ids.update(resolved_ids)
        player_names.update(resolved_names)
        return player_ids, player_names

    log_rows = load_all_star_game_logs()
    if not log_rows:
        return set(), set()
    names = extract_all_star_game_log_names(log_rows)
    return lookup_people_identities_by_names(connection, names)


def extract_all_star_full_identities(rows: list[dict[str, Any]]) -> tuple[set[str], set[str]]:
    player_ids: set[str] = set()
    player_names: set[str] = set()
    for row in rows:
        normalized = {str(key).lower(): value for key, value in row.items()}
        player_id = str(normalized.get("playerid") or normalized.get("player_id") or "").strip()
        if player_id:
            player_ids.add(player_id)
        first_name = str(normalized.get("namefirst") or "").strip()
        last_name = str(normalized.get("namelast") or "").strip()
        if first_name or last_name:
            player_names.add(" ".join(part for part in (first_name, last_name) if part).lower())
            continue
        full_name = str(
            normalized.get("playername")
            or normalized.get("player_name")
            or normalized.get("fullname")
            or normalized.get("full_name")
            or ""
        ).strip()
        if full_name:
            player_names.add(full_name.lower())
    return player_ids, player_names


def extract_all_star_game_log_names(rows: list[dict[str, Any]]) -> set[str]:
    names: set[str] = set()
    for row in rows:
        for key, value in row.items():
            lower_key = str(key).lower()
            if not lower_key.endswith("_name"):
                continue
            if lower_key.startswith("ump_") or "manager" in lower_key:
                continue
            text = str(value or "").strip()
            if not text or text == "(none)":
                continue
            names.add(text.lower())
    return names


def lookup_people_identities_by_names(connection, names: set[str]) -> tuple[set[str], set[str]]:
    if not names or not table_exists(connection, "lahman_people"):
        return set(), set(names)
    placeholders = ",".join("?" for _ in names)
    rows = connection.execute(
        f"""
        SELECT DISTINCT playerid, namefirst, namelast
        FROM lahman_people
        WHERE lower(trim(coalesce(namefirst, '') || ' ' || coalesce(namelast, ''))) IN ({placeholders})
        """,
        tuple(sorted(names)),
    ).fetchall()
    player_ids, player_names = extract_identity_sets(rows)
    unresolved_names = {name for name in names if name not in player_names}
    return player_ids, player_names | unresolved_names


def extract_identity_sets(rows) -> tuple[set[str], set[str]]:
    player_ids: set[str] = set()
    player_names: set[str] = set()
    for row in rows:
        player_id = str(row["playerid"] or "").strip()
        if player_id:
            player_ids.add(player_id)
        name = " ".join(part for part in (str(row["namefirst"] or "").strip(), str(row["namelast"] or "").strip()) if part)
        if name:
            player_names.add(name.lower())
    return player_ids, player_names


def clean_phrase(value: str) -> str:
    cleaned = value.strip(" ?.!,'\"")
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned


def strip_trailing_query_terms(value: str) -> str:
    return re.sub(r"\s+\b(?:by|using|with|for|in)\b\s+.+$", "", value, flags=re.IGNORECASE)
