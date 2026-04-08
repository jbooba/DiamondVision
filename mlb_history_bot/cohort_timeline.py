from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .manager_era_analysis import ManagerSeason, ManagerEraQuery, fetch_manager_seasons
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


@dataclass(slots=True, frozen=True)
class CohortFilter:
    kind: str
    label: str
    team_phrase: str | None = None
    manager_name: str | None = None
    country_filter: tuple[str, ...] | None = None


@dataclass(slots=True)
class ResolvedCohort:
    kind: str
    label: str
    seasons: tuple[int, ...]
    team_code: str | None = None
    team_name: str | None = None
    player_ids: set[str] | None = None
    country_filter: tuple[str, ...] | None = None


def parse_cohort_filter(question: str) -> CohortFilter | None:
    manager_filter = parse_manager_era_filter(question)
    if manager_filter is not None:
        return manager_filter
    return parse_birth_country_filter(question)


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
        player_ids = load_birth_country_player_ids(connection, cohort.country_filter or tuple())
        if not player_ids:
            return None
        return ResolvedCohort(
            kind="birth_country",
            label=cohort.label,
            seasons=tuple(),
            player_ids=player_ids,
            country_filter=cohort.country_filter,
        )
    return None


def load_birth_country_player_ids(connection, country_filter: tuple[str, ...]) -> set[str]:
    if not country_filter or not table_exists(connection, "lahman_people"):
        return set()
    placeholders = ",".join("?" for _ in country_filter)
    rows = connection.execute(
        f"""
        SELECT playerid
        FROM lahman_people
        WHERE COALESCE(playerid, '') <> ''
          AND birthcountry IN ({placeholders})
        """,
        tuple(country_filter),
    ).fetchall()
    return {str(row["playerid"]) for row in rows if str(row["playerid"] or "").strip()}


def clean_phrase(value: str) -> str:
    cleaned = value.strip(" ?.!,'\"")
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned


def strip_trailing_query_terms(value: str) -> str:
    return re.sub(r"\s+\b(?:by|using|with|for|in)\b\s+.+$", "", value, flags=re.IGNORECASE)
