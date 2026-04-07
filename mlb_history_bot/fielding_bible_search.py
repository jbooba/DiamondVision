from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .fielding_bible import FieldingBibleClient
from .models import EvidenceSnippet
from .query_utils import question_mentions_specific_date_reference
from .storage import fetch_rows, latest_snapshot_at, table_exists


DRS_HINTS = {"drs", "defensive runs saved"}
SINGLE_GAME_HINTS = {
    "single-game",
    "single game",
    "tonight",
    "today",
    "in one game",
    "in a game",
    "game tonight",
}

TEAM_HINT_PATTERN = re.compile(r"\bteam(?:s)?\b|\bclub(?:s)?\b", re.IGNORECASE)
CURRENT_DRS_PATTERN = re.compile(
    r"\b(current|latest|leaderboard|leaders|leading|lead(?:s|ing)?|this season|right now)\b",
    re.IGNORECASE,
)

DRS_COMPONENT_LABELS = [
    ("art", "ART/rPM"),
    ("gfpdm", "GFP/DM"),
    ("gdp", "rGDP"),
    ("bunt", "rBU"),
    ("of_arm", "rARM"),
    ("sb", "rSB"),
    ("sz", "Strike Zone"),
    ("adj_er", "Adj ER"),
]


@dataclass(slots=True)
class PositionRequest:
    description: str
    primary_labels: tuple[str, ...]
    fallback_labels: tuple[str, ...] = ()


@dataclass(slots=True)
class ComponentRequest:
    metric_name: str
    column_name: str | None
    display_label: str
    aliases: tuple[str, ...]
    notes: str = ""
    default_position_request: PositionRequest | None = None


POSITION_REQUESTS: list[tuple[tuple[str, ...], PositionRequest]] = [
    (("shortstop", "shortstops"), PositionRequest("shortstops", ("SS",))),
    (("second baseman", "second basemen", "second base"), PositionRequest("second basemen", ("2B",))),
    (("third baseman", "third basemen", "third base"), PositionRequest("third basemen", ("3B",))),
    (("first baseman", "first basemen", "first base"), PositionRequest("first basemen", ("1B",))),
    (("left fielder", "left fielders", "left field"), PositionRequest("left fielders", ("LF",))),
    (("center fielder", "center fielders", "center field"), PositionRequest("center fielders", ("CF",))),
    (("right fielder", "right fielders", "right field"), PositionRequest("right fielders", ("RF",))),
    (("catcher", "catchers"), PositionRequest("catchers", ("C",))),
    (("pitcher", "pitchers"), PositionRequest("pitchers", ("P",))),
    (("infielder", "infielders", "infield"), PositionRequest("infielders", ("IF",), ("1B", "2B", "3B", "SS"))),
    (("outfielder", "outfielders", "outfield"), PositionRequest("outfielders", ("OF",), ("LF", "CF", "RF"))),
]

POSITION_ABBREVIATIONS: list[tuple[re.Pattern[str], PositionRequest]] = [
    (re.compile(r"\b1b\b", re.IGNORECASE), PositionRequest("first basemen", ("1B",))),
    (re.compile(r"\b2b\b", re.IGNORECASE), PositionRequest("second basemen", ("2B",))),
    (re.compile(r"\b3b\b", re.IGNORECASE), PositionRequest("third basemen", ("3B",))),
    (re.compile(r"\bss\b", re.IGNORECASE), PositionRequest("shortstops", ("SS",))),
    (re.compile(r"\blf\b", re.IGNORECASE), PositionRequest("left fielders", ("LF",))),
    (re.compile(r"\bcf\b", re.IGNORECASE), PositionRequest("center fielders", ("CF",))),
    (re.compile(r"\brf\b", re.IGNORECASE), PositionRequest("right fielders", ("RF",))),
    (re.compile(r"\bif\b", re.IGNORECASE), PositionRequest("infielders", ("IF",), ("1B", "2B", "3B", "SS"))),
    (re.compile(r"\bof\b", re.IGNORECASE), PositionRequest("outfielders", ("OF",), ("LF", "CF", "RF"))),
]

SUPPORTED_COMPONENT_REQUESTS: tuple[ComponentRequest, ...] = (
    ComponentRequest(
        metric_name="rPM",
        column_name="art",
        display_label="ART/rPM",
        aliases=("rpm", "plus/minus", "plus minus", "plus/minus runs saved", "part system"),
    ),
    ComponentRequest(
        metric_name="rARM",
        column_name="of_arm",
        display_label="rARM",
        aliases=("rarm", "outfield arm runs saved"),
        default_position_request=PositionRequest("outfielders", ("OF",), ("LF", "CF", "RF")),
    ),
    ComponentRequest(
        metric_name="rSB",
        column_name="sb",
        display_label="rSB",
        aliases=("rsb", "stolen base runs saved"),
        default_position_request=PositionRequest("catchers and pitchers", ("C", "P")),
    ),
    ComponentRequest(
        metric_name="rGDP",
        column_name="gdp",
        display_label="rGDP",
        aliases=("rgdp", "double play runs saved", "double-play runs saved"),
        default_position_request=PositionRequest("infielders", ("IF",), ("1B", "2B", "3B", "SS")),
    ),
    ComponentRequest(
        metric_name="rBU",
        column_name="bunt",
        display_label="rBU",
        aliases=("rbu", "bunt runs saved"),
    ),
)

UNSUPPORTED_COMPONENT_REQUESTS: tuple[ComponentRequest, ...] = (
    ComponentRequest(
        metric_name="rHR",
        column_name=None,
        display_label="rHR",
        aliases=(
            "rhr",
            "home run robbery",
            "home run robberies",
            "home run robbing",
            "robbed home run",
            "robbed home runs",
        ),
        notes=(
            "The synced public Fielding Bible/SIS DRS feed does not expose the home-run-robbing component "
            "as a separate column, so this bot can define rHR but cannot cite exact rHR totals yet."
        ),
        default_position_request=PositionRequest("outfielders", ("OF",), ("LF", "CF", "RF")),
    ),
)


def is_drs_question(question: str) -> bool:
    lowered = question.lower()
    return any(contains_metric_term(lowered, hint) for hint in DRS_HINTS) or extract_component_request(question) is not None


def is_single_game_drs_question(question: str) -> bool:
    lowered = question.lower()
    return is_drs_question(question) and (
        any(hint in lowered for hint in SINGLE_GAME_HINTS) or question_mentions_specific_date_reference(question)
    )


def wants_team_drs(question: str) -> bool:
    return bool(TEAM_HINT_PATTERN.search(question)) and is_drs_question(question)


def wants_current_drs(question: str, current_year: int) -> bool:
    year = extract_year(question)
    if year is not None and year != current_year:
        return False
    lowered = question.lower()
    return is_drs_question(question) and (
        bool(CURRENT_DRS_PATTERN.search(question))
        or "today" in lowered
        or "tonight" in lowered
        or "this year" in lowered
    )


def extract_position_request(question: str) -> PositionRequest | None:
    lowered = question.lower()
    for pattern, request in POSITION_ABBREVIATIONS:
        if pattern.search(question):
            return request
    for phrases, request in POSITION_REQUESTS:
        if any(phrase in lowered for phrase in phrases):
            return request
    return None


def extract_component_request(question: str) -> ComponentRequest | None:
    lowered = question.lower()
    for request in (*SUPPORTED_COMPONENT_REQUESTS, *UNSUPPORTED_COMPONENT_REQUESTS):
        if any(contains_metric_term(lowered, alias) for alias in request.aliases):
            return request
    return None


def wants_drs_data_lookup(question: str) -> bool:
    lowered = question.lower()
    if extract_name_candidates(question):
        return True
    if extract_year(question) is not None:
        return True
    if extract_position_request(question) is not None:
        return True
    if wants_team_drs(question):
        return True
    return any(
        phrase in lowered
        for phrase in (
            "leader",
            "leaders",
            "leading",
            "top",
            "highest",
            "lowest",
            "best",
            "worst",
            "career",
            "this season",
            "right now",
            "today",
            "tonight",
            "current",
            "latest",
            "most",
            "least",
        )
    )


def extract_year(question: str) -> int | None:
    match = re.search(r"\b(18\d{2}|19\d{2}|20\d{2})\b", question)
    return int(match.group(1)) if match else None


def extract_name_candidates(question: str) -> list[str]:
    matches = re.findall(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b", question)
    unique: list[str] = []
    for match in matches:
        candidate = normalize_name_candidate(match)
        if candidate and candidate not in unique:
            unique.append(candidate)
    return unique


def normalize_name_candidate(candidate: str) -> str:
    words = candidate.split()
    if len(words) >= 3 and words[0] in {
        "Was",
        "Were",
        "Is",
        "Are",
        "Did",
        "Does",
        "Do",
        "Can",
        "Could",
        "Should",
        "Would",
        "Will",
        "Has",
        "Have",
        "Had",
    }:
        words = words[1:]
    return " ".join(words)


def contains_metric_term(query_lower: str, term: str) -> bool:
    needle = term.strip().lower()
    if not needle:
        return False
    compact = re.sub(r"[^a-z0-9]+", "", needle)
    if compact and len(compact) <= 4:
        pattern = rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])"
        return re.search(pattern, query_lower) is not None
    return needle in query_lower


class DrsResearchHelper:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = FieldingBibleClient(settings)

    def historical_snippets(self, connection, question: str) -> list[EvidenceSnippet]:
        if not table_exists(connection, "fielding_bible_player_drs"):
            return []
        current_year = self.settings.live_season or date.today().year
        currentish = wants_current_drs(question, current_year)
        season = extract_year(question)
        if season is None and currentish:
            season = current_year
        component_request = extract_component_request(question)
        if component_request and component_request.column_name is None:
            return []
        position_request = extract_position_request(question) or (
            component_request.default_position_request if component_request else None
        )

        if wants_team_drs(question):
            team_snippet = self._historical_team_leaderboard(connection, season=season, currentish=currentish)
            return [team_snippet] if team_snippet else []

        lowered = question.lower()
        snippets: list[EvidenceSnippet] = []
        for candidate in extract_name_candidates(question):
            if "career" in lowered or ("all time" in lowered and season is None and not is_single_game_drs_question(question)):
                snippet = self._historical_player_career(connection, candidate, position_request, component_request)
            else:
                snippet = self._historical_player_season(
                    connection,
                    candidate,
                    season=season,
                    currentish=currentish,
                    position_request=position_request,
                    component_request=component_request,
                )
            if snippet:
                snippets.append(snippet)
        if snippets:
            return snippets[:2]

        leaderboard = self._historical_player_leaderboard(
            connection,
            question,
            season=season,
            currentish=currentish,
            position_request=position_request,
            component_request=component_request,
        )
        return [leaderboard] if leaderboard else []

    def live_snippets(self, question: str) -> list[EvidenceSnippet]:
        current_year = self.settings.live_season or date.today().year
        season = extract_year(question) or current_year
        if season != current_year and not wants_current_drs(question, current_year):
            return []
        try:
            if wants_team_drs(question):
                team_rows = self.client.team_drs(season)
                top_rows = sorted(
                    team_rows,
                    key=lambda row: (float(row.get("total") or 0), -int(row.get("teamId") or 0)),
                    reverse=True,
                )[:5]
                if not top_rows:
                    return []
                summary = " ".join(
                    f"{index}. {row.get('nickname')}: {format_number(row.get('total'))} team DRS"
                    for index, row in enumerate(top_rows, start=1)
                )
                return [
                    EvidenceSnippet(
                        source="Fielding Bible / SIS Live",
                        title=f"{season} current team DRS leaders",
                        citation=f"Live Fielding Bible / SIS team DRS pull on {date.today().isoformat()}",
                        summary=summary,
                        payload={"rows": top_rows},
                    )
                ]

            player_rows = self.client.player_drs(season)
        except Exception:
            return []

        component_request = extract_component_request(question)
        if component_request and component_request.column_name is None:
            return []
        if component_request is not None:
            return []
        position_request = extract_position_request(question) or (
            component_request.default_position_request if component_request else None
        )
        snippets: list[EvidenceSnippet] = []
        for candidate in extract_name_candidates(question):
            selected = self._select_preferred_live_player_row(
                player_rows,
                candidate,
                position_request,
                component_request=component_request,
            )
            if not selected:
                continue
            stat_value = extract_component_value(selected, component_request)
            stat_label = component_request.display_label if component_request else "DRS"
            summary = (
                f"Live Fielding Bible / SIS pull: {selected.get('player')} is at "
                f"{format_number(stat_value)} {stat_label} in {season} "
                f"({selected.get('posAbbr') or format_live_position(selected)}, {format_number(selected.get('g'))} G)."
            )
            if component_request:
                summary = f"{summary} Total DRS: {format_number(selected.get('total'))}."
            snippets.append(
                EvidenceSnippet(
                    source="Fielding Bible / SIS Live",
                    title=f"{selected.get('player')} live {season} {stat_label}",
                    citation=f"Live Fielding Bible / SIS player DRS pull on {date.today().isoformat()}",
                    summary=summary,
                    payload=selected,
                )
            )
        if snippets:
            return snippets[:2]

        leaderboard_rows = self._live_leaderboard_rows(player_rows, position_request, component_request=component_request)[:5]
        if not leaderboard_rows:
            return []
        descriptor = f" {position_request.description}" if position_request else ""
        stat_label = component_request.display_label if component_request else "DRS"
        summary = " ".join(
            f"{index}. {row.get('player')}: {format_number(extract_component_value(row, component_request))} {stat_label}"
            for index, row in enumerate(leaderboard_rows, start=1)
        )
        return [
            EvidenceSnippet(
                source="Fielding Bible / SIS Live",
                title=f"{season} current{descriptor} {stat_label} leaders".strip(),
                citation=f"Live Fielding Bible / SIS player DRS pull on {date.today().isoformat()}",
                summary=summary,
                payload={"rows": leaderboard_rows},
            )
        ]

    def _historical_player_season(
        self,
        connection,
        player_name: str,
        *,
        season: int | None,
        currentish: bool,
        position_request: PositionRequest | None,
        component_request: ComponentRequest | None,
    ) -> EvidenceSnippet | None:
        rows = self._fetch_player_rows(connection, player_name, season=season, snapshot_at="")
        snapshot_at = ""
        if not rows and season is not None and currentish:
            snapshot_at = latest_snapshot_at(connection, "fielding_bible_player_drs", season=season) or ""
            if snapshot_at:
                rows = self._fetch_player_rows(connection, player_name, season=season, snapshot_at=snapshot_at)
        row = self._select_preferred_player_row(rows, position_request)
        if row is None:
            return None
        component_text = self._format_components(row)
        stat_value = extract_component_value(row, component_request)
        stat_label = component_request.display_label if component_request else "DRS"
        source_suffix = f" snapshot {snapshot_at}" if snapshot_at else ""
        summary = (
            f"{row['player']} was at {format_number(stat_value)} {stat_label} in {row['season']} "
            f"({format_position_label(row)}, {format_number(row['games'])} G, {format_number(row['innings'])} innings)."
        )
        if component_request:
            summary = f"{summary} Total DRS: {format_number(row['total'])}."
        elif component_text:
            summary = f"{summary} Components: {component_text}."
        return EvidenceSnippet(
            source="Fielding Bible / SIS",
            title=f"{row['player']} {row['season']} {stat_label}",
            citation=f"Fielding Bible / SIS player DRS{source_suffix}",
            summary=summary,
            payload=dict(row),
        )

    def _historical_player_career(
        self,
        connection,
        player_name: str,
        position_request: PositionRequest | None,
        component_request: ComponentRequest | None,
    ) -> EvidenceSnippet | None:
        row = self._player_career_row(connection, player_name, position_request, component_request)
        if row is None:
            return None
        descriptor = position_request.description if position_request else "overall"
        stat_label = component_request.display_label if component_request else "DRS"
        summary = (
            f"{row['player']} has {format_number(row['career_value'])} career {stat_label} across synced seasons "
            f"{row['first_season']}-{row['last_season']} ({descriptor})."
        )
        return EvidenceSnippet(
            source="Fielding Bible / SIS",
            title=f"{row['player']} career {stat_label}",
            citation="Fielding Bible / SIS player DRS",
            summary=summary,
            payload=dict(row),
        )

    def _historical_player_leaderboard(
        self,
        connection,
        question: str,
        *,
        season: int | None,
        currentish: bool,
        position_request: PositionRequest | None,
        component_request: ComponentRequest | None,
    ) -> EvidenceSnippet | None:
        lowered = question.lower()
        wants_career = "career" in lowered or ("all time" in lowered and season is None and "season" not in lowered)
        if wants_career and not is_single_game_drs_question(question):
            rows = self._career_leaderboard_rows(connection, position_request, component_request, limit=5)
            if not rows:
                return None
            stat_label = component_request.display_label if component_request else "DRS"
            lines = [
                f"{index}. {row['player']}: {format_number(row['career_value'])} career {stat_label} ({row['first_season']}-{row['last_season']})"
                for index, row in enumerate(rows, start=1)
            ]
            descriptor = f" {position_request.description}" if position_request else ""
            return EvidenceSnippet(
                source="Fielding Bible / SIS",
                title=f"Career{descriptor} {stat_label} leaders",
                citation="Fielding Bible / SIS player DRS",
                summary=" ".join(lines),
                payload={"rows": [dict(row) for row in rows]},
            )

        snapshot_at = ""
        rows = self._season_leaderboard_rows(connection, season, position_request, component_request, limit=5)
        if not rows and season is not None and currentish:
            snapshot_at = latest_snapshot_at(connection, "fielding_bible_player_drs", season=season) or ""
            if snapshot_at:
                rows = self._season_leaderboard_rows(
                    connection,
                    season,
                    position_request,
                    component_request,
                    limit=5,
                    snapshot_at=snapshot_at,
                )
        if not rows:
            return None
        descriptor = f" {position_request.description}" if position_request else ""
        stat_label = component_request.display_label if component_request else "DRS"
        title = f"{season}{descriptor} {stat_label} leaders" if season is not None else f"Top{descriptor} {stat_label} seasons"
        citation = "Fielding Bible / SIS player DRS"
        if snapshot_at:
            citation = f"{citation} snapshot {snapshot_at}"
        lines = []
        for index, row in enumerate(rows, start=1):
            if season is None:
                lines.append(f"{index}. {row['player']}, {row['season']}: {format_number(extract_component_value(row, component_request))} {stat_label}")
            else:
                lines.append(f"{index}. {row['player']} ({format_position_label(row)}): {format_number(extract_component_value(row, component_request))} {stat_label}")
        return EvidenceSnippet(
            source="Fielding Bible / SIS",
            title=title.strip(),
            citation=citation,
            summary=" ".join(lines),
            payload={"rows": [dict(row) for row in rows], "snapshot_at": snapshot_at},
        )

    def _historical_team_leaderboard(
        self,
        connection,
        *,
        season: int | None,
        currentish: bool,
    ) -> EvidenceSnippet | None:
        if not table_exists(connection, "fielding_bible_team_drs"):
            return None
        snapshot_at = ""
        rows = self._team_leaderboard_rows(connection, season=season, limit=5)
        if not rows and season is not None and currentish:
            snapshot_at = latest_snapshot_at(connection, "fielding_bible_team_drs", season=season) or ""
            if snapshot_at:
                rows = self._team_leaderboard_rows(connection, season=season, limit=5, snapshot_at=snapshot_at)
        if not rows:
            return None
        citation = "Fielding Bible / SIS team DRS"
        if snapshot_at:
            citation = f"{citation} snapshot {snapshot_at}"
        summary = " ".join(
            f"{index}. {row['nickname']}: {format_number(row['total'])} team DRS"
            for index, row in enumerate(rows, start=1)
        )
        return EvidenceSnippet(
            source="Fielding Bible / SIS",
            title=f"{season} team DRS leaders" if season is not None else "Top team DRS seasons",
            citation=citation,
            summary=summary,
            payload={"rows": [dict(row) for row in rows], "snapshot_at": snapshot_at},
        )

    def _fetch_player_rows(self, connection, player_name: str, *, season: int | None, snapshot_at: str):
        sql = """
            SELECT *
            FROM fielding_bible_player_drs
            WHERE snapshot_at = ?
              AND lower(player) LIKE ?
        """
        parameters: list[Any] = [snapshot_at, f"%{player_name.lower()}%"]
        if season is not None:
            sql += " AND season = ?"
            parameters.append(season)
        sql += """
            ORDER BY
                CASE WHEN lower(player) = ? THEN 0 ELSE 1 END,
                season DESC,
                CASE WHEN team_id = 0 THEN 0 ELSE 1 END,
                CASE WHEN pos = 0 THEN 0 ELSE 1 END,
                ABS(COALESCE(total, 0)) DESC
            LIMIT 80
        """
        parameters.append(player_name.lower())
        return fetch_rows(connection, sql, parameters)

    def _select_preferred_player_row(self, rows, position_request: PositionRequest | None):
        if not rows:
            return None
        if position_request is None:
            for row in rows:
                if int(row["team_id"] or 0) == 0 and int(row["pos"] or 0) == 0:
                    return row
            for row in rows:
                if int(row["team_id"] or 0) == 0:
                    return row
            return rows[0]

        primary = set(position_request.primary_labels)
        fallback = set(position_request.fallback_labels)
        for row in rows:
            if int(row["team_id"] or 0) == 0 and str(row["pos_abbr"] or "") in primary:
                return row
        for row in rows:
            if int(row["team_id"] or 0) == 0 and str(row["pos_abbr"] or "") in fallback:
                return row
        for row in rows:
            if str(row["pos_abbr"] or "") in primary:
                return row
        for row in rows:
            if str(row["pos_abbr"] or "") in fallback:
                return row
        return rows[0]

    def _player_career_row(
        self,
        connection,
        player_name: str,
        position_request: PositionRequest | None,
        component_request: ComponentRequest | None,
    ):
        row = self._run_player_career_query(
            connection,
            player_name,
            position_request,
            component_request,
            use_fallback=False,
        )
        if row or position_request is None or not position_request.fallback_labels:
            return row
        return self._run_player_career_query(
            connection,
            player_name,
            position_request,
            component_request,
            use_fallback=True,
        )

    def _run_player_career_query(
        self,
        connection,
        player_name: str,
        position_request: PositionRequest | None,
        component_request: ComponentRequest | None,
        *,
        use_fallback: bool,
    ):
        clause, params = build_position_clause(position_request, use_fallback=use_fallback)
        value_expression = aggregate_value_expression(component_request)
        return connection.execute(
            f"""
            SELECT
                player,
                player_id,
                {value_expression} AS career_value,
                MIN(season) AS first_season,
                MAX(season) AS last_season
            FROM fielding_bible_player_drs
            WHERE snapshot_at = ''
              AND team_id = 0
              AND lower(player) LIKE ?
              AND {clause}
            GROUP BY player, player_id
            ORDER BY
                CASE WHEN lower(player) = ? THEN 0 ELSE 1 END,
                ABS(COALESCE({value_expression}, 0)) DESC,
                MAX(season) DESC
            LIMIT 1
            """,
            (f"%{player_name.lower()}%", *params, player_name.lower()),
        ).fetchone()

    def _career_leaderboard_rows(
        self,
        connection,
        position_request: PositionRequest | None,
        component_request: ComponentRequest | None,
        *,
        limit: int,
    ):
        rows = self._run_career_leaderboard_query(
            connection,
            position_request,
            component_request,
            limit=limit,
            use_fallback=False,
        )
        if rows or position_request is None or not position_request.fallback_labels:
            return rows
        return self._run_career_leaderboard_query(
            connection,
            position_request,
            component_request,
            limit=limit,
            use_fallback=True,
        )

    def _run_career_leaderboard_query(
        self,
        connection,
        position_request: PositionRequest | None,
        component_request: ComponentRequest | None,
        *,
        limit: int,
        use_fallback: bool,
    ):
        clause, params = build_position_clause(position_request, use_fallback=use_fallback)
        value_expression = aggregate_value_expression(component_request)
        return fetch_rows(
            connection,
            f"""
            SELECT
                player,
                player_id,
                {value_expression} AS career_value,
                MIN(season) AS first_season,
                MAX(season) AS last_season
            FROM fielding_bible_player_drs
            WHERE snapshot_at = ''
              AND team_id = 0
              AND {clause}
            GROUP BY player, player_id
            ORDER BY career_value DESC, last_season DESC, player ASC
            LIMIT ?
            """,
            [*params, limit],
        )

    def _season_leaderboard_rows(
        self,
        connection,
        season: int | None,
        position_request: PositionRequest | None,
        component_request: ComponentRequest | None,
        *,
        limit: int,
        snapshot_at: str = "",
    ):
        rows = self._run_season_leaderboard_query(
            connection,
            season,
            position_request,
            component_request,
            limit=limit,
            snapshot_at=snapshot_at,
            use_fallback=False,
        )
        if rows or position_request is None or not position_request.fallback_labels:
            return rows
        return self._run_season_leaderboard_query(
            connection,
            season,
            position_request,
            component_request,
            limit=limit,
            snapshot_at=snapshot_at,
            use_fallback=True,
        )

    def _run_season_leaderboard_query(
        self,
        connection,
        season: int | None,
        position_request: PositionRequest | None,
        component_request: ComponentRequest | None,
        *,
        limit: int,
        snapshot_at: str,
        use_fallback: bool,
    ):
        clause, params = build_position_clause(position_request, use_fallback=use_fallback)
        sql = f"""
            SELECT *
            FROM fielding_bible_player_drs
            WHERE snapshot_at = ?
              AND team_id = 0
              AND {clause}
        """
        parameters: list[Any] = [snapshot_at, *params]
        if season is not None:
            sql += " AND season = ?"
            parameters.append(season)
        order_column = order_by_column(component_request)
        sql += """
            ORDER BY {order_column} DESC, season DESC, player ASC
            LIMIT ?
        """.format(order_column=order_column)
        parameters.append(limit)
        return fetch_rows(connection, sql, parameters)

    def _team_leaderboard_rows(self, connection, *, season: int | None, limit: int, snapshot_at: str = ""):
        sql = """
            SELECT *
            FROM fielding_bible_team_drs
            WHERE snapshot_at = ?
        """
        parameters: list[Any] = [snapshot_at]
        if season is not None:
            sql += " AND season = ?"
            parameters.append(season)
        sql += """
            ORDER BY total DESC, season DESC, nickname ASC
            LIMIT ?
        """
        parameters.append(limit)
        return fetch_rows(connection, sql, parameters)

    def _format_components(self, row) -> str:
        parts = []
        for key, label in DRS_COMPONENT_LABELS:
            value = row[key] if key in row.keys() else None
            if value in (None, 0, 0.0):
                continue
            parts.append(f"{label} {format_signed_number(value)}")
        return ", ".join(parts)

    def _select_preferred_live_player_row(
        self,
        rows: list[dict[str, Any]],
        player_name: str,
        position_request: PositionRequest | None,
        component_request: ComponentRequest | None,
    ) -> dict[str, Any] | None:
        matching = [row for row in rows if player_name.lower() in str(row.get("player") or "").lower()]
        if not matching:
            return None
        matching.sort(
            key=lambda row: (
                0 if str(row.get("player") or "").lower() == player_name.lower() else 1,
                0 if int(row.get("teamId") or 0) == 0 else 1,
                0 if int(row.get("pos") or 0) == 0 else 1,
                -float(extract_component_value(row, component_request)),
            )
        )
        if position_request is None:
            for row in matching:
                if int(row.get("teamId") or 0) == 0 and int(row.get("pos") or 0) == 0:
                    return row
            return matching[0]

        primary = set(position_request.primary_labels)
        fallback = set(position_request.fallback_labels)
        for row in matching:
            if int(row.get("teamId") or 0) == 0 and str(row.get("posAbbr") or "") in primary:
                if component_request is None or extract_component_value(row, component_request) != 0:
                    return row
        for row in matching:
            if int(row.get("teamId") or 0) == 0 and str(row.get("posAbbr") or "") in fallback:
                return row
        for row in matching:
            if str(row.get("posAbbr") or "") in primary:
                return row
        for row in matching:
            if str(row.get("posAbbr") or "") in fallback:
                return row
        return matching[0]

    def _live_leaderboard_rows(
        self,
        rows: list[dict[str, Any]],
        position_request: PositionRequest | None,
        component_request: ComponentRequest | None,
    ):
        if position_request is None:
            filtered = [
                row for row in rows if int(row.get("teamId") or 0) == 0 and int(row.get("pos") or 0) == 0
            ]
        else:
            filtered = [
                row
                for row in rows
                if int(row.get("teamId") or 0) == 0 and str(row.get("posAbbr") or "") in position_request.primary_labels
            ]
            if (
                position_request.fallback_labels
                and (not filtered or max(extract_component_value(row, component_request) for row in filtered) == 0)
            ):
                filtered = [
                    row
                    for row in rows
                    if int(row.get("teamId") or 0) == 0 and str(row.get("posAbbr") or "") in position_request.fallback_labels
                ]
        return sorted(
            filtered,
            key=lambda row: (
                float(extract_component_value(row, component_request)),
                float(row.get("inn") or 0),
                str(row.get("player") or ""),
            ),
            reverse=True,
        )


def build_position_clause(
    position_request: PositionRequest | None,
    *,
    use_fallback: bool,
) -> tuple[str, list[Any]]:
    if position_request is None:
        return ("pos = 0", [])
    labels = position_request.fallback_labels if use_fallback else position_request.primary_labels
    if not labels:
        return ("1 = 0", [])
    placeholders = ", ".join("?" for _ in labels)
    return (f"pos_abbr IN ({placeholders})", list(labels))


def aggregate_value_expression(component_request: ComponentRequest | None) -> str:
    if component_request is None or component_request.column_name is None:
        return "SUM(COALESCE(total, 0))"
    return f"SUM(COALESCE({component_request.column_name}, 0))"


def order_by_column(component_request: ComponentRequest | None) -> str:
    if component_request is None or component_request.column_name is None:
        return "COALESCE(total, 0)"
    return f"COALESCE({component_request.column_name}, 0)"


def extract_component_value(row: Any, component_request: ComponentRequest | None) -> float:
    if component_request is None or component_request.column_name is None:
        raw = row["total"] if hasattr(row, "keys") and "total" in row.keys() else row.get("total")
    else:
        if hasattr(row, "keys"):
            raw = row[component_request.column_name] if component_request.column_name in row.keys() else 0
        else:
            raw = row.get(component_request.column_name)
    return float(raw or 0)


def format_position_label(row) -> str:
    pos_abbr = str(row["pos_abbr"] or "").strip()
    if pos_abbr:
        return pos_abbr
    if int(row["pos"] or 0) == 0:
        return "overall"
    return f"pos {row['pos']}"


def format_live_position(row: dict[str, Any]) -> str:
    pos_abbr = str(row.get("posAbbr") or "").strip()
    if pos_abbr:
        return pos_abbr
    if int(row.get("pos") or 0) == 0:
        return "overall"
    return f"pos {row.get('pos')}"


def format_number(value: Any) -> str:
    if value in (None, ""):
        return "0"
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.1f}"


def format_signed_number(value: Any) -> str:
    number = float(value or 0)
    if number.is_integer():
        return f"{int(number):+d}"
    return f"{number:+.1f}"
