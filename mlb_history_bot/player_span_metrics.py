from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .models import EvidenceSnippet
from .person_query import choose_best_person_match
from .query_utils import extract_name_candidates, extract_season_span, normalize_person_name
from .season_metric_leaderboards import find_season_metric, normalize_metric_search_text, strip_qualifier_clauses
from .storage import table_exists
from .team_evaluator import safe_int


PLAYER_SPAN_REJECT_HINTS = (
    "highest",
    "lowest",
    "best",
    "worst",
    "leader",
    "leaders",
    "which hitter",
    "which pitcher",
    "which player",
    "who had",
    "who has",
)
SPAN_PLAYER_PATTERNS = (
    re.compile(
        r"\bhow\s+many\s+.+?\s+did\s+([a-z][a-z'.-]+(?:\s+[a-z][a-z'.-]+){0,2})\s+"
        r"(?:have|hit|record|compile|collect|get)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bwhat\s+(?:was|is)\s+([a-z][a-z'.-]+(?:\s+[a-z][a-z'.-]+){0,2})(?:'s)?\s+.+?\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b([a-z][a-z'.-]+(?:\s+[a-z][a-z'.-]+){0,2})\s+.+?\b(?:between|from)\s+(18\d{2}|19\d{2}|20\d{2})\b",
        re.IGNORECASE,
    ),
)


@dataclass(slots=True)
class PlayerSpanMetricQuery:
    player_query: str
    player_name: str
    start_season: int
    end_season: int
    scope_label: str
    metric_key: str
    metric_label: str


class PlayerSpanMetricResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.live_client = LiveStatsClient(settings)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_player_span_metric_query(question, self.live_client, self.settings.live_season or date.today().year)
        if query is None:
            return None
        row = fetch_player_span_row(connection, query)
        if row is None:
            return None
        return EvidenceSnippet(
            source="Player Span Metrics",
            title=f"{query.player_name} {query.scope_label} {query.metric_label}",
            citation="Lahman Batting and People tables aggregated across the requested season span",
            summary=build_player_span_summary(query, row),
            payload={
                "analysis_type": "player_span_metric",
                "mode": "historical",
                "player": row["player_name"],
                "metric": query.metric_label,
                "scope_label": query.scope_label,
                "complete": True,
                "total_row_count": 1,
                "rows": [row],
            },
        )


def parse_player_span_metric_query(
    question: str,
    live_client: LiveStatsClient,
    current_season: int,
) -> PlayerSpanMetricQuery | None:
    lowered = f" {question.lower().strip()} "
    if any(token in lowered for token in PLAYER_SPAN_REJECT_HINTS):
        return None
    span = extract_season_span(question, current_season)
    if span is None:
        return None
    metric_search_text = normalize_metric_search_text(strip_qualifier_clauses(lowered))
    metric = find_season_metric(metric_search_text)
    if metric is None or metric.source_family != "historical" or metric.entity_scope != "player" or metric.role != "hitter":
        return None
    player_query = extract_player_span_candidate(question)
    if not player_query:
        return None
    people = live_client.search_people(player_query)
    player_name = player_query
    if people:
        player_name = str(choose_best_person_match(people, player_query).get("fullName") or player_query).strip()
    return PlayerSpanMetricQuery(
        player_query=player_query,
        player_name=player_name,
        start_season=span.start_season,
        end_season=span.end_season,
        scope_label=span.label,
        metric_key=metric.key,
        metric_label=metric.label,
    )


def extract_player_span_candidate(question: str) -> str | None:
    candidates = extract_name_candidates(question)
    if candidates:
        return candidates[0]
    stripped = question.strip(" ?.!")
    for pattern in SPAN_PLAYER_PATTERNS:
        match = pattern.search(stripped)
        if not match:
            continue
        candidate = clean_loose_player_phrase(match.group(1))
        if candidate:
            return candidate
    return None


def clean_loose_player_phrase(value: str) -> str:
    cleaned = re.sub(r"'s\b", "", value.strip(" ?.!,'\""), flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(18\d{2}|19\d{2}|20\d{2})\b", " ", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    if not cleaned:
        return ""
    return " ".join(part.capitalize() for part in cleaned.split())


def fetch_player_span_row(connection, query: PlayerSpanMetricQuery) -> dict[str, Any] | None:
    if not (table_exists(connection, "lahman_batting") and table_exists(connection, "lahman_people")):
        return None
    person_match = resolve_best_lahman_person(connection, query.player_name, query.start_season, query.end_season)
    if person_match is None:
        return None
    row = connection.execute(
        """
        SELECT
            SUM(CAST(COALESCE(b.g, '0') AS INTEGER)) AS games,
            SUM(CAST(COALESCE(b.ab, '0') AS INTEGER)) AS at_bats,
            SUM(CAST(COALESCE(b.r, '0') AS INTEGER)) AS runs,
            SUM(CAST(COALESCE(b.h, '0') AS INTEGER)) AS hits,
            SUM(CAST(COALESCE(b.c_2b, '0') AS INTEGER)) AS doubles,
            SUM(CAST(COALESCE(b.c_3b, '0') AS INTEGER)) AS triples,
            SUM(CAST(COALESCE(b.hr, '0') AS INTEGER)) AS home_runs,
            SUM(CAST(COALESCE(b.rbi, '0') AS INTEGER)) AS rbi,
            SUM(CAST(COALESCE(b.sb, '0') AS INTEGER)) AS steals,
            SUM(CAST(COALESCE(b.cs, '0') AS INTEGER)) AS caught_stealing,
            SUM(CAST(COALESCE(b.bb, '0') AS INTEGER)) AS walks,
            SUM(CAST(COALESCE(b.so, '0') AS INTEGER)) AS strikeouts,
            SUM(CAST(COALESCE(b.hbp, '0') AS INTEGER)) AS hit_by_pitch,
            SUM(CAST(COALESCE(b.sh, '0') AS INTEGER)) AS sacrifice_hits,
            SUM(CAST(COALESCE(b.sf, '0') AS INTEGER)) AS sacrifice_flies
        FROM lahman_batting AS b
        WHERE b.playerid = ?
          AND CAST(b.yearid AS INTEGER) BETWEEN ? AND ?
        """,
        (person_match["playerid"], query.start_season, query.end_season),
    ).fetchone()
    if row is None:
        return None
    at_bats = safe_int(row["at_bats"]) or 0
    walks = safe_int(row["walks"]) or 0
    hit_by_pitch = safe_int(row["hit_by_pitch"]) or 0
    sacrifice_hits = safe_int(row["sacrifice_hits"]) or 0
    sacrifice_flies = safe_int(row["sacrifice_flies"]) or 0
    hits = safe_int(row["hits"]) or 0
    doubles = safe_int(row["doubles"]) or 0
    triples = safe_int(row["triples"]) or 0
    home_runs = safe_int(row["home_runs"]) or 0
    plate_appearances = at_bats + walks + hit_by_pitch + sacrifice_hits + sacrifice_flies
    if plate_appearances <= 0:
        return None
    singles = hits - doubles - triples - home_runs
    obp_denom = at_bats + walks + hit_by_pitch + sacrifice_flies
    avg = (hits / at_bats) if at_bats else None
    obp = ((hits + walks + hit_by_pitch) / obp_denom) if obp_denom else None
    slg = ((singles + (2 * doubles) + (3 * triples) + (4 * home_runs)) / at_bats) if at_bats else None
    ops = (obp + slg) if obp is not None and slg is not None else None
    metric_map = {
        "games": safe_int(row["games"]) or 0,
        "plate_appearances": plate_appearances,
        "at_bats": at_bats,
        "avg": avg,
        "obp": obp,
        "slg": slg,
        "ops": ops,
        "hits": hits,
        "home_runs": home_runs,
        "runs": safe_int(row["runs"]) or 0,
        "rbi": safe_int(row["rbi"]) or 0,
        "walks": walks,
        "strikeouts": safe_int(row["strikeouts"]) or 0,
        "doubles": doubles,
        "triples": triples,
        "steals": safe_int(row["steals"]) or 0,
        "hit_by_pitch": hit_by_pitch,
        "total_bases": singles + (2 * doubles) + (3 * triples) + (4 * home_runs),
        "extra_base_hits": doubles + triples + home_runs,
        "runs_per_game": ((safe_int(row["runs"]) or 0) / (safe_int(row["games"]) or 0)) if (safe_int(row["games"]) or 0) else None,
        "rbi_per_game": ((safe_int(row["rbi"]) or 0) / (safe_int(row["games"]) or 0)) if (safe_int(row["games"]) or 0) else None,
        "walks_per_game": (walks / (safe_int(row["games"]) or 0)) if (safe_int(row["games"]) or 0) else None,
        "strikeouts_per_game": ((safe_int(row["strikeouts"]) or 0) / (safe_int(row["games"]) or 0)) if (safe_int(row["games"]) or 0) else None,
        "hits_per_game": (hits / (safe_int(row["games"]) or 0)) if (safe_int(row["games"]) or 0) else None,
        "home_runs_per_game": (home_runs / (safe_int(row["games"]) or 0)) if (safe_int(row["games"]) or 0) else None,
    }
    metric_value = metric_map.get(query.metric_key)
    if metric_value is None:
        return None
    return {
        "player_name": person_match["player_name"],
        "scope_label": query.scope_label,
        "scope_start_season": query.start_season,
        "scope_end_season": query.end_season,
        "metric_value": float(metric_value),
        "games": safe_int(row["games"]) or 0,
        "plate_appearances": plate_appearances,
        "at_bats": at_bats,
        "runs": safe_int(row["runs"]) or 0,
        "hits": hits,
        "doubles": doubles,
        "triples": triples,
        "home_runs": home_runs,
        "runs_batted_in": safe_int(row["rbi"]) or 0,
        "walks": walks,
        "strikeouts": safe_int(row["strikeouts"]) or 0,
        "steals": safe_int(row["steals"]) or 0,
        "caught_stealing": safe_int(row["caught_stealing"]) or 0,
        "hit_by_pitch": hit_by_pitch,
        "avg": avg,
        "obp": obp,
        "slg": slg,
        "ops": ops,
        "total_bases": metric_map["total_bases"],
        "extra_base_hits": metric_map["extra_base_hits"],
    }


def resolve_best_lahman_person(connection, player_name: str, start_season: int, end_season: int) -> dict[str, Any] | None:
    normalized_target = normalize_person_name(player_name)
    target_parts = normalized_target.split()
    last_name_only = target_parts[-1] if target_parts else ""
    people = connection.execute(
        "SELECT playerid, namefirst, namelast FROM lahman_people"
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in people:
        full_name = f"{row['namefirst'] or ''} {row['namelast'] or ''}".strip()
        normalized_full = normalize_person_name(full_name)
        normalized_last = normalize_person_name(str(row["namelast"] or ""))
        if normalized_full == normalized_target or (last_name_only and normalized_last == last_name_only):
            games = connection.execute(
                """
                SELECT SUM(CAST(COALESCE(g, '0') AS INTEGER))
                FROM lahman_batting
                WHERE playerid = ?
                  AND CAST(yearid AS INTEGER) BETWEEN ? AND ?
                """,
                (row["playerid"], start_season, end_season),
            ).fetchone()[0] or 0
            candidates.append(
                {
                    "playerid": str(row["playerid"] or ""),
                    "player_name": full_name,
                    "games": int(games),
                    "exact_name_match": normalized_full == normalized_target,
                }
            )
    if not candidates:
        return None
    candidates.sort(key=lambda item: (0 if item["exact_name_match"] else 1, -item["games"], item["player_name"]))
    return candidates[0]


def build_player_span_summary(query: PlayerSpanMetricQuery, row: dict[str, Any]) -> str:
    metric_text = format_metric_value(query.metric_key, row["metric_value"])
    summary = (
        f"{row['player_name']} had {metric_text} {query.metric_label} from {query.scope_label}. "
        f"Over that span: {row['plate_appearances']} PA, {row['at_bats']} AB, {row['hits']} H, "
        f"{row['home_runs']} HR, and {format_rate(row['avg'])}/{format_rate(row['obp'])}/{format_rate(row['slg'])} "
        f"({format_rate(row['ops'])} OPS)."
    )
    if query.metric_key == "avg":
        summary = (
            f"{row['player_name']}'s batting average from {query.scope_label} was {format_rate(row['avg'])}, "
            f"going {row['hits']}-for-{row['at_bats']} in {row['plate_appearances']} PA."
        )
    elif query.metric_key == "home_runs":
        summary = (
            f"{row['player_name']} hit {row['home_runs']} home runs from {query.scope_label}. "
            f"He also had {row['hits']} hits in {row['plate_appearances']} PA / {row['at_bats']} AB, "
            f"for a {format_rate(row['avg'])} batting average."
        )
    elif query.metric_key == "hits":
        summary = (
            f"{row['player_name']} had {row['hits']} hits from {query.scope_label}. "
            f"Of those, {row['home_runs']} were home runs, across {row['plate_appearances']} PA / {row['at_bats']} AB."
        )
    return summary


def format_rate(value: Any) -> str:
    if value is None:
        return "N/A"
    text = f"{float(value):.3f}"
    return text[1:] if text.startswith("0") else text


def format_metric_value(metric_key: str, value: float) -> str:
    if metric_key in {"avg", "obp", "slg", "ops"}:
        return format_rate(value)
    if metric_key.endswith("_per_game"):
        return f"{float(value):.2f}"
    return f"{int(value):,}" if float(value).is_integer() else f"{float(value):.2f}"
