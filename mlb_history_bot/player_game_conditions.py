from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re
from typing import Any

from .config import Settings
from .models import EvidenceSnippet
from .query_intent import detect_ranking_intent
from .query_utils import extract_minimum_qualifier
from .season_metric_leaderboards import (
    SeasonMetricSpec,
    find_season_metric,
    normalize_metric_search_text,
    resolve_season_scope,
    strip_qualifier_clauses,
)
from .storage import table_exists
from .team_evaluator import safe_int
from .team_season_leaders import build_person_name, select_historical_hitting_metric


@dataclass(slots=True, frozen=True)
class PlayerGameConditionSpec:
    key: str
    label: str
    aliases: tuple[str, ...]
    game_label: str


@dataclass(slots=True)
class PlayerGameConditionQuery:
    condition: PlayerGameConditionSpec
    metric: SeasonMetricSpec
    descriptor: str
    sort_desc: bool
    start_season: int
    end_season: int
    scope_label: str
    minimum_value: int | None = None
    minimum_basis: str | None = None
    minimum_label: str | None = None


BIRTHDAY_CONDITION = PlayerGameConditionSpec(
    key="birthday",
    label="games played on a player's birthday",
    aliases=("birthday", "on their birthday", "on his birthday", "on her birthday"),
    game_label="Birthday G",
)

CONDITION_SPECS: tuple[PlayerGameConditionSpec, ...] = (BIRTHDAY_CONDITION,)
ROLE_HINT_PATTERN = re.compile(r"\b(hitter|batter|offensive player|pitcher|starter|reliever|fielder|defender)\b", re.IGNORECASE)
CONDITION_MINIMUM_QUALIFIERS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("plate_appearances", ("plate appearances", "pa"), "PA"),
    ("at_bats", ("at bats", "ab"), "AB"),
    ("games", ("games", "game"), "birthday games"),
    ("hits", ("hits", "hit"), "H"),
    ("home_runs", ("home runs", "home run", "hr", "homers", "homeruns"), "HR"),
    ("walks", ("walks", "walk", "bb"), "BB"),
    ("strikeouts", ("strikeouts", "strikeout", "so"), "SO"),
    ("runs_batted_in", ("rbi", "runs batted in"), "RBI"),
)


class PlayerGameConditionResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_player_game_condition_query(question, self.settings)
        if query is None:
            return None
        if query.metric.role not in {"hitter", "player"}:
            return None
        rows, metadata = fetch_hitting_condition_rows(connection, query)
        if not rows and not metadata.get("total_row_count"):
            return None
        summary = build_player_game_condition_summary(query, rows, metadata)
        display_rows = rows[:12]
        return EvidenceSnippet(
            source="Retrosheet Player Game Conditions",
            title=f"{query.condition.label} {query.metric.label} leaderboard",
            citation="Retrosheet batting game logs joined to Lahman player metadata",
            summary=summary,
            payload={
                "analysis_type": "player_game_condition_leaderboard",
                "mode": "historical",
                "condition_key": query.condition.key,
                "condition_label": query.condition.label,
                "condition_game_label": query.condition.game_label,
                "metric": query.metric.label,
                "role": query.metric.role,
                "scope_label": query.scope_label,
                "rows": display_rows,
                "displayed_row_count": len(display_rows),
                "total_row_count": int(metadata.get("total_row_count") or 0),
                "qualifying_row_count": len(rows),
                "leaderboard_complete": True,
                "leaderboard_scope_note": (
                    "This leaderboard was computed from the full local condition-matched game log dataset. "
                    "Any explicit minimum qualifier was applied against that full result set before ranking. "
                    "The rows array is only the top display slice."
                ),
                "minimum_value": query.minimum_value,
                "minimum_basis": query.minimum_basis,
                "minimum_label": query.minimum_label,
                "max_condition_games": int(metadata.get("max_condition_games") or 0),
                "max_plate_appearances": int(metadata.get("max_plate_appearances") or 0),
                "max_at_bats": int(metadata.get("max_at_bats") or 0),
                "max_basis_value": metadata.get("max_basis_value"),
            },
        )


def parse_player_game_condition_query(question: str, settings: Settings) -> PlayerGameConditionQuery | None:
    lowered = question.lower()
    condition = find_condition(lowered)
    if condition is None:
        return None
    if "all-star" in lowered or "cy young" in lowered or "gold glove" in lowered:
        return None
    if ROLE_HINT_PATTERN.search(lowered) and any(token in lowered for token in ("pitcher", "starter", "reliever", "fielder", "defender")):
        return None

    normalized_metric_text = normalize_metric_search_text(strip_qualifier_clauses(lowered))
    metric = find_season_metric(normalized_metric_text)
    if metric is None:
        return None
    if metric.source_family != "historical" or metric.entity_scope != "player":
        return None
    if metric.role not in {"hitter", "player"}:
        return None

    ranking = detect_ranking_intent(lowered, higher_is_better=metric.higher_is_better, require_hint=False)
    if ranking is None:
        return None
    current_season = settings.live_season or date.today().year
    start_season, end_season, scope_label, _aggregate = resolve_season_scope(question, current_season, "historical")
    if start_season is None or end_season is None:
        return None
    minimum_basis, minimum_label, minimum_value = parse_condition_minimum_qualifier(question)
    return PlayerGameConditionQuery(
        condition=condition,
        metric=metric,
        descriptor=ranking.descriptor,
        sort_desc=ranking.sort_desc,
        start_season=start_season,
        end_season=end_season,
        scope_label=scope_label,
        minimum_value=minimum_value,
        minimum_basis=minimum_basis,
        minimum_label=minimum_label,
    )


def find_condition(lowered_question: str) -> PlayerGameConditionSpec | None:
    for spec in CONDITION_SPECS:
        if any(alias in lowered_question for alias in spec.aliases):
            return spec
    return None


def parse_condition_minimum_qualifier(question: str) -> tuple[str | None, str | None, int | None]:
    for basis, nouns, label in CONDITION_MINIMUM_QUALIFIERS:
        value = extract_minimum_qualifier(question, nouns)
        if value is not None:
            return basis, label, value
    return None, None, None


def fetch_hitting_condition_rows(connection, query: PlayerGameConditionQuery) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not (table_exists(connection, "retrosheet_batting") and table_exists(connection, "lahman_people")):
        return [], {"total_row_count": 0}
    if query.condition.key != "birthday":
        return [], {"total_row_count": 0}
    rows = connection.execute(
        """
        SELECT
            b.id AS retro_id,
            p.namefirst,
            p.namelast,
            MIN(CAST(substr(b.date, 1, 4) AS INTEGER)) AS first_season,
            MAX(CAST(substr(b.date, 1, 4) AS INTEGER)) AS last_season,
            COUNT(*) AS games,
            SUM(CAST(COALESCE(b.b_pa, '0') AS INTEGER)) AS plate_appearances,
            SUM(CAST(COALESCE(b.b_ab, '0') AS INTEGER)) AS at_bats,
            SUM(CAST(COALESCE(b.b_r, '0') AS INTEGER)) AS runs,
            SUM(CAST(COALESCE(b.b_h, '0') AS INTEGER)) AS hits,
            SUM(CAST(COALESCE(b.b_d, '0') AS INTEGER)) AS doubles,
            SUM(CAST(COALESCE(b.b_t, '0') AS INTEGER)) AS triples,
            SUM(CAST(COALESCE(b.b_hr, '0') AS INTEGER)) AS home_runs,
            SUM(CAST(COALESCE(b.b_rbi, '0') AS INTEGER)) AS rbi,
            SUM(CAST(COALESCE(b.b_sb, '0') AS INTEGER)) AS steals,
            SUM(CAST(COALESCE(b.b_cs, '0') AS INTEGER)) AS caught_stealing,
            SUM(CAST(COALESCE(b.b_w, '0') AS INTEGER)) AS walks,
            SUM(CAST(COALESCE(b.b_k, '0') AS INTEGER)) AS strikeouts,
            SUM(CAST(COALESCE(b.b_hbp, '0') AS INTEGER)) AS hit_by_pitch,
            SUM(CAST(COALESCE(b.b_sh, '0') AS INTEGER)) AS sacrifice_hits,
            SUM(CAST(COALESCE(b.b_sf, '0') AS INTEGER)) AS sacrifice_flies
        FROM retrosheet_batting AS b
        JOIN lahman_people AS p
          ON p.retroid = b.id
        WHERE b.stattype = 'value'
          AND b.gametype IN ('R', 'regular')
          AND COALESCE(p.birthmonth, '') <> ''
          AND COALESCE(p.birthday, '') <> ''
          AND CAST(substr(b.date, 1, 4) AS INTEGER) BETWEEN ? AND ?
          AND substr(b.date, 5, 2) = printf('%02d', CAST(p.birthmonth AS INTEGER))
          AND substr(b.date, 7, 2) = printf('%02d', CAST(p.birthday AS INTEGER))
        GROUP BY b.id, p.namefirst, p.namelast
        """,
        (query.start_season, query.end_season),
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        at_bats = safe_int(row["at_bats"]) or 0
        walks = safe_int(row["walks"]) or 0
        hit_by_pitch = safe_int(row["hit_by_pitch"]) or 0
        sacrifice_flies = safe_int(row["sacrifice_flies"]) or 0
        sacrifice_hits = safe_int(row["sacrifice_hits"]) or 0
        plate_appearances = safe_int(row["plate_appearances"]) or (at_bats + walks + hit_by_pitch + sacrifice_flies + sacrifice_hits)
        hits = safe_int(row["hits"]) or 0
        doubles = safe_int(row["doubles"]) or 0
        triples = safe_int(row["triples"]) or 0
        home_runs = safe_int(row["home_runs"]) or 0
        avg = (hits / at_bats) if at_bats else None
        obp_denom = at_bats + walks + hit_by_pitch + sacrifice_flies
        obp = ((hits + walks + hit_by_pitch) / obp_denom) if obp_denom else None
        singles = hits - doubles - triples - home_runs
        slg = ((singles + (2 * doubles) + (3 * triples) + (4 * home_runs)) / at_bats) if at_bats else None
        ops = (obp + slg) if obp is not None and slg is not None else None
        metric_value = select_historical_hitting_metric(
            query.metric.key,
            at_bats,
            plate_appearances,
            avg,
            obp,
            slg,
            ops,
            row,
        )
        if metric_value is None:
            continue
        sample_values = {
            "games": safe_int(row["games"]) or 0,
            "plate_appearances": plate_appearances,
            "at_bats": at_bats,
        }
        candidates.append(
            {
                "player_name": build_person_name(row["namefirst"], row["namelast"], row["retro_id"]),
                "metric_value": float(metric_value),
                "sample_size": float(sample_values.get(query.metric.sample_basis or "plate_appearances") or 0.0),
                "games": safe_int(row["games"]) or 0,
                "condition_games": safe_int(row["games"]) or 0,
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
                "first_season": safe_int(row["first_season"]) or 0,
                "last_season": safe_int(row["last_season"]) or 0,
            }
        )
    total_row_count = len(candidates)
    max_condition_games = max((safe_int(row["condition_games"]) or 0) for row in candidates) if candidates else 0
    max_plate_appearances = max((safe_int(row["plate_appearances"]) or 0) for row in candidates) if candidates else 0
    max_at_bats = max((safe_int(row["at_bats"]) or 0) for row in candidates) if candidates else 0
    max_basis_value = 0.0
    if query.minimum_basis:
        max_basis_value = max((float(row.get(query.minimum_basis) or 0.0) for row in candidates), default=0.0)
    if query.minimum_value is not None and query.minimum_basis:
        candidates = [
            row for row in candidates
            if float(row.get(query.minimum_basis) or 0.0) >= float(query.minimum_value)
        ]
    candidates.sort(
        key=lambda row: (
            -float(row["metric_value"]) if query.sort_desc else float(row["metric_value"]),
            -(row.get("sample_size") or 0.0),
            int(row.get("last_season") or 0),
            str(row.get("player_name") or ""),
        )
    )
    for index, row in enumerate(candidates, start=1):
        row["rank"] = index
    metadata = {
        "total_row_count": total_row_count,
        "max_condition_games": max_condition_games,
        "max_plate_appearances": max_plate_appearances,
        "max_at_bats": max_at_bats,
        "max_basis_value": max_basis_value,
    }
    return candidates, metadata


def build_player_game_condition_summary(
    query: PlayerGameConditionQuery,
    rows: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> str:
    qualifier_text = ""
    if query.minimum_value is not None and query.minimum_label:
        qualifier_text = f" among hitters with at least {query.minimum_value} {query.minimum_label}"
    if not rows:
        max_basis_value = metadata.get("max_basis_value")
        max_basis_text = ""
        if query.minimum_label and max_basis_value is not None:
            max_basis_numeric = int(max_basis_value) if float(max_basis_value).is_integer() else float(max_basis_value)
            max_basis_text = f" The full local leaderboard's maximum {query.minimum_label} total was {max_basis_numeric}."
        return (
            f"For {query.scope_label}, no hitters in {query.condition.label}{qualifier_text} qualified "
            f"for a {query.metric.label} ranking.{max_basis_text}"
        )
    leader = rows[0]
    value_text = f"{float(leader['metric_value']):{query.metric.formatter}}"
    summary = (
        f"For {query.scope_label}, the {query.descriptor} hitter by {query.metric.label} in "
        f"{query.condition.label}{qualifier_text} is {leader['player_name']} at {value_text} across "
        f"{leader['condition_games']} game(s) from {leader['first_season']} to {leader['last_season']}."
    )
    trailing = rows[1:4]
    if trailing:
        trailing_text = "; ".join(
            f"{row['player_name']} ({float(row['metric_value']):{query.metric.formatter}})"
            for row in trailing
        )
        summary = f"{summary} Next on the board: {trailing_text}."
    if len(rows) > 12:
        summary = (
            f"{summary} This result was ranked against the full local leaderboard; "
            f"only the top {min(12, len(rows))} rows are shown in the evidence table."
        )
    return summary
