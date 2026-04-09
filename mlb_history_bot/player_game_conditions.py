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
from .team_season_leaders import build_person_name, select_historical_hitting_metric, select_historical_pitching_metric


@dataclass(slots=True, frozen=True)
class PlayerGameConditionSpec:
    key: str
    label: str
    aliases: tuple[str, ...]
    game_label: str
    value_label: str | None = None


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
    condition_value: str | None = None
    breakdown_all_values: bool = False


BIRTHDAY_CONDITION = PlayerGameConditionSpec(
    key="birthday",
    label="games played on a player's birthday",
    aliases=("birthday", "on their birthday", "on his birthday", "on her birthday"),
    game_label="Birthday G",
)

WEEKDAY_CONDITION = PlayerGameConditionSpec(
    key="weekday",
    label="games played on a given day of the week",
    aliases=("day of the week", "weekday", "weekdays"),
    game_label="Weekday G",
    value_label="Weekday",
)

CONDITION_SPECS: tuple[PlayerGameConditionSpec, ...] = (BIRTHDAY_CONDITION, WEEKDAY_CONDITION)
ROLE_HINT_PATTERN = re.compile(r"\b(hitter|batter|offensive player|pitcher|starter|reliever|fielder|defender)\b", re.IGNORECASE)
CONDITION_MINIMUM_QUALIFIERS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("plate_appearances", ("plate appearances", "pa"), "PA"),
    ("at_bats", ("at bats", "ab"), "AB"),
    ("games", ("games", "game"), "birthday games"),
    ("games_started", ("games started", "starts", "gs"), "GS"),
    ("ipouts", ("innings pitched", "innings", "ip"), "IP"),
    ("wins", ("wins", "win"), "Wins"),
    ("losses", ("losses", "loss"), "Losses"),
    ("saves", ("saves", "save"), "Saves"),
    ("hits", ("hits", "hit"), "H"),
    ("home_runs", ("home runs", "home run", "hr", "homers", "homeruns"), "HR"),
    ("walks", ("walks", "walk", "bb"), "BB"),
    ("strikeouts", ("strikeouts", "strikeout", "so"), "SO"),
    ("runs_batted_in", ("rbi", "runs batted in"), "RBI"),
)
WEEKDAY_ORDER: tuple[str, ...] = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
WEEKDAY_NAME_TO_SQLITE: dict[str, str] = {
    "sunday": "0",
    "monday": "1",
    "tuesday": "2",
    "wednesday": "3",
    "thursday": "4",
    "friday": "5",
    "saturday": "6",
}
WEEKDAY_LABEL_TO_ORDER: dict[str, int] = {label: index for index, label in enumerate(WEEKDAY_ORDER, start=1)}


class PlayerGameConditionResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_player_game_condition_query(question, self.settings)
        if query is None:
            return None
        if query.metric.role not in {"hitter", "player", "pitcher"}:
            return None
        rows, metadata = fetch_condition_rows(connection, query)
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
                "condition_value_label": query.condition.value_label,
                "metric": query.metric.label,
                "role": query.metric.role,
                "scope_label": query.scope_label,
                "rows": display_rows,
                "breakdown_all_values": query.breakdown_all_values,
                "condition_value": query.condition_value,
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
    condition, condition_value, breakdown_all_values = find_condition(lowered)
    if condition is None:
        return None
    if "all-star" in lowered or "cy young" in lowered or "gold glove" in lowered:
        return None

    normalized_metric_text = normalize_metric_search_text(strip_qualifier_clauses(lowered))
    metric = find_season_metric(normalized_metric_text)
    if metric is None:
        return None
    if metric.source_family != "historical" or metric.entity_scope != "player":
        return None
    if metric.role not in {"hitter", "player", "pitcher"}:
        return None
    if condition.key == "birthday" and metric.role not in {"hitter", "player"}:
        return None
    if condition.key == "weekday" and metric.role not in {"hitter", "player", "pitcher"}:
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
        condition_value=condition_value,
        breakdown_all_values=breakdown_all_values,
    )


def find_condition(lowered_question: str) -> tuple[PlayerGameConditionSpec | None, str | None, bool]:
    if any(alias in lowered_question for alias in BIRTHDAY_CONDITION.aliases):
        return BIRTHDAY_CONDITION, None, False
    weekday_value = extract_weekday_value(lowered_question)
    if weekday_value is not None:
        return WEEKDAY_CONDITION, weekday_value, False
    if any(alias in lowered_question for alias in WEEKDAY_CONDITION.aliases):
        breakdown = any(token in lowered_question for token in ("each day of the week", "every day of the week", "by day of week"))
        return WEEKDAY_CONDITION, None, breakdown or True
    return None, None, False


def extract_weekday_value(lowered_question: str) -> str | None:
    for label in WEEKDAY_ORDER:
        lowered_label = label.lower()
        if re.search(rf"\b{lowered_label}s?\b", lowered_question):
            return label
    return None


def parse_condition_minimum_qualifier(question: str) -> tuple[str | None, str | None, int | None]:
    for basis, nouns, label in CONDITION_MINIMUM_QUALIFIERS:
        value = extract_minimum_qualifier(question, nouns)
        if value is not None:
            return basis, label, value
    return None, None, None


def fetch_condition_rows(connection, query: PlayerGameConditionQuery) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if query.condition.key == "birthday":
        return fetch_hitting_birthday_rows(connection, query)
    if query.condition.key == "weekday":
        if query.metric.role == "pitcher":
            return fetch_pitching_weekday_rows(connection, query)
        if query.metric.role in {"hitter", "player"}:
            return fetch_hitting_weekday_rows(connection, query)
    return [], {"total_row_count": 0}


def fetch_hitting_birthday_rows(connection, query: PlayerGameConditionQuery) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not (table_exists(connection, "retrosheet_batting") and table_exists(connection, "lahman_people")):
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


def fetch_hitting_weekday_rows(connection, query: PlayerGameConditionQuery) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not (table_exists(connection, "retrosheet_batting") and table_exists(connection, "retrosheet_allplayers")):
        return [], {"total_row_count": 0}
    weekday_filter_sql = ""
    params: list[Any] = [query.start_season, query.end_season]
    if query.condition_value is not None:
        weekday_filter_sql = "AND strftime('%w', substr(b.date, 1, 4) || '-' || substr(b.date, 5, 2) || '-' || substr(b.date, 7, 2)) = ?"
        params.append(WEEKDAY_NAME_TO_SQLITE[query.condition_value.lower()])
    rows = connection.execute(
        f"""
        WITH names AS (
            SELECT id, MIN(first) AS first, MIN(last) AS last
            FROM retrosheet_allplayers
            GROUP BY id
        ),
        weekday_games AS (
            SELECT
                b.id AS retro_id,
                names.first,
                names.last,
                CASE strftime('%w', substr(b.date, 1, 4) || '-' || substr(b.date, 5, 2) || '-' || substr(b.date, 7, 2))
                    WHEN '1' THEN 'Monday'
                    WHEN '2' THEN 'Tuesday'
                    WHEN '3' THEN 'Wednesday'
                    WHEN '4' THEN 'Thursday'
                    WHEN '5' THEN 'Friday'
                    WHEN '6' THEN 'Saturday'
                    ELSE 'Sunday'
                END AS condition_value,
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
            LEFT JOIN names
              ON names.id = b.id
            WHERE b.stattype = 'value'
              AND b.gametype IN ('R', 'regular')
              AND CAST(substr(b.date, 1, 4) AS INTEGER) BETWEEN ? AND ?
              {weekday_filter_sql}
            GROUP BY b.id, names.first, names.last, condition_value
        )
        SELECT * FROM weekday_games
        """,
        params,
    ).fetchall()
    return rank_hitting_condition_rows(rows, query)


def fetch_pitching_weekday_rows(connection, query: PlayerGameConditionQuery) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not (table_exists(connection, "retrosheet_pitching") and table_exists(connection, "retrosheet_allplayers")):
        return [], {"total_row_count": 0}
    weekday_filter_sql = ""
    params: list[Any] = [query.start_season, query.end_season]
    if query.condition_value is not None:
        weekday_filter_sql = "AND strftime('%w', substr(p.date, 1, 4) || '-' || substr(p.date, 5, 2) || '-' || substr(p.date, 7, 2)) = ?"
        params.append(WEEKDAY_NAME_TO_SQLITE[query.condition_value.lower()])
    rows = connection.execute(
        f"""
        WITH names AS (
            SELECT id, MIN(first) AS first, MIN(last) AS last
            FROM retrosheet_allplayers
            GROUP BY id
        ),
        weekday_games AS (
            SELECT
                p.id AS retro_id,
                names.first,
                names.last,
                CASE strftime('%w', substr(p.date, 1, 4) || '-' || substr(p.date, 5, 2) || '-' || substr(p.date, 7, 2))
                    WHEN '1' THEN 'Monday'
                    WHEN '2' THEN 'Tuesday'
                    WHEN '3' THEN 'Wednesday'
                    WHEN '4' THEN 'Thursday'
                    WHEN '5' THEN 'Friday'
                    WHEN '6' THEN 'Saturday'
                    ELSE 'Sunday'
                END AS condition_value,
                MIN(CAST(substr(p.date, 1, 4) AS INTEGER)) AS first_season,
                MAX(CAST(substr(p.date, 1, 4) AS INTEGER)) AS last_season,
                COUNT(*) AS games,
                SUM(CAST(COALESCE(p.p_gs, '0') AS INTEGER)) AS games_started,
                SUM(CAST(COALESCE(p.p_ipouts, '0') AS INTEGER)) AS ipouts,
                SUM(CAST(COALESCE(p.p_h, '0') AS INTEGER)) AS hits_allowed,
                SUM(CAST(COALESCE(p.p_hr, '0') AS INTEGER)) AS home_runs_allowed,
                SUM(CAST(COALESCE(p.p_r, '0') AS INTEGER)) AS runs_allowed,
                SUM(CAST(COALESCE(p.p_er, '0') AS INTEGER)) AS earned_runs,
                SUM(CAST(COALESCE(p.p_w, '0') AS INTEGER)) + SUM(CAST(COALESCE(p.p_iw, '0') AS INTEGER)) AS walks,
                SUM(CAST(COALESCE(p.p_hbp, '0') AS INTEGER)) AS hit_by_pitch,
                SUM(CAST(COALESCE(p.p_k, '0') AS INTEGER)) AS strikeouts,
                SUM(CAST(COALESCE(p.wp, '0') AS INTEGER)) AS wins,
                SUM(CAST(COALESCE(p.lp, '0') AS INTEGER)) AS losses,
                SUM(CAST(COALESCE(p.save, '0') AS INTEGER)) AS saves
            FROM retrosheet_pitching AS p
            LEFT JOIN names
              ON names.id = p.id
            WHERE p.stattype = 'value'
              AND p.gametype IN ('R', 'regular')
              AND CAST(substr(p.date, 1, 4) AS INTEGER) BETWEEN ? AND ?
              {weekday_filter_sql}
            GROUP BY p.id, names.first, names.last, condition_value
        )
        SELECT * FROM weekday_games
        """,
        params,
    ).fetchall()
    return rank_pitching_condition_rows(rows, query)


def rank_hitting_condition_rows(rows, query: PlayerGameConditionQuery) -> tuple[list[dict[str, Any]], dict[str, Any]]:
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
            "hits": hits,
            "home_runs": home_runs,
            "walks": walks,
            "strikeouts": safe_int(row["strikeouts"]) or 0,
            "runs_batted_in": safe_int(row["rbi"]) or 0,
        }
        candidates.append(
            {
                "player_name": build_person_name(row["first"], row["last"], row["retro_id"]),
                "condition_value": str(row["condition_value"] or ""),
                "condition_order": WEEKDAY_LABEL_TO_ORDER.get(str(row["condition_value"] or ""), 99),
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
    return finalize_condition_candidates(candidates, query)


def rank_pitching_condition_rows(rows, query: PlayerGameConditionQuery) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        ipouts = safe_int(row["ipouts"]) or 0
        games = safe_int(row["games"]) or 0
        games_started = safe_int(row["games_started"]) or 0
        wins = safe_int(row["wins"]) or 0
        losses = safe_int(row["losses"]) or 0
        saves = safe_int(row["saves"]) or 0
        walks = safe_int(row["walks"]) or 0
        strikeouts = safe_int(row["strikeouts"]) or 0
        hits_allowed = safe_int(row["hits_allowed"]) or 0
        home_runs_allowed = safe_int(row["home_runs_allowed"]) or 0
        earned_runs = safe_int(row["earned_runs"]) or 0
        metric_source = {
            "games": games,
            "games_started": games_started,
            "wins": wins,
            "losses": losses,
            "saves": saves,
            "hits_allowed": hits_allowed,
            "earned_runs": earned_runs,
            "home_runs_allowed": home_runs_allowed,
            "walks": walks,
            "strikeouts": strikeouts,
            "hit_by_pitch": safe_int(row["hit_by_pitch"]) or 0,
        }
        metric_value = select_historical_pitching_metric(query.metric.key, ipouts, metric_source)
        if metric_value is None:
            continue
        sample_values = {
            "games": games,
            "games_started": games_started,
            "ipouts": ipouts,
            "wins": wins,
            "losses": losses,
            "saves": saves,
            "walks": walks,
            "strikeouts": strikeouts,
        }
        candidates.append(
            {
                "player_name": build_person_name(row["first"], row["last"], row["retro_id"]),
                "condition_value": str(row["condition_value"] or ""),
                "condition_order": WEEKDAY_LABEL_TO_ORDER.get(str(row["condition_value"] or ""), 99),
                "metric_value": float(metric_value),
                "sample_size": float(sample_values.get(query.minimum_basis or query.metric.sample_basis or "games") or 0.0),
                "games": games,
                "condition_games": games,
                "games_started": games_started,
                "ipouts": ipouts,
                "innings": ipouts / 3.0 if ipouts else 0.0,
                "wins": wins,
                "losses": losses,
                "saves": saves,
                "walks": walks,
                "strikeouts": strikeouts,
                "hits_allowed": hits_allowed,
                "home_runs_allowed": home_runs_allowed,
                "earned_runs": earned_runs,
                "era": (27.0 * earned_runs / ipouts) if ipouts else None,
                "whip": ((hits_allowed + walks) / (ipouts / 3.0)) if ipouts else None,
                "first_season": safe_int(row["first_season"]) or 0,
                "last_season": safe_int(row["last_season"]) or 0,
            }
        )
    return finalize_condition_candidates(candidates, query)


def finalize_condition_candidates(
    candidates: list[dict[str, Any]],
    query: PlayerGameConditionQuery,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    total_row_count = len(candidates)
    max_condition_games = max((safe_int(row["condition_games"]) or 0) for row in candidates) if candidates else 0
    max_plate_appearances = max((safe_int(row.get("plate_appearances")) or 0) for row in candidates) if candidates else 0
    max_at_bats = max((safe_int(row.get("at_bats")) or 0) for row in candidates) if candidates else 0
    max_basis_value = 0.0
    if query.minimum_basis:
        max_basis_value = max((float(row.get(query.minimum_basis) or 0.0) for row in candidates), default=0.0)
    if query.minimum_value is not None and query.minimum_basis:
        candidates = [
            row for row in candidates
            if float(row.get(query.minimum_basis) or 0.0) >= float(query.minimum_value)
        ]

    if query.breakdown_all_values:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in candidates:
            grouped.setdefault(str(row.get("condition_value") or ""), []).append(row)
        breakdown_rows: list[dict[str, Any]] = []
        for condition_value, rows_for_value in grouped.items():
            rows_for_value.sort(
                key=lambda row: (
                    -float(row["metric_value"]) if query.sort_desc else float(row["metric_value"]),
                    -(row.get("sample_size") or 0.0),
                    int(row.get("last_season") or 0),
                    str(row.get("player_name") or ""),
                )
            )
            top_row = dict(rows_for_value[0])
            top_row["condition_value"] = condition_value
            top_row["condition_order"] = WEEKDAY_LABEL_TO_ORDER.get(condition_value, 99)
            breakdown_rows.append(top_row)
        breakdown_rows.sort(key=lambda row: int(row.get("condition_order") or 99))
        for index, row in enumerate(breakdown_rows, start=1):
            row["rank"] = index
        metadata = {
            "total_row_count": total_row_count,
            "max_condition_games": max_condition_games,
            "max_plate_appearances": max_plate_appearances,
            "max_at_bats": max_at_bats,
            "max_basis_value": max_basis_value,
        }
        return breakdown_rows, metadata

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
    role_label = "pitcher" if query.metric.role == "pitcher" else "hitter"
    qualifier_text = ""
    if query.minimum_value is not None and query.minimum_label:
        qualifier_text = f" among {role_label}s with at least {query.minimum_value} {query.minimum_label}"
    if query.breakdown_all_values and rows:
        lead_bits = "; ".join(
            f"{row['condition_value']}: {row['player_name']} ({float(row['metric_value']):{query.metric.formatter}})"
            for row in rows
        )
        return (
            f"For {query.scope_label}, the {role_label} leaders by {query.condition.value_label or 'condition'} "
            f"for {query.metric.label}{qualifier_text} are: {lead_bits}."
        )
    if not rows:
        max_basis_value = metadata.get("max_basis_value")
        max_basis_text = ""
        if query.minimum_label and max_basis_value is not None:
            max_basis_numeric = int(max_basis_value) if float(max_basis_value).is_integer() else float(max_basis_value)
            max_basis_text = f" The full local leaderboard's maximum {query.minimum_label} total was {max_basis_numeric}."
        return (
            f"For {query.scope_label}, no {role_label}s in {query.condition.label}{qualifier_text} qualified "
            f"for a {query.metric.label} ranking.{max_basis_text}"
        )
    leader = rows[0]
    value_text = f"{float(leader['metric_value']):{query.metric.formatter}}"
    summary = (
        f"For {query.scope_label}, the {query.descriptor} {role_label} by {query.metric.label} in "
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
