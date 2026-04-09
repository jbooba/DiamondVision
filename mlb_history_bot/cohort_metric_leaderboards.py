from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re
from typing import Any

from .cohort_timeline import CohortFilter, ResolvedCohort, parse_cohort_filter, resolve_cohort_filter
from .config import Settings
from .models import EvidenceSnippet
from .query_intent import detect_ranking_intent
from .query_utils import extract_referenced_season, question_requests_current_scope
from .relationship_ontology import ROLE_HINTS
from .season_metric_leaderboards import (
    SEASON_METRICS,
    SeasonMetricSpec,
    find_season_metric,
    passes_sample_threshold,
    statcast_batter_metric_values,
)
from .storage import list_table_columns, table_exists
from .team_evaluator import safe_float, safe_int
from .team_season_leaders import (
    build_person_name,
    outs_to_innings_notation,
    select_historical_fielding_metric,
    select_historical_hitting_metric,
    select_historical_pitching_metric,
)


OFFENSE_HINTS = ("offensive", "offense", "hitting", "hitter", "batting", "lineup")
DEFENSE_HINTS = ("defensive", "defense", "fielding", "fielder", "glove")
PITCHING_HINTS = ("pitching", "pitcher", "starter", "rotation", "reliever", "bullpen", "closer")
RELATIONAL_OPPONENT_HINTS = (" against ", " versus ", " vs ", " facing ", " while facing ", " when facing ")


@dataclass(slots=True)
class CohortMetricQuery:
    cohort: ResolvedCohort
    metric: SeasonMetricSpec
    descriptor: str
    sort_desc: bool
    role: str
    start_season: int | None
    end_season: int | None
    scope_label: str


class CohortMetricLeaderboardResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_cohort_metric_query(connection, question, self.settings)
        if query is None:
            return None
        if query.metric.source_family == "statcast":
            rows = fetch_statcast_rows(connection, query)
        elif query.role in {"pitcher", "starter", "reliever"}:
            rows = fetch_pitching_rows(connection, query)
        elif query.role == "fielder":
            rows = fetch_fielding_rows(connection, query)
        else:
            rows = fetch_hitting_rows(connection, query)
        if not rows:
            if query.metric.source_family == "statcast":
                return build_statcast_cohort_gap_snippet(connection, query)
            return None
        leader = rows[0]
        summary = build_cohort_metric_summary(query, leader, rows[1:4])
        return EvidenceSnippet(
            source="Cohort Metric Leaderboards",
            title=f"{query.cohort.label} {query.metric.label} leaderboard",
            citation=build_citation(query),
            summary=summary,
            payload={
                "analysis_type": "cohort_metric_leaderboard",
                "mode": "historical",
                "cohort_kind": query.cohort.kind,
                "cohort_label": query.cohort.label,
                "metric": query.metric.label,
                "role": query.role,
                "source_family": query.metric.source_family,
                "scope_label": query.scope_label,
                "rows": rows[:12],
            },
        )


def parse_cohort_metric_query(connection, question: str, settings: Settings) -> CohortMetricQuery | None:
    cohort_filter = parse_cohort_filter(question)
    if cohort_filter is None:
        return None
    lowered = f" {question.lower()} "
    if any(token in lowered for token in RELATIONAL_OPPONENT_HINTS):
        return None
    cohort = resolve_cohort_filter(connection, cohort_filter)
    if cohort is None:
        return None
    metric_question = re.sub(r"[?.!,:'\"]", " ", lowered)
    metric = find_season_metric(metric_question)
    if cohort.kind == "award_winner" and metric is None:
        return None
    if metric is None or metric.entity_scope != "player" or metric.source_family not in {"historical", "statcast"}:
        metric = default_metric_for_cohort(question)
    if metric is None:
        return None

    ranking_intent = detect_ranking_intent(
        lowered,
        higher_is_better=metric.higher_is_better,
        require_hint=False,
    )
    if ranking_intent is None:
        return None

    role = infer_role(question, metric)
    if role is None:
        return None

    start_season = None
    end_season = None
    scope_label = cohort.label
    if cohort.kind != "manager_era":
        current_season = settings.live_season or date.today().year
        referenced_season = extract_referenced_season(question, current_season)
        if referenced_season is not None:
            start_season = referenced_season
            end_season = referenced_season
            scope_label = f"{cohort.label} in {referenced_season}"
        elif question_requests_current_scope(question):
            start_season = current_season
            end_season = current_season
            scope_label = f"{cohort.label} in {current_season}"
        else:
            scope_label = f"{cohort.label} career totals"
    elif cohort.seasons:
        scope_label = format_cohort_span_label(cohort)

    return CohortMetricQuery(
        cohort=cohort,
        metric=metric,
        descriptor=ranking_intent.descriptor,
        sort_desc=ranking_intent.sort_desc,
        role=role,
        start_season=start_season,
        end_season=end_season,
        scope_label=scope_label,
    )


def infer_role(question: str, metric: SeasonMetricSpec) -> str | None:
    if metric.role in {"pitcher", "fielder", "hitter"}:
        return metric.role
    lowered = question.lower()
    if any(token in lowered for token in DEFENSE_HINTS):
        return "fielder"
    if any(token in lowered for token in PITCHING_HINTS):
        return "pitcher"
    if any(token in lowered for token in OFFENSE_HINTS):
        return "hitter"
    if any(token in lowered for token in ROLE_HINTS):
        return metric.role
    return "hitter"


def default_metric_for_cohort(question: str) -> SeasonMetricSpec | None:
    lowered = question.lower()
    if any(token in lowered for token in DEFENSE_HINTS):
        return find_metric_by_key("fielding_pct", role="fielder")
    if any(token in lowered for token in PITCHING_HINTS):
        return find_metric_by_key("era", role="pitcher")
    return find_metric_by_key("ops", role="hitter")


def find_metric_by_key(metric_key: str, *, role: str) -> SeasonMetricSpec | None:
    for spec in SEASON_METRICS:
        if spec.source_family == "historical" and spec.entity_scope == "player" and spec.role == role and spec.key == metric_key:
            return spec
    return None


def build_where_clauses(
    query: CohortMetricQuery,
    *,
    season_column: str,
    team_column: str,
    player_column: str,
    player_name_expr: str,
) -> tuple[str, tuple[Any, ...]]:
    clauses = ["1=1"]
    parameters: list[Any] = []
    if query.cohort.kind == "manager_era":
        if query.cohort.seasons:
            placeholders = ", ".join("?" for _ in query.cohort.seasons)
            clauses.append(f"CAST({season_column} AS INTEGER) IN ({placeholders})")
            parameters.extend(query.cohort.seasons)
        if query.cohort.team_code:
            clauses.append(f"lower({team_column}) = ?")
            parameters.append(query.cohort.team_code.lower())
    if query.cohort.player_ids:
        placeholders = ", ".join("?" for _ in query.cohort.player_ids)
        clauses.append(f"{player_column} IN ({placeholders})")
        parameters.extend(sorted(query.cohort.player_ids))
    elif query.cohort.player_names:
        placeholders = ", ".join("?" for _ in query.cohort.player_names)
        clauses.append(f"lower(trim({player_name_expr})) IN ({placeholders})")
        parameters.extend(sorted(query.cohort.player_names))
    if query.start_season is not None:
        clauses.append(f"CAST({season_column} AS INTEGER) >= ?")
        parameters.append(query.start_season)
    if query.end_season is not None:
        clauses.append(f"CAST({season_column} AS INTEGER) <= ?")
        parameters.append(query.end_season)
    return " AND ".join(clauses), tuple(parameters)


def fetch_hitting_rows(connection, query: CohortMetricQuery) -> list[dict[str, Any]]:
    if not (table_exists(connection, "lahman_batting") and table_exists(connection, "lahman_people")):
        return []
    where_clause, parameters = build_where_clauses(
        query,
        season_column="b.yearid",
        team_column="b.teamid",
        player_column="b.playerid",
        player_name_expr="coalesce(p.namefirst, '') || ' ' || coalesce(p.namelast, '')",
    )
    rows = connection.execute(
        f"""
        SELECT
            b.playerid,
            p.namefirst,
            p.namelast,
            MIN(CAST(b.yearid AS INTEGER)) AS first_season,
            MAX(CAST(b.yearid AS INTEGER)) AS last_season,
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
        JOIN lahman_people AS p
          ON p.playerid = b.playerid
        WHERE {where_clause}
        GROUP BY b.playerid, p.namefirst, p.namelast
        """,
        parameters,
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        at_bats = safe_int(row["at_bats"]) or 0
        walks = safe_int(row["walks"]) or 0
        hit_by_pitch = safe_int(row["hit_by_pitch"]) or 0
        sacrifice_flies = safe_int(row["sacrifice_flies"]) or 0
        sacrifice_hits = safe_int(row["sacrifice_hits"]) or 0
        plate_appearances = at_bats + walks + hit_by_pitch + sacrifice_flies + sacrifice_hits
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
            "games": safe_float(row["games"]),
            "plate_appearances": float(plate_appearances),
            "at_bats": float(at_bats),
        }
        if not passes_sample_threshold(query.metric, sample_values):
            continue
        candidates.append(
            {
                "player_name": build_person_name(row["namefirst"], row["namelast"], row["playerid"]),
                "metric_value": float(metric_value),
                "sample_size": float(sample_values.get(query.metric.sample_basis or "plate_appearances") or 0.0),
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
                "first_season": safe_int(row["first_season"]) or 0,
                "last_season": safe_int(row["last_season"]) or 0,
            }
        )
    return rank_rows(candidates, query)


def fetch_statcast_rows(connection, query: CohortMetricQuery) -> list[dict[str, Any]]:
    if query.role not in {"hitter", "player"}:
        return []
    return fetch_statcast_hitting_rows(connection, query)


def fetch_statcast_hitting_rows(connection, query: CohortMetricQuery) -> list[dict[str, Any]]:
    row_sources = (
        (fetch_statcast_hitting_rows_from_events, fetch_statcast_hitting_summary_rows)
        if query.cohort.kind == "bat_handedness"
        else (fetch_statcast_hitting_summary_rows, fetch_statcast_hitting_rows_from_events)
    )
    for fetcher in row_sources:
        rows = fetcher(connection, query)
        candidates = build_statcast_candidates(rows, query)
        if candidates:
            return rank_rows(candidates, query)
    return []


def build_statcast_candidates(rows, query: CohortMetricQuery) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        metrics = statcast_batter_metric_values(row)
        if not passes_sample_threshold(query.metric, metrics):
            continue
        metric_value = metrics.get(query.metric.key)
        if metric_value is None:
            continue
        candidates.append(
            {
                "player_name": str(row["player_name"] or ""),
                "metric_value": float(metric_value),
                "sample_size": float(metrics.get(query.metric.sample_basis or "plate_appearances") or 0.0),
                "team": str(row["team"] or ""),
                "plate_appearances": safe_int(row["plate_appearances"]) or 0,
                "at_bats": safe_int(row["at_bats"]) or 0,
                "hits": safe_int(row["hits"]) or 0,
                "doubles": safe_int(row["doubles"]) or 0,
                "triples": safe_int(row["triples"]) or 0,
                "home_runs": safe_int(row["home_runs"]) or 0,
                "walks": safe_int(row["walks"]) or 0,
                "strikeouts": safe_int(row["strikeouts"]) or 0,
                "runs_batted_in": safe_int(row["runs_batted_in"]) or 0,
                "avg": metrics.get("avg"),
                "obp": metrics.get("obp"),
                "slg": metrics.get("slg"),
                "ops": metrics.get("ops"),
                "xBA": metrics.get("xba"),
                "xwOBA": metrics.get("xwoba"),
                "xSLG": metrics.get("xslg"),
                "hard_hit_rate": metrics.get("hard_hit_rate"),
                "barrel_rate": metrics.get("barrel_rate"),
                "avg_exit_velocity": metrics.get("avg_exit_velocity"),
                "max_exit_velocity": metrics.get("max_exit_velocity"),
                "avg_bat_speed": metrics.get("avg_bat_speed"),
                "max_bat_speed": metrics.get("max_bat_speed"),
                "first_season": safe_int(row["first_season"]) or 0,
                "last_season": safe_int(row["last_season"]) or 0,
            }
        )
    return candidates


def fetch_statcast_hitting_summary_rows(connection, query: CohortMetricQuery):
    if not table_exists(connection, "statcast_batter_games"):
        return []
    identity_sql, identity_params = build_statcast_identity_filter(query.cohort)
    if identity_sql is None:
        return []
    clauses = [identity_sql]
    parameters: list[Any] = list(identity_params)
    if query.start_season is not None:
        clauses.append("season >= ?")
        parameters.append(query.start_season)
    if query.end_season is not None:
        clauses.append("season <= ?")
        parameters.append(query.end_season)
    return connection.execute(
        f"""
        SELECT
            batter_id,
            MIN(batter_name) AS player_name,
            MIN(season) AS first_season,
            MAX(season) AS last_season,
            CASE WHEN COUNT(DISTINCT upper(team)) = 1 THEN MIN(upper(team)) ELSE 'MULTI' END AS team,
            COUNT(DISTINCT game_pk) AS games,
            SUM(plate_appearances) AS plate_appearances,
            SUM(at_bats) AS at_bats,
            SUM(hits) AS hits,
            SUM(singles) AS singles,
            SUM(doubles) AS doubles,
            SUM(triples) AS triples,
            SUM(home_runs) AS home_runs,
            SUM(walks) AS walks,
            SUM(strikeouts) AS strikeouts,
            SUM(runs_batted_in) AS runs_batted_in,
            SUM(batted_ball_events) AS batted_ball_events,
            SUM(xba_numerator) AS xba_numerator,
            SUM(xwoba_numerator) AS xwoba_numerator,
            SUM(xwoba_denom) AS xwoba_denom,
            SUM(xslg_numerator) AS xslg_numerator,
            SUM(hard_hit_bbe) AS hard_hit_bbe,
            SUM(barrel_bbe) AS barrel_bbe,
            SUM(launch_speed_sum) AS launch_speed_sum,
            SUM(launch_speed_count) AS launch_speed_count,
            MAX(max_launch_speed) AS max_launch_speed,
            AVG(avg_bat_speed) AS avg_bat_speed,
            MAX(max_bat_speed) AS max_bat_speed
        FROM statcast_batter_games
        WHERE {' AND '.join(clauses)}
        GROUP BY batter_id
        """,
        tuple(parameters),
    ).fetchall()


def fetch_statcast_hitting_rows_from_events(connection, query: CohortMetricQuery):
    if not table_exists(connection, "statcast_events"):
        return []
    identity_sql, identity_params = build_statcast_event_identity_filter(query.cohort)
    if identity_sql is None:
        return []
    clauses = [identity_sql, "event <> ''"]
    parameters: list[Any] = list(identity_params)
    if query.start_season is not None:
        clauses.append("season >= ?")
        parameters.append(query.start_season)
    if query.end_season is not None:
        clauses.append("season <= ?")
        parameters.append(query.end_season)
    return connection.execute(
        f"""
        SELECT
            batter_id,
            MIN(batter_name) AS player_name,
            MIN(season) AS first_season,
            MAX(season) AS last_season,
            CASE WHEN COUNT(DISTINCT upper(batting_team)) = 1 THEN MIN(upper(batting_team)) ELSE 'MULTI' END AS team,
            COUNT(DISTINCT game_pk) AS games,
            COUNT(*) AS plate_appearances,
            SUM(is_ab) AS at_bats,
            SUM(is_hit) AS hits,
            SUM(CASE WHEN event = 'single' THEN 1 ELSE 0 END) AS singles,
            SUM(CASE WHEN event = 'double' THEN 1 ELSE 0 END) AS doubles,
            SUM(CASE WHEN event = 'triple' THEN 1 ELSE 0 END) AS triples,
            SUM(CASE WHEN event = 'home_run' THEN 1 ELSE 0 END) AS home_runs,
            SUM(CASE WHEN event IN ('walk', 'intent_walk') THEN 1 ELSE 0 END) AS walks,
            SUM(is_strikeout) AS strikeouts,
            SUM(runs_batted_in) AS runs_batted_in,
            SUM(CASE WHEN launch_speed IS NOT NULL THEN 1 ELSE 0 END) AS batted_ball_events,
            SUM(COALESCE(estimated_ba, 0.0)) AS xba_numerator,
            SUM(COALESCE(estimated_woba, 0.0)) AS xwoba_numerator,
            SUM(CASE WHEN estimated_woba IS NOT NULL THEN 1 ELSE 0 END) AS xwoba_denom,
            SUM(COALESCE(estimated_slg, 0.0)) AS xslg_numerator,
            SUM(CASE WHEN launch_speed >= 95 THEN 1 ELSE 0 END) AS hard_hit_bbe,
            SUM(CASE WHEN launch_speed IS NOT NULL AND launch_angle IS NOT NULL AND launch_speed >= 98 AND launch_angle BETWEEN 26 AND 30 THEN 1 ELSE 0 END) AS barrel_bbe,
            SUM(COALESCE(launch_speed, 0.0)) AS launch_speed_sum,
            SUM(CASE WHEN launch_speed IS NOT NULL THEN 1 ELSE 0 END) AS launch_speed_count,
            MAX(launch_speed) AS max_launch_speed,
            AVG(bat_speed) AS avg_bat_speed,
            MAX(bat_speed) AS max_bat_speed
        FROM statcast_events
        WHERE {' AND '.join(clauses)}
        GROUP BY batter_id
        """,
        tuple(parameters),
    ).fetchall()


def build_statcast_identity_filter(cohort: ResolvedCohort) -> tuple[str | None, tuple[Any, ...]]:
    clauses: list[str] = []
    parameters: list[Any] = []
    if cohort.kind == "manager_era":
        if cohort.team_name:
            clauses.append("lower(trim(team_name)) = ?")
            parameters.append(str(cohort.team_name).strip().lower())
        if cohort.team_code:
            clauses.append("upper(team) = ?")
            parameters.append(str(cohort.team_code).strip().upper())
    player_names = sorted({name.strip().lower() for name in (cohort.player_names or set()) if str(name).strip()})
    if player_names:
        placeholders = ", ".join("?" for _ in player_names)
        clauses.append(f"lower(trim(batter_name)) IN ({placeholders})")
        parameters.extend(player_names)
    if not clauses:
        return None, tuple()
    return "(" + " OR ".join(clauses) + ")", tuple(parameters)


def build_statcast_event_identity_filter(cohort: ResolvedCohort) -> tuple[str | None, tuple[Any, ...]]:
    clauses: list[str] = []
    parameters: list[Any] = []
    if cohort.kind == "manager_era" and cohort.team_code:
        clauses.append("upper(batting_team) = ?")
        parameters.append(str(cohort.team_code).strip().upper())
    stand_filter = normalize_statcast_stand_filter(cohort.bats_filter)
    if stand_filter:
        placeholders = ", ".join("?" for _ in stand_filter)
        clauses.append(f"upper(stand) IN ({placeholders})")
        parameters.extend(stand_filter)
    player_names = sorted({name.strip().lower() for name in (cohort.player_names or set()) if str(name).strip()})
    if player_names:
        placeholders = ", ".join("?" for _ in player_names)
        clauses.append(f"lower(trim(batter_name)) IN ({placeholders})")
        parameters.extend(player_names)
    if not clauses:
        return None, tuple()
    return "(" + " OR ".join(clauses) + ")", tuple(parameters)


def normalize_statcast_stand_filter(bats_filter: tuple[str, ...] | None) -> tuple[str, ...]:
    if not bats_filter:
        return tuple()
    values: list[str] = []
    for code in bats_filter:
        normalized = str(code or "").strip().upper()
        if normalized in {"B", "S"}:
            normalized = "S"
        if normalized in {"L", "R", "S"} and normalized not in values:
            values.append(normalized)
    return tuple(values)


def fetch_pitching_rows(connection, query: CohortMetricQuery) -> list[dict[str, Any]]:
    if not (table_exists(connection, "lahman_pitching") and table_exists(connection, "lahman_people")):
        return []
    pitching_columns = {column.lower() for column in list_table_columns(connection, "lahman_pitching")}
    hit_by_pitch_expr = (
        "SUM(CAST(COALESCE(pch.hbp, '0') AS INTEGER))"
        if "hbp" in pitching_columns
        else "0"
    )
    where_clause, parameters = build_where_clauses(
        query,
        season_column="pch.yearid",
        team_column="pch.teamid",
        player_column="pch.playerid",
        player_name_expr="coalesce(ppl.namefirst, '') || ' ' || coalesce(ppl.namelast, '')",
    )
    rows = connection.execute(
        f"""
        SELECT
            pch.playerid,
            ppl.namefirst,
            ppl.namelast,
            MIN(CAST(pch.yearid AS INTEGER)) AS first_season,
            MAX(CAST(pch.yearid AS INTEGER)) AS last_season,
            SUM(CAST(COALESCE(pch.w, '0') AS INTEGER)) AS wins,
            SUM(CAST(COALESCE(pch.l, '0') AS INTEGER)) AS losses,
            SUM(CAST(COALESCE(pch.g, '0') AS INTEGER)) AS games,
            SUM(CAST(COALESCE(pch.gs, '0') AS INTEGER)) AS games_started,
            SUM(CAST(COALESCE(pch.sv, '0') AS INTEGER)) AS saves,
            SUM(CAST(COALESCE(pch.ipouts, '0') AS INTEGER)) AS ipouts,
            SUM(CAST(COALESCE(pch.h, '0') AS INTEGER)) AS hits_allowed,
            SUM(CAST(COALESCE(pch.er, '0') AS INTEGER)) AS earned_runs,
            SUM(CAST(COALESCE(pch.hr, '0') AS INTEGER)) AS home_runs_allowed,
            SUM(CAST(COALESCE(pch.bb, '0') AS INTEGER)) AS walks,
            SUM(CAST(COALESCE(pch.so, '0') AS INTEGER)) AS strikeouts,
            {hit_by_pitch_expr} AS hit_by_pitch
        FROM lahman_pitching AS pch
        JOIN lahman_people AS ppl
          ON ppl.playerid = pch.playerid
        WHERE {where_clause}
        GROUP BY pch.playerid, ppl.namefirst, ppl.namelast
        """,
        parameters,
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        games = safe_int(row["games"]) or 0
        games_started = safe_int(row["games_started"]) or 0
        if query.role == "starter" and games_started <= 0:
            continue
        if query.role == "reliever" and (games - games_started) <= 0:
            continue
        ipouts = safe_int(row["ipouts"]) or 0
        metric_value = select_historical_pitching_metric(query.metric.key, ipouts, row)
        if metric_value is None:
            continue
        sample_values = {
            "games": safe_float(row["games"]),
            "games_started": safe_float(row["games_started"]),
            "ipouts": float(ipouts),
        }
        if not passes_sample_threshold(query.metric, sample_values):
            continue
        hits_allowed = safe_int(row["hits_allowed"]) or 0
        walks = safe_int(row["walks"]) or 0
        strikeouts = safe_int(row["strikeouts"]) or 0
        candidates.append(
            {
                "player_name": build_person_name(row["namefirst"], row["namelast"], row["playerid"]),
                "metric_value": float(metric_value),
                "sample_size": float(sample_values.get(query.metric.sample_basis or "ipouts") or 0.0),
                "games": games,
                "games_started": games_started,
                "innings": outs_to_innings_notation(ipouts),
                "era": (27.0 * (safe_int(row["earned_runs"]) or 0) / ipouts) if ipouts else None,
                "whip": ((hits_allowed + walks) / (ipouts / 3.0)) if ipouts else None,
                "wins": safe_int(row["wins"]) or 0,
                "losses": safe_int(row["losses"]) or 0,
                "saves": safe_int(row["saves"]) or 0,
                "hits_allowed": hits_allowed,
                "earned_runs": safe_int(row["earned_runs"]) or 0,
                "home_runs_allowed": safe_int(row["home_runs_allowed"]) or 0,
                "walks": walks,
                "strikeouts": strikeouts,
                "strikeouts_per_9": ((27.0 * strikeouts) / ipouts) if ipouts else None,
                "walks_per_9": ((27.0 * walks) / ipouts) if ipouts else None,
                "hits_per_9": ((27.0 * hits_allowed) / ipouts) if ipouts else None,
                "home_runs_per_9": ((27.0 * (safe_int(row["home_runs_allowed"]) or 0)) / ipouts) if ipouts else None,
                "strikeout_to_walk": (strikeouts / walks) if walks else None,
                "first_season": safe_int(row["first_season"]) or 0,
                "last_season": safe_int(row["last_season"]) or 0,
            }
        )
    return rank_rows(candidates, query)


def fetch_fielding_rows(connection, query: CohortMetricQuery) -> list[dict[str, Any]]:
    if not (table_exists(connection, "lahman_fielding") and table_exists(connection, "lahman_people")):
        return []
    where_clause, parameters = build_where_clauses(
        query,
        season_column="fld.yearid",
        team_column="fld.teamid",
        player_column="fld.playerid",
        player_name_expr="coalesce(ppl.namefirst, '') || ' ' || coalesce(ppl.namelast, '')",
    )
    rows = connection.execute(
        f"""
        SELECT
            fld.playerid,
            ppl.namefirst,
            ppl.namelast,
            MIN(CAST(fld.yearid AS INTEGER)) AS first_season,
            MAX(CAST(fld.yearid AS INTEGER)) AS last_season,
            GROUP_CONCAT(DISTINCT fld.pos) AS positions,
            SUM(CAST(COALESCE(fld.g, '0') AS INTEGER)) AS games,
            SUM(CAST(COALESCE(fld.po, '0') AS INTEGER)) AS putouts,
            SUM(CAST(COALESCE(fld.a, '0') AS INTEGER)) AS assists,
            SUM(CAST(COALESCE(fld.e, '0') AS INTEGER)) AS errors,
            SUM(CAST(COALESCE(fld.dp, '0') AS INTEGER)) AS double_plays
        FROM lahman_fielding AS fld
        JOIN lahman_people AS ppl
          ON ppl.playerid = fld.playerid
        WHERE {where_clause}
        GROUP BY fld.playerid, ppl.namefirst, ppl.namelast
        """,
        parameters,
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        games = safe_int(row["games"]) or 0
        putouts = safe_int(row["putouts"]) or 0
        assists = safe_int(row["assists"]) or 0
        errors = safe_int(row["errors"]) or 0
        chances = putouts + assists + errors
        fielding_pct = ((putouts + assists) / chances) if chances else None
        metric_value = select_historical_fielding_metric(query.metric.key, fielding_pct, row)
        if metric_value is None:
            continue
        sample_values = {"games": float(games)}
        if not passes_sample_threshold(query.metric, sample_values):
            continue
        candidates.append(
            {
                "player_name": build_person_name(row["namefirst"], row["namelast"], row["playerid"]),
                "metric_value": float(metric_value),
                "sample_size": float(games),
                "games": games,
                "position": summarize_positions(row["positions"]),
                "fielding_pct": fielding_pct,
                "errors": errors,
                "assists": assists,
                "putouts": putouts,
                "double_plays": safe_int(row["double_plays"]) or 0,
                "first_season": safe_int(row["first_season"]) or 0,
                "last_season": safe_int(row["last_season"]) or 0,
            }
        )
    return rank_rows(candidates, query)


def rank_rows(rows: list[dict[str, Any]], query: CohortMetricQuery) -> list[dict[str, Any]]:
    rows = [row for row in rows if row.get("metric_value") is not None]
    rows.sort(
        key=lambda row: (
            -float(row["metric_value"]) if query.sort_desc else float(row["metric_value"]),
            -(row.get("sample_size") or 0.0),
            str(row.get("player_name") or ""),
        )
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def build_cohort_metric_summary(
    query: CohortMetricQuery,
    leader: dict[str, Any],
    trailing: list[dict[str, Any]],
) -> str:
    value = format_metric_value(query.metric.formatter, leader.get("metric_value"))
    summary = (
        f"Across {query.scope_label}, the {query.descriptor} {query.role} by {query.metric.label} is "
        f"{leader.get('player_name')} at {value}{build_sample_text(query, leader)}."
    )
    if trailing:
        summary = (
            f"{summary} Next on the board: "
            + "; ".join(
                f"{row.get('player_name')} {format_metric_value(query.metric.formatter, row.get('metric_value'))}"
                for row in trailing
            )
            + "."
        )
    return summary


def build_sample_text(query: CohortMetricQuery, row: dict[str, Any]) -> str:
    if query.role == "fielder":
        return f" across {int(row.get('games') or 0)} games"
    if query.role in {"pitcher", "starter", "reliever"}:
        innings = row.get("innings")
        if innings is not None:
            return f" over {innings} IP"
        return ""
    plate_appearances = safe_int(row.get("plate_appearances"))
    if plate_appearances:
        return f" over {plate_appearances} PA"
    return ""


def build_citation(query: CohortMetricQuery) -> str:
    if query.metric.source_family == "statcast":
        return "Local Statcast batter summaries aggregated from synced public Statcast data"
    if query.cohort.kind == "manager_era":
        return "Lahman managers, batting, pitching, fielding, and people tables"
    return "Lahman people, batting, pitching, and fielding tables"


def format_cohort_span_label(cohort: ResolvedCohort) -> str:
    if not cohort.seasons:
        return cohort.label
    if len(cohort.seasons) == 1:
        return f"{cohort.label} ({cohort.seasons[0]})"
    return f"{cohort.label} ({cohort.seasons[0]}-{cohort.seasons[-1]})"


def summarize_positions(raw_positions: Any) -> str:
    text = str(raw_positions or "").strip()
    if not text:
        return ""
    parts = [part.strip() for part in text.split(",") if part.strip()]
    return "/".join(parts[:4])


def format_metric_value(formatter: str, value: Any) -> str:
    numeric = safe_float(value)
    if numeric is None:
        return "n/a"
    return f"{numeric:{formatter}}"


def build_statcast_cohort_gap_snippet(connection, query: CohortMetricQuery) -> EvidenceSnippet:
    batter_games_ready = table_exists(connection, "statcast_batter_games")
    statcast_events_ready = table_exists(connection, "statcast_events")
    scope_text = query.scope_label.lower()
    if batter_games_ready or statcast_events_ready:
        detail = (
            f"The local Statcast cohort path understood this as {query.cohort.label} ranked by {query.metric.label} "
            f"for {scope_text}, but the synced Statcast warehouse returned no qualifying rows for that filter."
        )
    else:
        detail = (
            f"The local Statcast cohort path understood this as {query.cohort.label} ranked by {query.metric.label} "
            f"for {scope_text}, but the synced Statcast warehouse tables are not available yet."
        )
    return EvidenceSnippet(
        source="Cohort Metric Leaderboards",
        title=f"{query.cohort.label} {query.metric.label} cohort gap",
        citation="Local Statcast cohort warehouse",
        summary=(
            f"I recognize this as a cohort Statcast leaderboard query for {query.cohort.label} by {query.metric.label}. "
            f"{detail}"
        ),
        payload={
            "analysis_type": "cohort_metric_gap",
            "mode": "historical",
            "cohort_kind": query.cohort.kind,
            "cohort_label": query.cohort.label,
            "metric": query.metric.label,
            "source_family": query.metric.source_family,
            "scope_label": query.scope_label,
        },
    )
