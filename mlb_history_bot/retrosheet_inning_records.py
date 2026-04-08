from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .config import Settings
from .models import EvidenceSnippet
from .query_intent import detect_ranking_intent
from .storage import list_table_columns, resolve_column, table_exists


INNING_HINT_PATTERN = re.compile(r"\b(single inning|one inning|inning)\b", re.IGNORECASE)
RUNS_GIVEN_UP_PATTERN = re.compile(r"\bruns?\s+(?:given up|allowed)\b", re.IGNORECASE)
RUNS_SCORED_PATTERN = re.compile(r"\bruns?\s+scored\b|\bscore(?:d)? the most runs\b", re.IGNORECASE)


@dataclass(slots=True, frozen=True)
class InningRecordQuery:
    record_key: str
    label: str
    subject_label: str
    sort_desc: bool
    descriptor: str


class RetrosheetInningRecordResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_inning_record_query(question)
        if query is None:
            return None
        if not table_exists(connection, "retrosheet_teamstats"):
            return None
        rows = fetch_inning_record_rows(connection, query)
        if not rows:
            return None
        return EvidenceSnippet(
            source="Retrosheet Inning Records",
            title=f"{query.label} leaderboard",
            citation="Retrosheet teamstats inning columns",
            summary=build_inning_record_summary(query, rows),
            payload={
                "analysis_type": "inning_record_leaderboard",
                "mode": "historical",
                "record_key": query.record_key,
                "metric": query.label,
                "rows": rows,
            },
        )


def parse_inning_record_query(question: str) -> InningRecordQuery | None:
    lowered = question.lower()
    if INNING_HINT_PATTERN.search(lowered) is None or "run" not in lowered:
        return None
    if RUNS_GIVEN_UP_PATTERN.search(lowered):
        ranking_intent = detect_ranking_intent(lowered, higher_is_better=True, fallback_label="most")
        if ranking_intent is None:
            return None
        return InningRecordQuery(
            record_key="runs_allowed_single_inning",
            label="Runs Allowed",
            subject_label="team",
            sort_desc=ranking_intent.sort_desc,
            descriptor=ranking_intent.descriptor,
        )
    if RUNS_SCORED_PATTERN.search(lowered) or "most runs" in lowered:
        ranking_intent = detect_ranking_intent(lowered, higher_is_better=True, fallback_label="most")
        if ranking_intent is None:
            return None
        return InningRecordQuery(
            record_key="runs_scored_single_inning",
            label="Runs Scored",
            subject_label="team",
            sort_desc=ranking_intent.sort_desc,
            descriptor=ranking_intent.descriptor,
        )
    return None


def fetch_inning_record_rows(connection, query: InningRecordQuery) -> list[dict[str, Any]]:
    teamstats_columns = {column.lower() for column in list_table_columns(connection, "retrosheet_teamstats")}
    required_columns = {f"inn{inning}" for inning in range(1, 29)}
    if not required_columns.issubset(teamstats_columns):
        return []
    if not table_exists(connection, "retrosheet_gameinfo"):
        return []
    game_date_column = resolve_column(connection, "retrosheet_gameinfo", ("date",))
    game_season_column = resolve_column(connection, "retrosheet_gameinfo", ("season",))
    team_code_column = resolve_column(connection, "lahman_teams", ("teamidretro", "teamid"))
    team_name_select = "team_name"
    team_name_cte = ""
    team_name_join = ""
    if table_exists(connection, "lahman_teams") and team_code_column is not None:
        team_name_cte = f"""
        , team_names AS (
            SELECT
                CAST(yearid AS TEXT) AS season,
                upper({team_code_column}) AS team_code,
                MIN(name) AS team_name
            FROM lahman_teams
            GROUP BY CAST(yearid AS TEXT), upper({team_code_column})
        )
        """
        team_name_join = """
        LEFT JOIN team_names AS team_names
          ON team_names.season = base.season
         AND team_names.team_code = upper(base.team)
        LEFT JOIN team_names AS opp_names
          ON opp_names.season = base.season
         AND opp_names.team_code = upper(base.opponent)
        """
        team_name_select = "COALESCE(team_names.team_name, base.team)"
        opponent_name_select = "COALESCE(opp_names.team_name, base.opponent)"
    else:
        opponent_name_select = "base.opponent"
    date_select = f"COALESCE(gi.{game_date_column}, '')" if game_date_column else "''"
    season_select = (
        f"COALESCE(CAST(gi.{game_season_column} AS TEXT), substr({date_select}, 1, 4), substr(t.gid, 4, 4))"
        if game_season_column
        else f"COALESCE(substr({date_select}, 1, 4), substr(t.gid, 4, 4))"
    )
    team_select_columns = ", ".join(f"t.inn{inning} AS t_inn{inning}" for inning in range(1, 29))
    opp_select_columns = ", ".join(f"o.inn{inning} AS o_inn{inning}" for inning in range(1, 29))
    sql = f"""
        WITH base AS (
            SELECT
                t.gid AS gid,
                t.team AS team,
                o.team AS opponent,
                {date_select} AS game_date,
                {season_select} AS season,
                {team_select_columns},
                {opp_select_columns}
            FROM retrosheet_teamstats AS t
            JOIN retrosheet_teamstats AS o
              ON o.gid = t.gid
             AND upper(o.team) <> upper(t.team)
             AND lower(COALESCE(o.stattype, '')) = 'value'
            LEFT JOIN retrosheet_gameinfo AS gi
              ON gi.gid = t.gid
            WHERE lower(COALESCE(t.stattype, '')) = 'value'
              AND lower(COALESCE(t.gametype, '')) = 'regular'
              AND lower(COALESCE(o.gametype, '')) = 'regular'
        )
        {team_name_cte}
        SELECT
            base.*,
            {team_name_select} AS team_name,
            {opponent_name_select} AS opponent_name
        FROM base
        {team_name_join}
    """
    rows = connection.execute(sql).fetchall()
    candidates: list[dict[str, Any]] = []
    value_prefix = "o_inn" if query.record_key == "runs_allowed_single_inning" else "t_inn"
    for row in rows:
        for inning in range(1, 29):
            metric_value = safe_int(row[f"{value_prefix}{inning}"])
            candidate = {
                "season": str(row["season"] or ""),
                "game_date": str(row["game_date"] or ""),
                "gid": str(row["gid"] or ""),
                "team": str(row["team"] or ""),
                "team_name": str(row["team_name"] or row["team"] or ""),
                "opponent": str(row["opponent"] or ""),
                "opponent_name": str(row["opponent_name"] or row["opponent"] or ""),
                "inning": inning,
                "metric_value": metric_value,
            }
            candidates.append(candidate)
            candidates.sort(
                key=lambda item: (
                    -int(item["metric_value"]) if query.sort_desc else int(item["metric_value"]),
                    str(item["season"]),
                    str(item["game_date"]),
                    str(item["team_name"]),
                )
            )
            if len(candidates) > 12:
                candidates.pop()
    normalized: list[dict[str, Any]] = []
    for index, row in enumerate(candidates[:12], start=1):
        normalized.append(
            {
                "rank": index,
                **row,
            }
        )
    return normalized


def build_inning_record_summary(query: InningRecordQuery, rows: list[dict[str, Any]]) -> str:
    leader = rows[0]
    if query.record_key == "runs_allowed_single_inning":
        summary = (
            f"Across regular-season Retrosheet history, the {query.descriptor} {query.label.lower()} in a single inning "
            f"belongs to {leader['team_name']}, which yielded {leader['metric_value']} in inning {leader['inning']} "
            f"against {leader['opponent_name']} on {leader['game_date']}."
        )
    else:
        summary = (
            f"Across regular-season Retrosheet history, the {query.descriptor} {query.label.lower()} in a single inning "
            f"belongs to {leader['team_name']}, which scored {leader['metric_value']} in inning {leader['inning']} "
            f"against {leader['opponent_name']} on {leader['game_date']}."
        )
    trailing = rows[1:4]
    if trailing:
        next_up = "; ".join(
            f"{row['team_name']} {row['metric_value']} (inning {row['inning']}, {row['game_date']})"
            for row in trailing
        )
        summary = f"{summary} Next on the board: {next_up}."
    return summary


def safe_int(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0
