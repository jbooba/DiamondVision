from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import pandas as pd

from .config import Settings
from .models import EvidenceSnippet
from .query_intent import detect_ranking_intent
from .retrosheet_splits import open_retrosheet_plays_stream
from .storage import (
    clear_retrosheet_player_streak_records,
    get_connection,
    initialize_database,
    list_table_columns,
    set_metadata_value,
    table_exists,
    upsert_retrosheet_player_streak_records,
)


PLAY_STREAK_USECOLS = [
    "gid",
    "batter",
    "pa",
    "ab",
    "k",
    "date",
    "gametype",
]

GAME_STREAK_SOURCE_COLUMNS = (
    "id",
    "gid",
    "date",
    "gametype",
    "b_pa",
    "b_ab",
    "b_h",
    "b_hr",
    "b_w",
    "b_hbp",
    "b_k",
)

AB_WITHOUT_STRIKEOUT_PATTERN = re.compile(
    r"\b(?:at[- ]bats?|at[- ]bat streak|abs?|ab)\b.*\bwithout\b.*\bstrike(?: ?out|outs?)\b|\bwithout\b.*\bstrike(?: ?out|outs?)\b.*\b(?:at[- ]bats?|at[- ]bat streak|abs?|ab)\b",
    re.IGNORECASE,
)
PA_WITHOUT_STRIKEOUT_PATTERN = re.compile(
    r"\b(?:plate appearances?|pas?|pa)\b.*\bwithout\b.*\bstrike(?: ?out|outs?)\b|\bwithout\b.*\bstrike(?: ?out|outs?)\b.*\b(?:plate appearances?|pas?|pa)\b",
    re.IGNORECASE,
)
HIT_STREAK_PATTERN = re.compile(
    r"\bhit streak\b|\bconsecutive games?\b.*\bwith\b.*\bhits?\b|\bgames?\b.*\bwith\b.*\bhits?\b",
    re.IGNORECASE,
)
HOME_RUN_STREAK_PATTERN = re.compile(
    r"\bhome run streak\b|\bhomer streak\b|\bconsecutive games?\b.*\bwith\b.*\bhome runs?\b|\bgames?\b.*\bwith\b.*\bhome runs?\b",
    re.IGNORECASE,
)
ON_BASE_STREAK_PATTERN = re.compile(
    r"\bon[- ]base streak\b|\bconsecutive games?\b.*\breaching base\b|\bconsecutive games?\b.*\bon base\b",
    re.IGNORECASE,
)
GAMES_WITHOUT_STRIKEOUT_PATTERN = re.compile(
    r"\bgames?\b.*\bwithout\b.*\bstrike(?: ?out|outs?)\b|\bwithout\b.*\bstrike(?: ?out|outs?)\b.*\bgames?\b",
    re.IGNORECASE,
)
STREAK_HINT_PATTERN = re.compile(r"\b(longest|most consecutive|record|streak)\b", re.IGNORECASE)


@dataclass(slots=True, frozen=True)
class StreakSpec:
    key: str
    label: str
    unit_label: str
    aliases: tuple[str, ...]


@dataclass(slots=True)
class PlayerStreakQuery:
    spec: StreakSpec
    descriptor: str
    sort_desc: bool


@dataclass(slots=True)
class _StreakState:
    length: int
    start_date: str
    start_gid: str
    first_season: int


STREAK_SPECS: tuple[StreakSpec, ...] = (
    StreakSpec(
        key="ab_without_strikeout",
        label="at-bats without a strikeout",
        unit_label="AB",
        aliases=("at-bats without a strikeout", "at bats without a strikeout", "ab without a strikeout"),
    ),
    StreakSpec(
        key="pa_without_strikeout",
        label="plate appearances without a strikeout",
        unit_label="PA",
        aliases=("plate appearances without a strikeout", "pa without a strikeout"),
    ),
    StreakSpec(
        key="games_with_hit",
        label="hit streak",
        unit_label="games",
        aliases=("hit streak", "games with a hit", "games with hits"),
    ),
    StreakSpec(
        key="games_with_home_run",
        label="home run streak",
        unit_label="games",
        aliases=("home run streak", "homer streak", "games with a home run"),
    ),
    StreakSpec(
        key="games_on_base",
        label="on-base streak",
        unit_label="games",
        aliases=("on-base streak", "games reaching base", "games on base"),
    ),
    StreakSpec(
        key="games_without_strikeout",
        label="games without a strikeout",
        unit_label="games",
        aliases=("games without a strikeout", "games without striking out"),
    ),
)
STREAK_SPEC_BY_KEY = {spec.key: spec for spec in STREAK_SPECS}


class RetrosheetStreakResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_player_streak_query(question)
        if query is None:
            return None
        if not table_exists(connection, "retrosheet_player_streak_records"):
            return build_streak_sync_gap_snippet(query)
        rows = fetch_player_streak_rows(connection, query)
        if not rows:
            return build_streak_sync_gap_snippet(query)
        leader = rows[0]
        return EvidenceSnippet(
            source="Retrosheet Streak Warehouse",
            title=f"{query.spec.label} leaderboard",
            citation="Retrosheet regular-season batting logs and plays.csv streak warehouse",
            summary=build_player_streak_summary(query, leader, rows[1:4]),
            payload={
                "analysis_type": "player_streak_leaderboard",
                "mode": "historical",
                "streak_key": query.spec.key,
                "streak_label": query.spec.label,
                "rows": rows[:12],
            },
        )


def parse_player_streak_query(question: str) -> PlayerStreakQuery | None:
    lowered = f" {question.lower()} "
    if not STREAK_HINT_PATTERN.search(lowered):
        return None
    spec = find_streak_spec(question)
    if spec is None:
        return None
    if "shortest" in lowered:
        descriptor = "shortest"
        sort_desc = False
    else:
        ranking_intent = detect_ranking_intent(lowered, higher_is_better=True, fallback_label="longest")
        if ranking_intent is None:
            return None
        descriptor = "longest" if ("longest" in lowered or "most consecutive" in lowered or " record" in lowered) else ranking_intent.descriptor
        sort_desc = descriptor != "shortest"
    return PlayerStreakQuery(
        spec=spec,
        descriptor=descriptor,
        sort_desc=sort_desc,
    )


def find_streak_spec(question: str) -> StreakSpec | None:
    if AB_WITHOUT_STRIKEOUT_PATTERN.search(question):
        return STREAK_SPEC_BY_KEY["ab_without_strikeout"]
    if PA_WITHOUT_STRIKEOUT_PATTERN.search(question):
        return STREAK_SPEC_BY_KEY["pa_without_strikeout"]
    if HIT_STREAK_PATTERN.search(question):
        return STREAK_SPEC_BY_KEY["games_with_hit"]
    if HOME_RUN_STREAK_PATTERN.search(question):
        return STREAK_SPEC_BY_KEY["games_with_home_run"]
    if ON_BASE_STREAK_PATTERN.search(question):
        return STREAK_SPEC_BY_KEY["games_on_base"]
    if GAMES_WITHOUT_STRIKEOUT_PATTERN.search(question):
        return STREAK_SPEC_BY_KEY["games_without_strikeout"]
    return None


def fetch_player_streak_rows(connection, query: PlayerStreakQuery) -> list[dict[str, Any]]:
    people_columns = set(list_table_columns(connection, "lahman_people"))
    join_clause = "p.playerid = s.player_id"
    if "retroid" in people_columns:
        join_clause += " OR lower(COALESCE(p.retroid, '')) = lower(s.player_id)"
    rows = connection.execute(
        f"""
        SELECT
            s.player_id,
            p.namefirst,
            p.namelast,
            s.streak_key,
            s.streak_length,
            s.start_date,
            s.end_date,
            s.start_gid,
            s.end_gid,
            s.first_season,
            s.last_season
        FROM retrosheet_player_streak_records AS s
        LEFT JOIN lahman_people AS p
          ON {join_clause}
        WHERE s.streak_key = ?
        ORDER BY s.streak_length DESC, s.start_date ASC, s.player_id ASC
        LIMIT 12
        """,
        (query.spec.key,),
    ).fetchall()
    normalized: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        normalized.append(
            {
                "rank": index,
                "player_id": str(row["player_id"] or ""),
                "player_name": build_person_name(row["namefirst"], row["namelast"], row["player_id"]),
                "streak_length": int(row["streak_length"] or 0),
                "start_date": str(row["start_date"] or ""),
                "end_date": str(row["end_date"] or ""),
                "start_gid": str(row["start_gid"] or ""),
                "end_gid": str(row["end_gid"] or ""),
                "first_season": int(row["first_season"] or 0),
                "last_season": int(row["last_season"] or 0),
                "unit_label": query.spec.unit_label,
            }
        )
    return normalized


def build_player_streak_summary(
    query: PlayerStreakQuery,
    leader: dict[str, Any],
    others: list[dict[str, Any]],
) -> str:
    leader_value = leader["streak_length"]
    leader_unit = leader["unit_label"]
    leader_dates = format_streak_dates(leader)
    parts = [
        f"Across regular-season Retrosheet history, the {query.descriptor} {query.spec.label} belongs to {leader['player_name']} at {leader_value} {leader_unit}{leader_dates}."
    ]
    if others:
        runner_text = "; ".join(
            f"{row['player_name']} {row['streak_length']} {row['unit_label']}"
            for row in others
        )
        parts.append(f"Next on the board: {runner_text}.")
    return " ".join(parts)


def build_streak_sync_gap_snippet(query: PlayerStreakQuery) -> EvidenceSnippet:
    return EvidenceSnippet(
        source="Retrosheet Streak Warehouse",
        title=f"{query.spec.label} sync gap",
        citation="Retrosheet streak warehouse missing",
        summary=(
            f"I understand this as a historical streak query for {query.spec.label}, but the local Retrosheet streak warehouse "
            "has not been synced yet. This answer needs a compact consecutive-record table built from Retrosheet regular-season "
            "plays and batting logs."
        ),
        payload={
            "analysis_type": "player_streak_gap",
            "mode": "historical",
            "streak_key": query.spec.key,
            "streak_label": query.spec.label,
        },
    )


def sync_retrosheet_player_streaks(
    settings: Settings,
    *,
    retrosheet_dir: Path | None = None,
    chunk_size: int = 250_000,
) -> list[str]:
    source_dir = retrosheet_dir or settings.raw_data_dir / "retrosheet"
    connection = get_connection(settings.database_path)
    initialize_database(connection)
    clear_retrosheet_player_streak_records(connection)
    play_records, play_messages = build_play_streak_records(source_dir, chunk_size=chunk_size)
    game_records, game_messages = build_game_streak_records(connection)
    combined = merge_streak_record_sets(play_records, game_records)
    stored = upsert_retrosheet_player_streak_records(connection, combined)
    set_metadata_value(connection, "retrosheet_streaks_last_synced", pd.Timestamp.utcnow().isoformat())
    connection.close()
    messages = [*play_messages, *game_messages]
    messages.append(
        f"Built Retrosheet player streak warehouse from {source_dir} and imported batting logs ({stored} streak record row(s) stored)."
    )
    return messages


def build_play_streak_records(
    source_dir: Path,
    *,
    chunk_size: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    best_by_key: dict[str, dict[str, dict[str, Any]]] = {
        "ab_without_strikeout": {},
        "pa_without_strikeout": {},
    }
    current_by_key: dict[str, dict[str, _StreakState]] = {
        "ab_without_strikeout": {},
        "pa_without_strikeout": {},
    }
    total_chunks = 0
    total_regular_pa = 0
    with open_retrosheet_plays_stream(source_dir) as handle:
        reader = pd.read_csv(
            handle,
            usecols=PLAY_STREAK_USECOLS,
            dtype=str,
            chunksize=chunk_size,
            low_memory=False,
        )
        for chunk in reader:
            total_chunks += 1
            chunk = chunk.fillna("")
            chunk = chunk[chunk["gametype"].str.lower() == "regular"]
            if chunk.empty:
                continue
            chunk["pa"] = pd.to_numeric(chunk["pa"], errors="coerce").fillna(0).astype(int)
            chunk["ab"] = pd.to_numeric(chunk["ab"], errors="coerce").fillna(0).astype(int)
            chunk["k"] = pd.to_numeric(chunk["k"], errors="coerce").fillna(0).astype(int)
            total_regular_pa += int(chunk["pa"].sum())
            for row in chunk.itertuples(index=False):
                batter = str(row.batter or "").strip()
                if not batter:
                    continue
                game_id = str(row.gid or "")
                game_date = str(row.date or "")
                season = parse_season(game_date)
                if row.pa:
                    update_success_streak(
                        best_by_key["pa_without_strikeout"],
                        current_by_key["pa_without_strikeout"],
                        batter,
                        success=row.k == 0,
                        increment=1,
                        game_date=game_date,
                        game_id=game_id,
                        season=season,
                        streak_key="pa_without_strikeout",
                    )
                if row.ab:
                    update_success_streak(
                        best_by_key["ab_without_strikeout"],
                        current_by_key["ab_without_strikeout"],
                        batter,
                        success=row.k == 0,
                        increment=1,
                        game_date=game_date,
                        game_id=game_id,
                        season=season,
                        streak_key="ab_without_strikeout",
                    )
    records = [record for records in best_by_key.values() for record in records.values()]
    messages = [
        f"Built Retrosheet play-level streak records from {source_dir} ({total_chunks} chunk(s), {total_regular_pa} regular-season plate appearance row(s) scanned)."
    ]
    return records, messages


def build_game_streak_records(connection) -> tuple[list[dict[str, Any]], list[str]]:
    if not table_exists(connection, "retrosheet_batting"):
        return [], ["Skipped game-based Retrosheet streaks because retrosheet_batting is not available."]
    best_by_key: dict[str, dict[str, dict[str, Any]]] = {
        "games_with_hit": {},
        "games_with_home_run": {},
        "games_on_base": {},
        "games_without_strikeout": {},
    }
    current_by_key: dict[str, dict[str, _StreakState]] = {
        "games_with_hit": {},
        "games_with_home_run": {},
        "games_on_base": {},
        "games_without_strikeout": {},
    }
    total_rows = 0
    cursor = connection.execute(
        """
        SELECT id, gid, date, gametype, b_pa, b_ab, b_h, b_hr, b_w, b_hbp, b_k
        FROM retrosheet_batting
        WHERE lower(COALESCE(gametype, '')) = 'regular'
        ORDER BY date, gid, id
        """
    )
    for row in cursor:
        total_rows += 1
        player_id = str(row["id"] or "").strip()
        if not player_id:
            continue
        plate_appearances = safe_int(row["b_pa"])
        at_bats = safe_int(row["b_ab"])
        if plate_appearances <= 0:
            continue
        game_id = str(row["gid"] or "")
        game_date = str(row["date"] or "")
        season = parse_season(game_date)
        hits = safe_int(row["b_h"])
        home_runs = safe_int(row["b_hr"])
        walks = safe_int(row["b_w"])
        hit_by_pitch = safe_int(row["b_hbp"])
        strikeouts = safe_int(row["b_k"])
        if at_bats > 0:
            update_success_streak(
                best_by_key["games_with_hit"],
                current_by_key["games_with_hit"],
                player_id,
                success=hits > 0,
                increment=1,
                game_date=game_date,
                game_id=game_id,
                season=season,
                streak_key="games_with_hit",
            )
        update_success_streak(
            best_by_key["games_with_home_run"],
            current_by_key["games_with_home_run"],
            player_id,
            success=home_runs > 0,
            increment=1,
            game_date=game_date,
            game_id=game_id,
            season=season,
            streak_key="games_with_home_run",
        )
        update_success_streak(
            best_by_key["games_on_base"],
            current_by_key["games_on_base"],
            player_id,
            success=(hits + walks + hit_by_pitch) > 0,
            increment=1,
            game_date=game_date,
            game_id=game_id,
            season=season,
            streak_key="games_on_base",
        )
        update_success_streak(
            best_by_key["games_without_strikeout"],
            current_by_key["games_without_strikeout"],
            player_id,
            success=strikeouts == 0,
            increment=1,
            game_date=game_date,
            game_id=game_id,
            season=season,
            streak_key="games_without_strikeout",
        )
    records = [record for records in best_by_key.values() for record in records.values()]
    messages = [
        f"Built Retrosheet game-level streak records from imported batting logs ({total_rows} regular-season batting row(s) scanned)."
    ]
    return records, messages


def update_success_streak(
    best_records: dict[str, dict[str, Any]],
    current_states: dict[str, _StreakState],
    player_id: str,
    *,
    success: bool,
    increment: int,
    game_date: str,
    game_id: str,
    season: int,
    streak_key: str,
) -> None:
    if not success:
        current_states.pop(player_id, None)
        return
    state = current_states.get(player_id)
    if state is None:
        state = _StreakState(length=0, start_date=game_date, start_gid=game_id, first_season=season)
    new_state = _StreakState(
        length=state.length + increment,
        start_date=state.start_date,
        start_gid=state.start_gid,
        first_season=state.first_season,
    )
    current_states[player_id] = new_state
    best = best_records.get(player_id)
    if best is None or new_state.length > int(best["streak_length"]):
        best_records[player_id] = {
            "player_id": player_id,
            "streak_key": streak_key,
            "streak_length": new_state.length,
            "start_date": new_state.start_date,
            "end_date": game_date,
            "start_gid": new_state.start_gid,
            "end_gid": game_id,
            "first_season": new_state.first_season,
            "last_season": season,
        }


def merge_streak_record_sets(*record_sets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for records in record_sets:
        for record in records:
            key = (str(record["player_id"]), str(record["streak_key"]))
            existing = merged.get(key)
            if existing is None or int(record["streak_length"]) > int(existing["streak_length"]):
                merged[key] = record
    return list(merged.values())


def build_person_name(first_name: Any, last_name: Any, fallback_id: Any) -> str:
    parts = [str(first_name or "").strip(), str(last_name or "").strip()]
    text = " ".join(part for part in parts if part)
    return text or str(fallback_id or "").strip()


def format_streak_dates(row: dict[str, Any]) -> str:
    start_date = str(row.get("start_date") or "").strip()
    end_date = str(row.get("end_date") or "").strip()
    if not start_date and not end_date:
        return ""
    if start_date and end_date:
        if start_date == end_date:
            return f" on {start_date}"
        return f" from {start_date} through {end_date}"
    return f" through {end_date or start_date}"


def parse_season(value: str) -> int:
    text = str(value or "").strip()
    if len(text) >= 4 and text[:4].isdigit():
        return int(text[:4])
    return 0


def safe_int(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0
