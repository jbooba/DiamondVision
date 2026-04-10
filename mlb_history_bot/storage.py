from __future__ import annotations

import csv
import json
import re
import sqlite3
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .metrics import MetricCatalog, MetricDefinition

STATCAST_HISTORY_BATTER_TABLE = "statcast_history_batter_seasons"
STATCAST_HISTORY_PITCHER_TABLE = "statcast_history_pitcher_seasons"


def get_connection(database_path: Path) -> sqlite3.Connection:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA foreign_keys=OFF")
    return connection


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS metrics (
            name TEXT PRIMARY KEY,
            aliases_json TEXT NOT NULL,
            category TEXT NOT NULL,
            definition TEXT NOT NULL,
            formula TEXT NOT NULL,
            exact_formula_public INTEGER NOT NULL,
            notes TEXT NOT NULL,
            historical_support TEXT NOT NULL,
            live_support TEXT NOT NULL,
            citations_json TEXT NOT NULL
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS metrics_fts USING fts5(
            name,
            aliases,
            definition,
            formula,
            notes,
            historical_support,
            live_support
        );

        CREATE TABLE IF NOT EXISTS document_chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_kind TEXT NOT NULL,
            source_name TEXT NOT NULL,
            title TEXT NOT NULL,
            citation TEXT NOT NULL,
            content TEXT NOT NULL,
            metadata_json TEXT NOT NULL
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS document_chunks_fts USING fts5(
            source_name,
            title,
            citation,
            content
        );

        CREATE TABLE IF NOT EXISTS csv_manifests (
            source_name TEXT NOT NULL,
            dataset_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            columns_json TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            notes TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (source_name, dataset_name)
        );

        CREATE TABLE IF NOT EXISTS fielding_bible_player_drs (
            season INTEGER NOT NULL,
            snapshot_at TEXT NOT NULL DEFAULT '',
            source_name TEXT NOT NULL,
            player TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL DEFAULT 0,
            pos INTEGER NOT NULL,
            pos_abbr TEXT NOT NULL DEFAULT '',
            games REAL,
            innings REAL,
            total REAL,
            art REAL,
            gfpdm REAL,
            gdp REAL,
            bunt REAL,
            of_arm REAL,
            sb REAL,
            sz REAL,
            adj_er REAL,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (season, snapshot_at, player_id, team_id, pos, pos_abbr)
        );

        CREATE TABLE IF NOT EXISTS fielding_bible_team_drs (
            season INTEGER NOT NULL,
            snapshot_at TEXT NOT NULL DEFAULT '',
            source_name TEXT NOT NULL,
            team_id INTEGER NOT NULL,
            nickname TEXT NOT NULL,
            rank INTEGER,
            games REAL,
            pitcher REAL,
            catcher REAL,
            first_base REAL,
            second_base REAL,
            third_base REAL,
            shortstop REAL,
            left_field REAL,
            center_field REAL,
            right_field REAL,
            outfield_positioning_runs_saved REAL,
            non_shift REAL,
            shifts REAL,
            total REAL,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (season, snapshot_at, team_id)
        );

        CREATE TABLE IF NOT EXISTS statcast_team_games (
            season INTEGER NOT NULL,
            game_date TEXT NOT NULL,
            game_pk INTEGER NOT NULL,
            team TEXT NOT NULL,
            team_name TEXT NOT NULL,
            opponent TEXT NOT NULL,
            opponent_name TEXT NOT NULL,
            is_home INTEGER NOT NULL DEFAULT 0,
            plate_appearances INTEGER NOT NULL DEFAULT 0,
            at_bats INTEGER NOT NULL DEFAULT 0,
            hits INTEGER NOT NULL DEFAULT 0,
            strikeouts INTEGER NOT NULL DEFAULT 0,
            batted_ball_events INTEGER NOT NULL DEFAULT 0,
            xba_numerator REAL NOT NULL DEFAULT 0,
            xwoba_numerator REAL NOT NULL DEFAULT 0,
            xwoba_denom REAL NOT NULL DEFAULT 0,
            xslg_numerator REAL NOT NULL DEFAULT 0,
            hard_hit_bbe INTEGER NOT NULL DEFAULT 0,
            barrel_bbe INTEGER NOT NULL DEFAULT 0,
            launch_speed_sum REAL NOT NULL DEFAULT 0,
            launch_speed_count INTEGER NOT NULL DEFAULT 0,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (game_pk, team)
        );

        CREATE TABLE IF NOT EXISTS statcast_pitcher_games (
            season INTEGER NOT NULL,
            game_date TEXT NOT NULL,
            game_pk INTEGER NOT NULL,
            pitcher_id INTEGER NOT NULL,
            pitcher_name TEXT NOT NULL,
            team TEXT NOT NULL,
            team_name TEXT NOT NULL,
            opponent TEXT NOT NULL,
            opponent_name TEXT NOT NULL,
            total_pitches INTEGER NOT NULL DEFAULT 0,
            max_release_speed REAL,
            pitches_95_plus INTEGER NOT NULL DEFAULT 0,
            pitches_97_plus INTEGER NOT NULL DEFAULT 0,
            pitches_98_plus INTEGER NOT NULL DEFAULT 0,
            pitches_99_plus INTEGER NOT NULL DEFAULT 0,
            pitches_100_plus INTEGER NOT NULL DEFAULT 0,
            pitches_101_plus INTEGER NOT NULL DEFAULT 0,
            pitches_102_plus INTEGER NOT NULL DEFAULT 0,
            fastball_pitches INTEGER NOT NULL DEFAULT 0,
            fastball_strikeouts INTEGER NOT NULL DEFAULT 0,
            changeup_pitches INTEGER NOT NULL DEFAULT 0,
            changeup_strikeouts INTEGER NOT NULL DEFAULT 0,
            curveball_pitches INTEGER NOT NULL DEFAULT 0,
            curveball_strikeouts INTEGER NOT NULL DEFAULT 0,
            slider_pitches INTEGER NOT NULL DEFAULT 0,
            slider_strikeouts INTEGER NOT NULL DEFAULT 0,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (game_pk, pitcher_id)
        );

        CREATE TABLE IF NOT EXISTS statcast_batter_games (
            season INTEGER NOT NULL,
            game_date TEXT NOT NULL,
            game_pk INTEGER NOT NULL,
            batter_id INTEGER NOT NULL,
            batter_name TEXT NOT NULL,
            team TEXT NOT NULL,
            team_name TEXT NOT NULL,
            opponent TEXT NOT NULL,
            opponent_name TEXT NOT NULL,
            plate_appearances INTEGER NOT NULL DEFAULT 0,
            at_bats INTEGER NOT NULL DEFAULT 0,
            hits INTEGER NOT NULL DEFAULT 0,
            singles INTEGER NOT NULL DEFAULT 0,
            doubles INTEGER NOT NULL DEFAULT 0,
            triples INTEGER NOT NULL DEFAULT 0,
            home_runs INTEGER NOT NULL DEFAULT 0,
            walks INTEGER NOT NULL DEFAULT 0,
            strikeouts INTEGER NOT NULL DEFAULT 0,
            runs_batted_in INTEGER NOT NULL DEFAULT 0,
            batted_ball_events INTEGER NOT NULL DEFAULT 0,
            xba_numerator REAL NOT NULL DEFAULT 0,
            xwoba_numerator REAL NOT NULL DEFAULT 0,
            xwoba_denom REAL NOT NULL DEFAULT 0,
            xslg_numerator REAL NOT NULL DEFAULT 0,
            hard_hit_bbe INTEGER NOT NULL DEFAULT 0,
            barrel_bbe INTEGER NOT NULL DEFAULT 0,
            launch_speed_sum REAL NOT NULL DEFAULT 0,
            launch_speed_count INTEGER NOT NULL DEFAULT 0,
            max_launch_speed REAL,
            avg_bat_speed REAL,
            max_bat_speed REAL,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (game_pk, batter_id)
        );

        CREATE TABLE IF NOT EXISTS statcast_pitch_type_games (
            season INTEGER NOT NULL,
            game_date TEXT NOT NULL,
            game_pk INTEGER NOT NULL,
            pitcher_id INTEGER NOT NULL,
            pitcher_name TEXT NOT NULL,
            team TEXT NOT NULL,
            team_name TEXT NOT NULL,
            opponent TEXT NOT NULL,
            opponent_name TEXT NOT NULL,
            pitch_type TEXT NOT NULL,
            pitch_name TEXT NOT NULL,
            pitch_family TEXT NOT NULL,
            pitches INTEGER NOT NULL DEFAULT 0,
            avg_release_speed REAL,
            max_release_speed REAL,
            avg_release_spin_rate REAL,
            max_release_spin_rate REAL,
            called_strikes INTEGER NOT NULL DEFAULT 0,
            swinging_strikes INTEGER NOT NULL DEFAULT 0,
            whiffs INTEGER NOT NULL DEFAULT 0,
            strikeouts INTEGER NOT NULL DEFAULT 0,
            walks INTEGER NOT NULL DEFAULT 0,
            hits_allowed INTEGER NOT NULL DEFAULT 0,
            extra_base_hits_allowed INTEGER NOT NULL DEFAULT 0,
            home_runs_allowed INTEGER NOT NULL DEFAULT 0,
            batted_ball_events INTEGER NOT NULL DEFAULT 0,
            xba_numerator REAL NOT NULL DEFAULT 0,
            xwoba_numerator REAL NOT NULL DEFAULT 0,
            xwoba_denom REAL NOT NULL DEFAULT 0,
            xslg_numerator REAL NOT NULL DEFAULT 0,
            launch_speed_sum REAL NOT NULL DEFAULT 0,
            launch_speed_count INTEGER NOT NULL DEFAULT 0,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (game_pk, pitcher_id, pitch_type)
        );

        CREATE TABLE IF NOT EXISTS statcast_batter_pitch_type_games (
            season INTEGER NOT NULL,
            game_date TEXT NOT NULL,
            game_pk INTEGER NOT NULL,
            batter_id INTEGER NOT NULL,
            batter_name TEXT NOT NULL,
            team TEXT NOT NULL,
            team_name TEXT NOT NULL,
            opponent TEXT NOT NULL,
            opponent_name TEXT NOT NULL,
            pitch_type TEXT NOT NULL,
            pitch_name TEXT NOT NULL,
            pitch_family TEXT NOT NULL,
            plate_appearances INTEGER NOT NULL DEFAULT 0,
            at_bats INTEGER NOT NULL DEFAULT 0,
            hits INTEGER NOT NULL DEFAULT 0,
            singles INTEGER NOT NULL DEFAULT 0,
            doubles INTEGER NOT NULL DEFAULT 0,
            triples INTEGER NOT NULL DEFAULT 0,
            home_runs INTEGER NOT NULL DEFAULT 0,
            walks INTEGER NOT NULL DEFAULT 0,
            strikeouts INTEGER NOT NULL DEFAULT 0,
            runs_batted_in INTEGER NOT NULL DEFAULT 0,
            batted_ball_events INTEGER NOT NULL DEFAULT 0,
            xba_numerator REAL NOT NULL DEFAULT 0,
            xwoba_numerator REAL NOT NULL DEFAULT 0,
            xwoba_denom REAL NOT NULL DEFAULT 0,
            xslg_numerator REAL NOT NULL DEFAULT 0,
            hard_hit_bbe INTEGER NOT NULL DEFAULT 0,
            barrel_bbe INTEGER NOT NULL DEFAULT 0,
            launch_speed_sum REAL NOT NULL DEFAULT 0,
            launch_speed_count INTEGER NOT NULL DEFAULT 0,
            avg_bat_speed REAL,
            max_bat_speed REAL,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (game_pk, batter_id, pitch_type)
        );

        CREATE TABLE IF NOT EXISTS statcast_events (
            season INTEGER NOT NULL,
            game_date TEXT NOT NULL,
            game_pk INTEGER NOT NULL,
            at_bat_number INTEGER NOT NULL,
            pitch_number INTEGER NOT NULL,
            batter_id INTEGER NOT NULL,
            batter_name TEXT NOT NULL,
            pitcher_id INTEGER NOT NULL,
            pitcher_name TEXT NOT NULL,
            batting_team TEXT NOT NULL,
            pitching_team TEXT NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            stand TEXT NOT NULL DEFAULT '',
            p_throws TEXT NOT NULL DEFAULT '',
            pitch_type TEXT NOT NULL DEFAULT '',
            pitch_name TEXT NOT NULL DEFAULT '',
            pitch_family TEXT NOT NULL DEFAULT '',
            event TEXT NOT NULL DEFAULT '',
            is_ab INTEGER NOT NULL DEFAULT 0,
            is_hit INTEGER NOT NULL DEFAULT 0,
            is_xbh INTEGER NOT NULL DEFAULT 0,
            is_home_run INTEGER NOT NULL DEFAULT 0,
            is_strikeout INTEGER NOT NULL DEFAULT 0,
            has_risp INTEGER NOT NULL DEFAULT 0,
            balls INTEGER NOT NULL DEFAULT 0,
            strikes INTEGER NOT NULL DEFAULT 0,
            count_key TEXT NOT NULL DEFAULT '',
            outs_when_up INTEGER NOT NULL DEFAULT 0,
            runs_batted_in INTEGER NOT NULL DEFAULT 0,
            horizontal_location TEXT NOT NULL DEFAULT '',
            vertical_location TEXT NOT NULL DEFAULT '',
            field_direction TEXT NOT NULL DEFAULT '',
            release_speed REAL,
            release_spin_rate REAL,
            launch_speed REAL,
            launch_angle REAL,
            hit_distance REAL,
            bat_speed REAL,
            estimated_ba REAL,
            estimated_woba REAL,
            estimated_slg REAL,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (game_pk, at_bat_number, pitch_number)
        );

        CREATE TABLE IF NOT EXISTS retrosheet_team_split_games (
            season INTEGER NOT NULL,
            game_date TEXT NOT NULL,
            gid TEXT NOT NULL,
            team TEXT NOT NULL,
            split_key TEXT NOT NULL,
            plate_appearances INTEGER NOT NULL DEFAULT 0,
            at_bats INTEGER NOT NULL DEFAULT 0,
            hits INTEGER NOT NULL DEFAULT 0,
            doubles INTEGER NOT NULL DEFAULT 0,
            triples INTEGER NOT NULL DEFAULT 0,
            home_runs INTEGER NOT NULL DEFAULT 0,
            walks INTEGER NOT NULL DEFAULT 0,
            hit_by_pitch INTEGER NOT NULL DEFAULT 0,
            sacrifice_flies INTEGER NOT NULL DEFAULT 0,
            strikeouts INTEGER NOT NULL DEFAULT 0,
            runs_batted_in INTEGER NOT NULL DEFAULT 0,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (gid, team, split_key)
        );

        CREATE TABLE IF NOT EXISTS retrosheet_player_count_splits (
            player_id TEXT NOT NULL,
            count_key TEXT NOT NULL,
            plate_appearances INTEGER NOT NULL DEFAULT 0,
            at_bats INTEGER NOT NULL DEFAULT 0,
            hits INTEGER NOT NULL DEFAULT 0,
            doubles INTEGER NOT NULL DEFAULT 0,
            triples INTEGER NOT NULL DEFAULT 0,
            home_runs INTEGER NOT NULL DEFAULT 0,
            walks INTEGER NOT NULL DEFAULT 0,
            hit_by_pitch INTEGER NOT NULL DEFAULT 0,
            sacrifice_flies INTEGER NOT NULL DEFAULT 0,
            strikeouts INTEGER NOT NULL DEFAULT 0,
            runs_batted_in INTEGER NOT NULL DEFAULT 0,
            first_season INTEGER NOT NULL DEFAULT 0,
            last_season INTEGER NOT NULL DEFAULT 0,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (player_id, count_key)
        );

        CREATE TABLE IF NOT EXISTS retrosheet_player_reached_count_splits (
            player_id TEXT NOT NULL,
            count_key TEXT NOT NULL,
            plate_appearances INTEGER NOT NULL DEFAULT 0,
            at_bats INTEGER NOT NULL DEFAULT 0,
            hits INTEGER NOT NULL DEFAULT 0,
            doubles INTEGER NOT NULL DEFAULT 0,
            triples INTEGER NOT NULL DEFAULT 0,
            home_runs INTEGER NOT NULL DEFAULT 0,
            walks INTEGER NOT NULL DEFAULT 0,
            hit_by_pitch INTEGER NOT NULL DEFAULT 0,
            sacrifice_flies INTEGER NOT NULL DEFAULT 0,
            strikeouts INTEGER NOT NULL DEFAULT 0,
            runs_batted_in INTEGER NOT NULL DEFAULT 0,
            first_season INTEGER NOT NULL DEFAULT 0,
            last_season INTEGER NOT NULL DEFAULT 0,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (player_id, count_key)
        );

        CREATE TABLE IF NOT EXISTS retrosheet_player_opponent_contexts (
            player_id TEXT NOT NULL,
            opponent TEXT NOT NULL,
            context_key TEXT NOT NULL,
            plate_appearances INTEGER NOT NULL DEFAULT 0,
            at_bats INTEGER NOT NULL DEFAULT 0,
            hits INTEGER NOT NULL DEFAULT 0,
            doubles INTEGER NOT NULL DEFAULT 0,
            triples INTEGER NOT NULL DEFAULT 0,
            home_runs INTEGER NOT NULL DEFAULT 0,
            walks INTEGER NOT NULL DEFAULT 0,
            intentional_walks INTEGER NOT NULL DEFAULT 0,
            hit_by_pitch INTEGER NOT NULL DEFAULT 0,
            sacrifice_flies INTEGER NOT NULL DEFAULT 0,
            strikeouts INTEGER NOT NULL DEFAULT 0,
            runs_batted_in INTEGER NOT NULL DEFAULT 0,
            first_season INTEGER NOT NULL DEFAULT 0,
            last_season INTEGER NOT NULL DEFAULT 0,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (player_id, opponent, context_key)
        );

        CREATE TABLE IF NOT EXISTS retrosheet_player_opponent_pitcher_cohorts (
            player_id TEXT NOT NULL,
            cohort_kind TEXT NOT NULL,
            cohort_value TEXT NOT NULL,
            plate_appearances INTEGER NOT NULL DEFAULT 0,
            at_bats INTEGER NOT NULL DEFAULT 0,
            hits INTEGER NOT NULL DEFAULT 0,
            doubles INTEGER NOT NULL DEFAULT 0,
            triples INTEGER NOT NULL DEFAULT 0,
            home_runs INTEGER NOT NULL DEFAULT 0,
            walks INTEGER NOT NULL DEFAULT 0,
            intentional_walks INTEGER NOT NULL DEFAULT 0,
            hit_by_pitch INTEGER NOT NULL DEFAULT 0,
            sacrifice_flies INTEGER NOT NULL DEFAULT 0,
            strikeouts INTEGER NOT NULL DEFAULT 0,
            runs_batted_in INTEGER NOT NULL DEFAULT 0,
            pitchers_faced INTEGER NOT NULL DEFAULT 0,
            first_season INTEGER NOT NULL DEFAULT 0,
            last_season INTEGER NOT NULL DEFAULT 0,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (player_id, cohort_kind, cohort_value)
        );

        CREATE TABLE IF NOT EXISTS retrosheet_player_opponent_pitchers (
            player_id TEXT NOT NULL,
            pitcher_id TEXT NOT NULL,
            plate_appearances INTEGER NOT NULL DEFAULT 0,
            at_bats INTEGER NOT NULL DEFAULT 0,
            hits INTEGER NOT NULL DEFAULT 0,
            doubles INTEGER NOT NULL DEFAULT 0,
            triples INTEGER NOT NULL DEFAULT 0,
            home_runs INTEGER NOT NULL DEFAULT 0,
            walks INTEGER NOT NULL DEFAULT 0,
            intentional_walks INTEGER NOT NULL DEFAULT 0,
            hit_by_pitch INTEGER NOT NULL DEFAULT 0,
            sacrifice_flies INTEGER NOT NULL DEFAULT 0,
            strikeouts INTEGER NOT NULL DEFAULT 0,
            runs_batted_in INTEGER NOT NULL DEFAULT 0,
            batter_birthday_plate_appearances INTEGER NOT NULL DEFAULT 0,
            batter_birthday_at_bats INTEGER NOT NULL DEFAULT 0,
            batter_birthday_hits INTEGER NOT NULL DEFAULT 0,
            batter_birthday_doubles INTEGER NOT NULL DEFAULT 0,
            batter_birthday_triples INTEGER NOT NULL DEFAULT 0,
            batter_birthday_home_runs INTEGER NOT NULL DEFAULT 0,
            batter_birthday_walks INTEGER NOT NULL DEFAULT 0,
            batter_birthday_intentional_walks INTEGER NOT NULL DEFAULT 0,
            batter_birthday_hit_by_pitch INTEGER NOT NULL DEFAULT 0,
            batter_birthday_sacrifice_flies INTEGER NOT NULL DEFAULT 0,
            batter_birthday_strikeouts INTEGER NOT NULL DEFAULT 0,
            batter_birthday_runs_batted_in INTEGER NOT NULL DEFAULT 0,
            pitcher_birthday_plate_appearances INTEGER NOT NULL DEFAULT 0,
            pitcher_birthday_at_bats INTEGER NOT NULL DEFAULT 0,
            pitcher_birthday_hits INTEGER NOT NULL DEFAULT 0,
            pitcher_birthday_doubles INTEGER NOT NULL DEFAULT 0,
            pitcher_birthday_triples INTEGER NOT NULL DEFAULT 0,
            pitcher_birthday_home_runs INTEGER NOT NULL DEFAULT 0,
            pitcher_birthday_walks INTEGER NOT NULL DEFAULT 0,
            pitcher_birthday_intentional_walks INTEGER NOT NULL DEFAULT 0,
            pitcher_birthday_hit_by_pitch INTEGER NOT NULL DEFAULT 0,
            pitcher_birthday_sacrifice_flies INTEGER NOT NULL DEFAULT 0,
            pitcher_birthday_strikeouts INTEGER NOT NULL DEFAULT 0,
            pitcher_birthday_runs_batted_in INTEGER NOT NULL DEFAULT 0,
            first_season INTEGER NOT NULL DEFAULT 0,
            last_season INTEGER NOT NULL DEFAULT 0,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (player_id, pitcher_id)
        );

        CREATE TABLE IF NOT EXISTS retrosheet_player_streak_records (
            player_id TEXT NOT NULL,
            streak_key TEXT NOT NULL,
            streak_length INTEGER NOT NULL DEFAULT 0,
            start_date TEXT NOT NULL DEFAULT '',
            end_date TEXT NOT NULL DEFAULT '',
            start_gid TEXT NOT NULL DEFAULT '',
            end_gid TEXT NOT NULL DEFAULT '',
            first_season INTEGER NOT NULL DEFAULT 0,
            last_season INTEGER NOT NULL DEFAULT 0,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (player_id, streak_key)
        );
        """
    )
    _ensure_table_columns(
        connection,
        "statcast_events",
        {
            "balls": "INTEGER NOT NULL DEFAULT 0",
            "strikes": "INTEGER NOT NULL DEFAULT 0",
            "count_key": "TEXT NOT NULL DEFAULT ''",
            "outs_when_up": "INTEGER NOT NULL DEFAULT 0",
            "runs_batted_in": "INTEGER NOT NULL DEFAULT 0",
        },
    )
    _ensure_table_columns(
        connection,
        "retrosheet_player_opponent_pitchers",
        {
            "batter_birthday_plate_appearances": "INTEGER NOT NULL DEFAULT 0",
            "batter_birthday_at_bats": "INTEGER NOT NULL DEFAULT 0",
            "batter_birthday_hits": "INTEGER NOT NULL DEFAULT 0",
            "batter_birthday_doubles": "INTEGER NOT NULL DEFAULT 0",
            "batter_birthday_triples": "INTEGER NOT NULL DEFAULT 0",
            "batter_birthday_home_runs": "INTEGER NOT NULL DEFAULT 0",
            "batter_birthday_walks": "INTEGER NOT NULL DEFAULT 0",
            "batter_birthday_intentional_walks": "INTEGER NOT NULL DEFAULT 0",
            "batter_birthday_hit_by_pitch": "INTEGER NOT NULL DEFAULT 0",
            "batter_birthday_sacrifice_flies": "INTEGER NOT NULL DEFAULT 0",
            "batter_birthday_strikeouts": "INTEGER NOT NULL DEFAULT 0",
            "batter_birthday_runs_batted_in": "INTEGER NOT NULL DEFAULT 0",
            "pitcher_birthday_plate_appearances": "INTEGER NOT NULL DEFAULT 0",
            "pitcher_birthday_at_bats": "INTEGER NOT NULL DEFAULT 0",
            "pitcher_birthday_hits": "INTEGER NOT NULL DEFAULT 0",
            "pitcher_birthday_doubles": "INTEGER NOT NULL DEFAULT 0",
            "pitcher_birthday_triples": "INTEGER NOT NULL DEFAULT 0",
            "pitcher_birthday_home_runs": "INTEGER NOT NULL DEFAULT 0",
            "pitcher_birthday_walks": "INTEGER NOT NULL DEFAULT 0",
            "pitcher_birthday_intentional_walks": "INTEGER NOT NULL DEFAULT 0",
            "pitcher_birthday_hit_by_pitch": "INTEGER NOT NULL DEFAULT 0",
            "pitcher_birthday_sacrifice_flies": "INTEGER NOT NULL DEFAULT 0",
            "pitcher_birthday_strikeouts": "INTEGER NOT NULL DEFAULT 0",
            "pitcher_birthday_runs_batted_in": "INTEGER NOT NULL DEFAULT 0",
        },
    )
    connection.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_fielding_bible_player_drs_lookup
        ON fielding_bible_player_drs (season, snapshot_at, pos_abbr, total DESC);

        CREATE INDEX IF NOT EXISTS idx_fielding_bible_player_drs_player
        ON fielding_bible_player_drs (player_id, season, snapshot_at);

        CREATE INDEX IF NOT EXISTS idx_fielding_bible_player_drs_name
        ON fielding_bible_player_drs (player COLLATE NOCASE, season, snapshot_at);

        CREATE INDEX IF NOT EXISTS idx_fielding_bible_team_drs_lookup
        ON fielding_bible_team_drs (season, snapshot_at, total DESC);

        CREATE INDEX IF NOT EXISTS idx_fielding_bible_team_drs_name
        ON fielding_bible_team_drs (nickname COLLATE NOCASE, season, snapshot_at);

        CREATE INDEX IF NOT EXISTS idx_statcast_team_games_season_date
        ON statcast_team_games (season, game_date, team);

        CREATE INDEX IF NOT EXISTS idx_statcast_team_games_team
        ON statcast_team_games (team, season, game_date);

        CREATE INDEX IF NOT EXISTS idx_statcast_pitcher_games_season_date
        ON statcast_pitcher_games (season, game_date, pitcher_id);

        CREATE INDEX IF NOT EXISTS idx_statcast_pitcher_games_pitcher
        ON statcast_pitcher_games (pitcher_id, season, game_date);

        CREATE INDEX IF NOT EXISTS idx_statcast_pitcher_games_team
        ON statcast_pitcher_games (team, season, game_date);

        CREATE INDEX IF NOT EXISTS idx_statcast_batter_games_season_date
        ON statcast_batter_games (season, game_date, batter_id);

        CREATE INDEX IF NOT EXISTS idx_statcast_batter_games_batter
        ON statcast_batter_games (batter_id, season, game_date);

        CREATE INDEX IF NOT EXISTS idx_statcast_batter_games_team
        ON statcast_batter_games (team, season, game_date);

        CREATE INDEX IF NOT EXISTS idx_statcast_pitch_type_games_season_pitcher
        ON statcast_pitch_type_games (season, pitcher_id, pitch_family, pitch_type, game_date);

        CREATE INDEX IF NOT EXISTS idx_statcast_pitch_type_games_team
        ON statcast_pitch_type_games (team, season, pitch_family, game_date);

        CREATE INDEX IF NOT EXISTS idx_statcast_batter_pitch_type_games_season_batter
        ON statcast_batter_pitch_type_games (season, batter_id, pitch_family, pitch_type, game_date);

        CREATE INDEX IF NOT EXISTS idx_statcast_batter_pitch_type_games_team
        ON statcast_batter_pitch_type_games (team, season, pitch_family, game_date);

        CREATE INDEX IF NOT EXISTS idx_statcast_events_season_date
        ON statcast_events (season, game_date, event);

        CREATE INDEX IF NOT EXISTS idx_statcast_events_batter
        ON statcast_events (batter_id, season, game_date);

        CREATE INDEX IF NOT EXISTS idx_statcast_events_pitcher
        ON statcast_events (pitcher_id, season, game_date);

        CREATE INDEX IF NOT EXISTS idx_statcast_events_filters
        ON statcast_events (pitch_family, event, horizontal_location, vertical_location, field_direction, home_team, season);

        CREATE INDEX IF NOT EXISTS idx_statcast_events_count_filters
        ON statcast_events (count_key, event, season, game_date);

        CREATE INDEX IF NOT EXISTS idx_retrosheet_team_split_games_lookup
        ON retrosheet_team_split_games (split_key, season, game_date, team);

        CREATE INDEX IF NOT EXISTS idx_retrosheet_team_split_games_team
        ON retrosheet_team_split_games (team, season, split_key, game_date);

        CREATE INDEX IF NOT EXISTS idx_retrosheet_player_count_splits_lookup
        ON retrosheet_player_count_splits (count_key, plate_appearances DESC, player_id);

        CREATE INDEX IF NOT EXISTS idx_retrosheet_player_reached_count_splits_lookup
        ON retrosheet_player_reached_count_splits (count_key, plate_appearances DESC, player_id);

        CREATE INDEX IF NOT EXISTS idx_retrosheet_player_opponent_contexts_lookup
        ON retrosheet_player_opponent_contexts (context_key, opponent, plate_appearances DESC, player_id);

        CREATE INDEX IF NOT EXISTS idx_retrosheet_player_opponent_pitcher_cohorts_lookup
        ON retrosheet_player_opponent_pitcher_cohorts (cohort_kind, cohort_value, plate_appearances DESC, player_id);

        CREATE INDEX IF NOT EXISTS idx_retrosheet_player_opponent_pitchers_pitcher
        ON retrosheet_player_opponent_pitchers (pitcher_id, plate_appearances DESC, player_id);

        CREATE INDEX IF NOT EXISTS idx_retrosheet_player_streak_records_lookup
        ON retrosheet_player_streak_records (streak_key, streak_length DESC, player_id);

        """
    )
    if table_exists(connection, "retrosheet_batting"):
        connection.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_retrosheet_batting_player_date
            ON retrosheet_batting (id, date, gametype, stattype);

            CREATE INDEX IF NOT EXISTS idx_retrosheet_batting_date_player
            ON retrosheet_batting (date, id, gametype, stattype);
            """
        )
    if table_exists(connection, "lahman_people"):
        people_columns = list_table_columns(connection, "lahman_people")
        if {"retroid", "birthmonth", "birthday"}.issubset(people_columns):
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_lahman_people_retroid_birth
                ON lahman_people (retroid, birthmonth, birthday)
                """
            )
    connection.commit()


def sync_metric_catalog(connection: sqlite3.Connection, catalog: MetricCatalog) -> None:
    initialize_database(connection)
    connection.execute("DELETE FROM metrics")
    connection.execute("DELETE FROM metrics_fts")
    for metric in catalog.metrics:
        _insert_metric(connection, metric)
    connection.commit()


def _insert_metric(connection: sqlite3.Connection, metric: MetricDefinition) -> None:
    connection.execute(
        """
        INSERT INTO metrics (
            name,
            aliases_json,
            category,
            definition,
            formula,
            exact_formula_public,
            notes,
            historical_support,
            live_support,
            citations_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            metric.name,
            json.dumps(metric.aliases),
            metric.category,
            metric.definition,
            metric.formula,
            1 if metric.exact_formula_public else 0,
            metric.notes,
            metric.historical_support,
            metric.live_support,
            json.dumps(metric.citations),
        ),
    )
    connection.execute(
        """
        INSERT INTO metrics_fts (
            name,
            aliases,
            definition,
            formula,
            notes,
            historical_support,
            live_support
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            metric.name,
            " ".join(metric.aliases),
            metric.definition,
            metric.formula,
            metric.notes,
            metric.historical_support,
            metric.live_support,
        ),
    )


def normalize_identifier(value: str) -> str:
    cleaned = re.sub(r"[^0-9a-zA-Z_]+", "_", value.strip()).strip("_").lower()
    if not cleaned:
        cleaned = "column"
    if cleaned[0].isdigit():
        cleaned = f"c_{cleaned}"
    return cleaned


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def list_table_columns(connection: sqlite3.Connection, table_name: str) -> list[str]:
    if not table_exists(connection, table_name):
        return []
    rows = connection.execute(f"PRAGMA table_info({quote_identifier(table_name)})").fetchall()
    return [str(row["name"]) for row in rows]


def _ensure_table_columns(
    connection: sqlite3.Connection,
    table_name: str,
    columns: dict[str, str],
) -> None:
    existing = {column.lower() for column in list_table_columns(connection, table_name)}
    for name, column_sql in columns.items():
        if name.lower() in existing:
            continue
        connection.execute(
            f"ALTER TABLE {quote_identifier(table_name)} ADD COLUMN {quote_identifier(name)} {column_sql}"
        )


def resolve_column(connection: sqlite3.Connection, table_name: str, candidates: Iterable[str]) -> str | None:
    columns = {column.lower(): column for column in list_table_columns(connection, table_name)}
    for candidate in candidates:
        normalized = normalize_identifier(candidate)
        if normalized in columns:
            return columns[normalized]
    return None


def import_csv_file(
    connection: sqlite3.Connection,
    csv_path: Path,
    *,
    table_name: str,
    source_name: str,
    dataset_name: str,
    notes: str = "",
) -> int:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{csv_path} has no header row")
        original_headers = [header or "" for header in reader.fieldnames]
        headers = _sanitize_headers(original_headers)
        connection.execute(f"DROP TABLE IF EXISTS {quote_identifier(table_name)}")
        column_sql = ", ".join(f"{quote_identifier(header)} TEXT" for header in headers)
        connection.execute(f"CREATE TABLE {quote_identifier(table_name)} ({column_sql})")
        row_count = 0
        batch: list[tuple[Any, ...]] = []
        placeholders = ", ".join("?" for _ in headers)
        insert_sql = (
            f"INSERT INTO {quote_identifier(table_name)} "
            f"({', '.join(quote_identifier(header) for header in headers)}) "
            f"VALUES ({placeholders})"
        )
        for row in reader:
            normalized = tuple((row.get(header_name) or "").strip() for header_name in original_headers)
            batch.append(normalized)
            row_count += 1
            if len(batch) >= 2000:
                connection.executemany(insert_sql, batch)
                batch.clear()
        if batch:
            connection.executemany(insert_sql, batch)

    _create_common_indexes(connection, table_name)
    connection.execute(
        """
        INSERT OR REPLACE INTO csv_manifests (
            source_name,
            dataset_name,
            table_name,
            columns_json,
            row_count,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (source_name, dataset_name, table_name, json.dumps(headers), row_count, notes),
    )
    connection.commit()
    return row_count


def import_statcast_history_exports(
    connection: sqlite3.Connection,
    *,
    batter_csv: Path | None = None,
    pitcher_csv: Path | None = None,
) -> list[str]:
    initialize_database(connection)
    messages: list[str] = []
    if batter_csv is not None:
        row_count = import_csv_file(
            connection,
            batter_csv,
            table_name=STATCAST_HISTORY_BATTER_TABLE,
            source_name="Statcast Custom Leaderboards",
            dataset_name="batter_stats_history",
            notes="Imported from a custom Statcast batter history leaderboard export.",
        )
        messages.append(
            f"Imported {row_count} rows from {batter_csv.name} into {STATCAST_HISTORY_BATTER_TABLE}."
        )
    if pitcher_csv is not None:
        row_count = import_csv_file(
            connection,
            pitcher_csv,
            table_name=STATCAST_HISTORY_PITCHER_TABLE,
            source_name="Statcast Custom Leaderboards",
            dataset_name="pitcher_stats_history",
            notes="Imported from a custom Statcast pitcher history leaderboard export.",
        )
        messages.append(
            f"Imported {row_count} rows from {pitcher_csv.name} into {STATCAST_HISTORY_PITCHER_TABLE}."
        )
    return messages


def _sanitize_headers(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    sanitized: list[str] = []
    for header in headers:
        candidate = normalize_identifier(header)
        count = seen.get(candidate, 0)
        seen[candidate] = count + 1
        if count:
            candidate = f"{candidate}_{count + 1}"
        sanitized.append(candidate)
    return sanitized


def _create_common_indexes(connection: sqlite3.Connection, table_name: str) -> None:
    columns = list_table_columns(connection, table_name)
    lower_columns = {column.lower(): column for column in columns}
    candidate_sets = [
        ("player_id",),
        ("playerid",),
        ("year",),
        ("yearid",),
        ("teamid",),
        ("game_id", "gameid"),
        ("date", "gamedate"),
        ("last_name_first_name",),
        ("namefirst", "namelast"),
    ]
    for index_number, candidates in enumerate(candidate_sets, start=1):
        chosen = [lower_columns[candidate] for candidate in candidates if candidate in lower_columns]
        if not chosen:
            continue
        index_name = normalize_identifier(f"idx_{table_name}_{'_'.join(chosen)}_{index_number}")
        columns_sql = ", ".join(quote_identifier(column) for column in chosen)
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS {quote_identifier(index_name)} "
            f"ON {quote_identifier(table_name)} ({columns_sql})"
        )


def replace_document_chunks(
    connection: sqlite3.Connection,
    *,
    source_kind: str,
    source_name: str,
    chunks: list[dict[str, Any]],
) -> None:
    initialize_database(connection)
    connection.execute("DELETE FROM document_chunks WHERE source_name=?", (source_name,))
    connection.execute("DELETE FROM document_chunks_fts WHERE source_name=?", (source_name,))
    for chunk in chunks:
        connection.execute(
            """
            INSERT INTO document_chunks (
                source_kind,
                source_name,
                title,
                citation,
                content,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                source_kind,
                source_name,
                chunk["title"],
                chunk["citation"],
                chunk["content"],
                json.dumps(chunk.get("metadata", {})),
            ),
        )
        connection.execute(
            """
            INSERT INTO document_chunks_fts (
                source_name,
                title,
                citation,
                content
            ) VALUES (?, ?, ?, ?)
            """,
            (source_name, chunk["title"], chunk["citation"], chunk["content"]),
        )
    connection.commit()


def chunk_text(text: str, *, max_chars: int = 1400, overlap: int = 200) -> list[str]:
    normalized = re.sub(r"\s+", " ", text).strip()
    if not normalized:
        return []
    chunks: list[str] = []
    cursor = 0
    while cursor < len(normalized):
        end = min(len(normalized), cursor + max_chars)
        chunk = normalized[cursor:end]
        if end < len(normalized):
            split = chunk.rfind(". ")
            if split > max_chars // 2:
                chunk = chunk[: split + 1]
                end = cursor + len(chunk)
        chunks.append(chunk.strip())
        if end >= len(normalized):
            break
        cursor = max(0, end - overlap)
    return chunks


def search_document_chunks(connection: sqlite3.Connection, query: str, *, limit: int = 5) -> list[sqlite3.Row]:
    raw_tokens = re.findall(r"[a-zA-Z0-9+]+", query.lower())
    tokens = []
    for token in raw_tokens:
        normalized = re.sub(r"[^a-z0-9]+", "", token)
        if len(normalized) > 2:
            tokens.append(normalized)
    if not tokens:
        return []
    fts_query = " OR ".join(f"{token}*" for token in tokens[:8])
    return connection.execute(
        """
        SELECT rowid, source_name, title, citation, content
        FROM document_chunks_fts
        WHERE document_chunks_fts MATCH ?
        LIMIT ?
        """,
        (fts_query, limit),
    ).fetchall()


def fetch_rows(
    connection: sqlite3.Connection,
    sql: str,
    parameters: Iterable[Any] | None = None,
) -> list[sqlite3.Row]:
    return connection.execute(sql, tuple(parameters or ())).fetchall()


def get_metadata_value(connection: sqlite3.Connection, key: str) -> str | None:
    initialize_database(connection)
    row = connection.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
    if row is None:
        return None
    return str(row["value"])


def set_metadata_value(connection: sqlite3.Connection, key: str, value: str) -> None:
    initialize_database(connection)
    connection.execute(
        """
        INSERT INTO metadata (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )
    connection.commit()


def replace_statcast_team_games(
    connection: sqlite3.Connection,
    *,
    start_date: str,
    end_date: str,
    rows: Iterable[dict[str, Any]],
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            _coerce_int(row.get("season")),
            str(row.get("game_date") or ""),
            _coerce_int(row.get("game_pk")),
            str(row.get("team") or "").strip(),
            str(row.get("team_name") or row.get("team") or "").strip(),
            str(row.get("opponent") or "").strip(),
            str(row.get("opponent_name") or row.get("opponent") or "").strip(),
            _coerce_int(row.get("is_home")),
            _coerce_int(row.get("plate_appearances")),
            _coerce_int(row.get("at_bats")),
            _coerce_int(row.get("hits")),
            _coerce_int(row.get("strikeouts")),
            _coerce_int(row.get("batted_ball_events")),
            float(row.get("xba_numerator") or 0.0),
            float(row.get("xwoba_numerator") or 0.0),
            float(row.get("xwoba_denom") or 0.0),
            float(row.get("xslg_numerator") or 0.0),
            _coerce_int(row.get("hard_hit_bbe")),
            _coerce_int(row.get("barrel_bbe")),
            float(row.get("launch_speed_sum") or 0.0),
            _coerce_int(row.get("launch_speed_count")),
        )
        for row in rows
        if str(row.get("game_date") or "").strip() and str(row.get("team") or "").strip()
    ]
    connection.execute(
        """
        DELETE FROM statcast_team_games
        WHERE game_date >= ? AND game_date <= ?
        """,
        (start_date, end_date),
    )
    if normalized_rows:
        connection.executemany(
            """
            INSERT INTO statcast_team_games (
                season,
                game_date,
                game_pk,
                team,
                team_name,
                opponent,
                opponent_name,
                is_home,
                plate_appearances,
                at_bats,
                hits,
                strikeouts,
                batted_ball_events,
                xba_numerator,
                xwoba_numerator,
                xwoba_denom,
                xslg_numerator,
                hard_hit_bbe,
                barrel_bbe,
                launch_speed_sum,
                launch_speed_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            normalized_rows,
        )
    connection.commit()
    return len(normalized_rows)


def replace_statcast_pitcher_games(
    connection: sqlite3.Connection,
    *,
    start_date: str,
    end_date: str,
    rows: Iterable[dict[str, Any]],
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            _coerce_int(row.get("season")),
            str(row.get("game_date") or ""),
            _coerce_int(row.get("game_pk")),
            _coerce_int(row.get("pitcher_id")),
            str(row.get("pitcher_name") or "").strip(),
            str(row.get("team") or "").strip(),
            str(row.get("team_name") or row.get("team") or "").strip(),
            str(row.get("opponent") or "").strip(),
            str(row.get("opponent_name") or row.get("opponent") or "").strip(),
            _coerce_int(row.get("total_pitches")),
            float(row.get("max_release_speed") or 0.0) if row.get("max_release_speed") not in (None, "") else None,
            _coerce_int(row.get("pitches_95_plus")),
            _coerce_int(row.get("pitches_97_plus")),
            _coerce_int(row.get("pitches_98_plus")),
            _coerce_int(row.get("pitches_99_plus")),
            _coerce_int(row.get("pitches_100_plus")),
            _coerce_int(row.get("pitches_101_plus")),
            _coerce_int(row.get("pitches_102_plus")),
            _coerce_int(row.get("fastball_pitches")),
            _coerce_int(row.get("fastball_strikeouts")),
            _coerce_int(row.get("changeup_pitches")),
            _coerce_int(row.get("changeup_strikeouts")),
            _coerce_int(row.get("curveball_pitches")),
            _coerce_int(row.get("curveball_strikeouts")),
            _coerce_int(row.get("slider_pitches")),
            _coerce_int(row.get("slider_strikeouts")),
        )
        for row in rows
        if str(row.get("game_date") or "").strip() and _coerce_int(row.get("pitcher_id")) and str(row.get("pitcher_name") or "").strip()
    ]
    connection.execute(
        """
        DELETE FROM statcast_pitcher_games
        WHERE game_date >= ? AND game_date <= ?
        """,
        (start_date, end_date),
    )
    if normalized_rows:
        connection.executemany(
            """
            INSERT INTO statcast_pitcher_games (
                season,
                game_date,
                game_pk,
                pitcher_id,
                pitcher_name,
                team,
                team_name,
                opponent,
                opponent_name,
                total_pitches,
                max_release_speed,
                pitches_95_plus,
                pitches_97_plus,
                pitches_98_plus,
                pitches_99_plus,
                pitches_100_plus,
                pitches_101_plus,
                pitches_102_plus,
                fastball_pitches,
                fastball_strikeouts,
                changeup_pitches,
                changeup_strikeouts,
                curveball_pitches,
                curveball_strikeouts,
                slider_pitches,
                slider_strikeouts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            normalized_rows,
        )
    connection.commit()
    return len(normalized_rows)


def replace_statcast_batter_games(
    connection: sqlite3.Connection,
    *,
    start_date: str,
    end_date: str,
    rows: Iterable[dict[str, Any]],
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            _coerce_int(row.get("season")),
            str(row.get("game_date") or ""),
            _coerce_int(row.get("game_pk")),
            _coerce_int(row.get("batter_id")),
            str(row.get("batter_name") or "").strip(),
            str(row.get("team") or "").strip(),
            str(row.get("team_name") or row.get("team") or "").strip(),
            str(row.get("opponent") or "").strip(),
            str(row.get("opponent_name") or row.get("opponent") or "").strip(),
            _coerce_int(row.get("plate_appearances")),
            _coerce_int(row.get("at_bats")),
            _coerce_int(row.get("hits")),
            _coerce_int(row.get("singles")),
            _coerce_int(row.get("doubles")),
            _coerce_int(row.get("triples")),
            _coerce_int(row.get("home_runs")),
            _coerce_int(row.get("walks")),
            _coerce_int(row.get("strikeouts")),
            _coerce_int(row.get("runs_batted_in")),
            _coerce_int(row.get("batted_ball_events")),
            float(row.get("xba_numerator") or 0.0),
            float(row.get("xwoba_numerator") or 0.0),
            float(row.get("xwoba_denom") or 0.0),
            float(row.get("xslg_numerator") or 0.0),
            _coerce_int(row.get("hard_hit_bbe")),
            _coerce_int(row.get("barrel_bbe")),
            float(row.get("launch_speed_sum") or 0.0),
            _coerce_int(row.get("launch_speed_count")),
            _coerce_float(row.get("max_launch_speed")),
            _coerce_float(row.get("avg_bat_speed")),
            _coerce_float(row.get("max_bat_speed")),
        )
        for row in rows
        if str(row.get("game_date") or "").strip()
        and _coerce_int(row.get("batter_id"))
        and str(row.get("batter_name") or "").strip()
    ]
    connection.execute(
        """
        DELETE FROM statcast_batter_games
        WHERE game_date >= ? AND game_date <= ?
        """,
        (start_date, end_date),
    )
    if normalized_rows:
        connection.executemany(
            """
            INSERT INTO statcast_batter_games (
                season,
                game_date,
                game_pk,
                batter_id,
                batter_name,
                team,
                team_name,
                opponent,
                opponent_name,
                plate_appearances,
                at_bats,
                hits,
                singles,
                doubles,
                triples,
                home_runs,
                walks,
                strikeouts,
                runs_batted_in,
                batted_ball_events,
                xba_numerator,
                xwoba_numerator,
                xwoba_denom,
                xslg_numerator,
                hard_hit_bbe,
                barrel_bbe,
                launch_speed_sum,
                launch_speed_count,
                max_launch_speed,
                avg_bat_speed,
                max_bat_speed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            normalized_rows,
        )
    connection.commit()
    return len(normalized_rows)


def replace_statcast_pitch_type_games(
    connection: sqlite3.Connection,
    *,
    start_date: str,
    end_date: str,
    rows: Iterable[dict[str, Any]],
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            _coerce_int(row.get("season")),
            str(row.get("game_date") or ""),
            _coerce_int(row.get("game_pk")),
            _coerce_int(row.get("pitcher_id")),
            str(row.get("pitcher_name") or "").strip(),
            str(row.get("team") or "").strip(),
            str(row.get("team_name") or row.get("team") or "").strip(),
            str(row.get("opponent") or "").strip(),
            str(row.get("opponent_name") or row.get("opponent") or "").strip(),
            str(row.get("pitch_type") or "").strip(),
            str(row.get("pitch_name") or "").strip(),
            str(row.get("pitch_family") or "").strip(),
            _coerce_int(row.get("pitches")),
            _coerce_float(row.get("avg_release_speed")),
            _coerce_float(row.get("max_release_speed")),
            _coerce_float(row.get("avg_release_spin_rate")),
            _coerce_float(row.get("max_release_spin_rate")),
            _coerce_int(row.get("called_strikes")),
            _coerce_int(row.get("swinging_strikes")),
            _coerce_int(row.get("whiffs")),
            _coerce_int(row.get("strikeouts")),
            _coerce_int(row.get("walks")),
            _coerce_int(row.get("hits_allowed")),
            _coerce_int(row.get("extra_base_hits_allowed")),
            _coerce_int(row.get("home_runs_allowed")),
            _coerce_int(row.get("batted_ball_events")),
            float(row.get("xba_numerator") or 0.0),
            float(row.get("xwoba_numerator") or 0.0),
            float(row.get("xwoba_denom") or 0.0),
            float(row.get("xslg_numerator") or 0.0),
            float(row.get("launch_speed_sum") or 0.0),
            _coerce_int(row.get("launch_speed_count")),
        )
        for row in rows
        if str(row.get("game_date") or "").strip()
        and _coerce_int(row.get("pitcher_id"))
        and str(row.get("pitch_type") or "").strip()
    ]
    connection.execute(
        """
        DELETE FROM statcast_pitch_type_games
        WHERE game_date >= ? AND game_date <= ?
        """,
        (start_date, end_date),
    )
    if normalized_rows:
        connection.executemany(
            """
            INSERT INTO statcast_pitch_type_games (
                season,
                game_date,
                game_pk,
                pitcher_id,
                pitcher_name,
                team,
                team_name,
                opponent,
                opponent_name,
                pitch_type,
                pitch_name,
                pitch_family,
                pitches,
                avg_release_speed,
                max_release_speed,
                avg_release_spin_rate,
                max_release_spin_rate,
                called_strikes,
                swinging_strikes,
                whiffs,
                strikeouts,
                walks,
                hits_allowed,
                extra_base_hits_allowed,
                home_runs_allowed,
                batted_ball_events,
                xba_numerator,
                xwoba_numerator,
                xwoba_denom,
                xslg_numerator,
                launch_speed_sum,
                launch_speed_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            normalized_rows,
        )
    connection.commit()
    return len(normalized_rows)


def replace_statcast_batter_pitch_type_games(
    connection: sqlite3.Connection,
    *,
    start_date: str,
    end_date: str,
    rows: Iterable[dict[str, Any]],
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            _coerce_int(row.get("season")),
            str(row.get("game_date") or ""),
            _coerce_int(row.get("game_pk")),
            _coerce_int(row.get("batter_id")),
            str(row.get("batter_name") or "").strip(),
            str(row.get("team") or "").strip(),
            str(row.get("team_name") or row.get("team") or "").strip(),
            str(row.get("opponent") or "").strip(),
            str(row.get("opponent_name") or row.get("opponent") or "").strip(),
            str(row.get("pitch_type") or "").strip(),
            str(row.get("pitch_name") or "").strip(),
            str(row.get("pitch_family") or "").strip(),
            _coerce_int(row.get("plate_appearances")),
            _coerce_int(row.get("at_bats")),
            _coerce_int(row.get("hits")),
            _coerce_int(row.get("singles")),
            _coerce_int(row.get("doubles")),
            _coerce_int(row.get("triples")),
            _coerce_int(row.get("home_runs")),
            _coerce_int(row.get("walks")),
            _coerce_int(row.get("strikeouts")),
            _coerce_int(row.get("runs_batted_in")),
            _coerce_int(row.get("batted_ball_events")),
            float(row.get("xba_numerator") or 0.0),
            float(row.get("xwoba_numerator") or 0.0),
            float(row.get("xwoba_denom") or 0.0),
            float(row.get("xslg_numerator") or 0.0),
            _coerce_int(row.get("hard_hit_bbe")),
            _coerce_int(row.get("barrel_bbe")),
            float(row.get("launch_speed_sum") or 0.0),
            _coerce_int(row.get("launch_speed_count")),
            _coerce_float(row.get("avg_bat_speed")),
            _coerce_float(row.get("max_bat_speed")),
        )
        for row in rows
        if str(row.get("game_date") or "").strip()
        and _coerce_int(row.get("batter_id"))
        and str(row.get("pitch_type") or "").strip()
    ]
    connection.execute(
        """
        DELETE FROM statcast_batter_pitch_type_games
        WHERE game_date >= ? AND game_date <= ?
        """,
        (start_date, end_date),
    )
    if normalized_rows:
        connection.executemany(
            """
            INSERT INTO statcast_batter_pitch_type_games (
                season,
                game_date,
                game_pk,
                batter_id,
                batter_name,
                team,
                team_name,
                opponent,
                opponent_name,
                pitch_type,
                pitch_name,
                pitch_family,
                plate_appearances,
                at_bats,
                hits,
                singles,
                doubles,
                triples,
                home_runs,
                walks,
                strikeouts,
                runs_batted_in,
                batted_ball_events,
                xba_numerator,
                xwoba_numerator,
                xwoba_denom,
                xslg_numerator,
                hard_hit_bbe,
                barrel_bbe,
                launch_speed_sum,
                launch_speed_count,
                avg_bat_speed,
                max_bat_speed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            normalized_rows,
        )
    connection.commit()
    return len(normalized_rows)


def replace_statcast_events(
    connection: sqlite3.Connection,
    *,
    start_date: str,
    end_date: str,
    rows: Iterable[dict[str, Any]],
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            _coerce_int(row.get("season")),
            str(row.get("game_date") or ""),
            _coerce_int(row.get("game_pk")),
            _coerce_int(row.get("at_bat_number")),
            _coerce_int(row.get("pitch_number")),
            _coerce_int(row.get("batter_id")),
            str(row.get("batter_name") or "").strip(),
            _coerce_int(row.get("pitcher_id")),
            str(row.get("pitcher_name") or "").strip(),
            str(row.get("batting_team") or "").strip(),
            str(row.get("pitching_team") or "").strip(),
            str(row.get("home_team") or "").strip(),
            str(row.get("away_team") or "").strip(),
            str(row.get("stand") or "").strip(),
            str(row.get("p_throws") or "").strip(),
            str(row.get("pitch_type") or "").strip(),
            str(row.get("pitch_name") or "").strip(),
            str(row.get("pitch_family") or "").strip(),
            str(row.get("event") or "").strip(),
            _coerce_int(row.get("is_ab")),
            _coerce_int(row.get("is_hit")),
            _coerce_int(row.get("is_xbh")),
            _coerce_int(row.get("is_home_run")),
            _coerce_int(row.get("is_strikeout")),
            _coerce_int(row.get("has_risp")),
            _coerce_int(row.get("balls")),
            _coerce_int(row.get("strikes")),
            str(row.get("count_key") or "").strip(),
            _coerce_int(row.get("outs_when_up")),
            _coerce_int(row.get("runs_batted_in")),
            str(row.get("horizontal_location") or "").strip(),
            str(row.get("vertical_location") or "").strip(),
            str(row.get("field_direction") or "").strip(),
            _coerce_float(row.get("release_speed")),
            _coerce_float(row.get("release_spin_rate")),
            _coerce_float(row.get("launch_speed")),
            _coerce_float(row.get("launch_angle")),
            _coerce_float(row.get("hit_distance")),
            _coerce_float(row.get("bat_speed")),
            _coerce_float(row.get("estimated_ba")),
            _coerce_float(row.get("estimated_woba")),
            _coerce_float(row.get("estimated_slg")),
        )
        for row in rows
        if str(row.get("game_date") or "").strip()
        and _coerce_int(row.get("game_pk")) is not None
        and _coerce_int(row.get("at_bat_number")) is not None
        and _coerce_int(row.get("pitch_number")) is not None
    ]
    connection.execute(
        """
        DELETE FROM statcast_events
        WHERE game_date >= ? AND game_date <= ?
        """,
        (start_date, end_date),
    )
    if normalized_rows:
        connection.executemany(
            """
            INSERT INTO statcast_events (
                season,
                game_date,
                game_pk,
                at_bat_number,
                pitch_number,
                batter_id,
                batter_name,
                pitcher_id,
                pitcher_name,
                batting_team,
                pitching_team,
                home_team,
                away_team,
                stand,
                p_throws,
                pitch_type,
                pitch_name,
                pitch_family,
                event,
                is_ab,
                is_hit,
                is_xbh,
                is_home_run,
                is_strikeout,
                has_risp,
                balls,
                strikes,
                count_key,
                outs_when_up,
                runs_batted_in,
                horizontal_location,
                vertical_location,
                field_direction,
                release_speed,
                release_spin_rate,
                launch_speed,
                launch_angle,
                hit_distance,
                bat_speed,
                estimated_ba,
                estimated_woba,
                estimated_slg
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            normalized_rows,
        )
    connection.commit()
    return len(normalized_rows)


def clear_retrosheet_team_split_games(connection: sqlite3.Connection) -> None:
    initialize_database(connection)
    connection.execute("DELETE FROM retrosheet_team_split_games")
    connection.commit()


def clear_retrosheet_player_count_splits(connection: sqlite3.Connection) -> None:
    initialize_database(connection)
    connection.execute("DELETE FROM retrosheet_player_count_splits")
    connection.commit()


def clear_retrosheet_player_reached_count_splits(connection: sqlite3.Connection) -> None:
    initialize_database(connection)
    connection.execute("DELETE FROM retrosheet_player_reached_count_splits")
    connection.commit()


def clear_retrosheet_player_opponent_contexts(connection: sqlite3.Connection) -> None:
    initialize_database(connection)
    connection.execute("DELETE FROM retrosheet_player_opponent_contexts")
    connection.commit()


def clear_retrosheet_player_opponent_pitcher_cohorts(connection: sqlite3.Connection) -> None:
    initialize_database(connection)
    connection.execute("DELETE FROM retrosheet_player_opponent_pitcher_cohorts")
    connection.commit()


def clear_retrosheet_player_opponent_pitchers(connection: sqlite3.Connection) -> None:
    initialize_database(connection)
    connection.execute("DELETE FROM retrosheet_player_opponent_pitchers")
    connection.commit()


def clear_retrosheet_player_streak_records(connection: sqlite3.Connection) -> None:
    initialize_database(connection)
    connection.execute("DELETE FROM retrosheet_player_streak_records")
    connection.commit()


def upsert_retrosheet_team_split_games(
    connection: sqlite3.Connection,
    rows: Iterable[dict[str, Any]],
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            _coerce_int(row.get("season")),
            str(row.get("game_date") or ""),
            str(row.get("gid") or ""),
            str(row.get("team") or ""),
            str(row.get("split_key") or ""),
            _coerce_int(row.get("plate_appearances")),
            _coerce_int(row.get("at_bats")),
            _coerce_int(row.get("hits")),
            _coerce_int(row.get("doubles")),
            _coerce_int(row.get("triples")),
            _coerce_int(row.get("home_runs")),
            _coerce_int(row.get("walks")),
            _coerce_int(row.get("hit_by_pitch")),
            _coerce_int(row.get("sacrifice_flies")),
            _coerce_int(row.get("strikeouts")),
            _coerce_int(row.get("runs_batted_in")),
        )
        for row in rows
        if row.get("gid") and row.get("team") and row.get("split_key")
    ]
    if not normalized_rows:
        return 0
    connection.executemany(
        """
        INSERT INTO retrosheet_team_split_games (
            season,
            game_date,
            gid,
            team,
            split_key,
            plate_appearances,
            at_bats,
            hits,
            doubles,
            triples,
            home_runs,
            walks,
            hit_by_pitch,
            sacrifice_flies,
            strikeouts,
            runs_batted_in
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(gid, team, split_key) DO UPDATE SET
            season = excluded.season,
            game_date = excluded.game_date,
            plate_appearances = retrosheet_team_split_games.plate_appearances + excluded.plate_appearances,
            at_bats = retrosheet_team_split_games.at_bats + excluded.at_bats,
            hits = retrosheet_team_split_games.hits + excluded.hits,
            doubles = retrosheet_team_split_games.doubles + excluded.doubles,
            triples = retrosheet_team_split_games.triples + excluded.triples,
            home_runs = retrosheet_team_split_games.home_runs + excluded.home_runs,
            walks = retrosheet_team_split_games.walks + excluded.walks,
            hit_by_pitch = retrosheet_team_split_games.hit_by_pitch + excluded.hit_by_pitch,
            sacrifice_flies = retrosheet_team_split_games.sacrifice_flies + excluded.sacrifice_flies,
            strikeouts = retrosheet_team_split_games.strikeouts + excluded.strikeouts,
            runs_batted_in = retrosheet_team_split_games.runs_batted_in + excluded.runs_batted_in,
            imported_at = CURRENT_TIMESTAMP
        """,
        normalized_rows,
    )
    connection.commit()
    return len(normalized_rows)


def upsert_retrosheet_player_count_splits(
    connection: sqlite3.Connection,
    rows: Iterable[dict[str, Any]],
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            str(row.get("player_id") or ""),
            str(row.get("count_key") or ""),
            _coerce_int(row.get("plate_appearances")),
            _coerce_int(row.get("at_bats")),
            _coerce_int(row.get("hits")),
            _coerce_int(row.get("doubles")),
            _coerce_int(row.get("triples")),
            _coerce_int(row.get("home_runs")),
            _coerce_int(row.get("walks")),
            _coerce_int(row.get("hit_by_pitch")),
            _coerce_int(row.get("sacrifice_flies")),
            _coerce_int(row.get("strikeouts")),
            _coerce_int(row.get("runs_batted_in")),
            _coerce_int(row.get("first_season")),
            _coerce_int(row.get("last_season")),
        )
        for row in rows
        if row.get("player_id") and row.get("count_key")
    ]
    if not normalized_rows:
        return 0
    connection.executemany(
        """
        INSERT INTO retrosheet_player_count_splits (
            player_id,
            count_key,
            plate_appearances,
            at_bats,
            hits,
            doubles,
            triples,
            home_runs,
            walks,
            hit_by_pitch,
            sacrifice_flies,
            strikeouts,
            runs_batted_in,
            first_season,
            last_season
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_id, count_key) DO UPDATE SET
            plate_appearances = excluded.plate_appearances,
            at_bats = excluded.at_bats,
            hits = excluded.hits,
            doubles = excluded.doubles,
            triples = excluded.triples,
            home_runs = excluded.home_runs,
            walks = excluded.walks,
            hit_by_pitch = excluded.hit_by_pitch,
            sacrifice_flies = excluded.sacrifice_flies,
            strikeouts = excluded.strikeouts,
            runs_batted_in = excluded.runs_batted_in,
            first_season = excluded.first_season,
            last_season = excluded.last_season,
            imported_at = CURRENT_TIMESTAMP
        """,
        normalized_rows,
    )
    connection.commit()
    return len(normalized_rows)


def upsert_retrosheet_player_reached_count_splits(
    connection: sqlite3.Connection,
    rows: Iterable[dict[str, Any]],
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            str(row.get("player_id") or ""),
            str(row.get("count_key") or ""),
            _coerce_int(row.get("plate_appearances")),
            _coerce_int(row.get("at_bats")),
            _coerce_int(row.get("hits")),
            _coerce_int(row.get("doubles")),
            _coerce_int(row.get("triples")),
            _coerce_int(row.get("home_runs")),
            _coerce_int(row.get("walks")),
            _coerce_int(row.get("hit_by_pitch")),
            _coerce_int(row.get("sacrifice_flies")),
            _coerce_int(row.get("strikeouts")),
            _coerce_int(row.get("runs_batted_in")),
            _coerce_int(row.get("first_season")),
            _coerce_int(row.get("last_season")),
        )
        for row in rows
        if row.get("player_id") and row.get("count_key")
    ]
    if not normalized_rows:
        return 0
    connection.executemany(
        """
        INSERT INTO retrosheet_player_reached_count_splits (
            player_id,
            count_key,
            plate_appearances,
            at_bats,
            hits,
            doubles,
            triples,
            home_runs,
            walks,
            hit_by_pitch,
            sacrifice_flies,
            strikeouts,
            runs_batted_in,
            first_season,
            last_season
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_id, count_key) DO UPDATE SET
            plate_appearances = excluded.plate_appearances,
            at_bats = excluded.at_bats,
            hits = excluded.hits,
            doubles = excluded.doubles,
            triples = excluded.triples,
            home_runs = excluded.home_runs,
            walks = excluded.walks,
            hit_by_pitch = excluded.hit_by_pitch,
            sacrifice_flies = excluded.sacrifice_flies,
            strikeouts = excluded.strikeouts,
            runs_batted_in = excluded.runs_batted_in,
            first_season = excluded.first_season,
            last_season = excluded.last_season,
            imported_at = CURRENT_TIMESTAMP
        """,
        normalized_rows,
    )
    connection.commit()
    return len(normalized_rows)


def upsert_retrosheet_player_opponent_contexts(
    connection: sqlite3.Connection,
    rows: Iterable[dict[str, Any]],
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            str(row.get("player_id") or ""),
            str(row.get("opponent") or ""),
            str(row.get("context_key") or ""),
            _coerce_int(row.get("plate_appearances")),
            _coerce_int(row.get("at_bats")),
            _coerce_int(row.get("hits")),
            _coerce_int(row.get("doubles")),
            _coerce_int(row.get("triples")),
            _coerce_int(row.get("home_runs")),
            _coerce_int(row.get("walks")),
            _coerce_int(row.get("intentional_walks")),
            _coerce_int(row.get("hit_by_pitch")),
            _coerce_int(row.get("sacrifice_flies")),
            _coerce_int(row.get("strikeouts")),
            _coerce_int(row.get("runs_batted_in")),
            _coerce_int(row.get("first_season")),
            _coerce_int(row.get("last_season")),
        )
        for row in rows
        if row.get("player_id") and row.get("opponent") and row.get("context_key")
    ]
    if not normalized_rows:
        return 0
    connection.executemany(
        """
        INSERT INTO retrosheet_player_opponent_contexts (
            player_id,
            opponent,
            context_key,
            plate_appearances,
            at_bats,
            hits,
            doubles,
            triples,
            home_runs,
            walks,
            intentional_walks,
            hit_by_pitch,
            sacrifice_flies,
            strikeouts,
            runs_batted_in,
            first_season,
            last_season
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_id, opponent, context_key) DO UPDATE SET
            plate_appearances = excluded.plate_appearances,
            at_bats = excluded.at_bats,
            hits = excluded.hits,
            doubles = excluded.doubles,
            triples = excluded.triples,
            home_runs = excluded.home_runs,
            walks = excluded.walks,
            intentional_walks = excluded.intentional_walks,
            hit_by_pitch = excluded.hit_by_pitch,
            sacrifice_flies = excluded.sacrifice_flies,
            strikeouts = excluded.strikeouts,
            runs_batted_in = excluded.runs_batted_in,
            first_season = excluded.first_season,
            last_season = excluded.last_season,
            imported_at = CURRENT_TIMESTAMP
        """,
        normalized_rows,
    )
    connection.commit()
    return len(normalized_rows)


def upsert_retrosheet_player_opponent_pitcher_cohorts(
    connection: sqlite3.Connection,
    rows: Iterable[dict[str, Any]],
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            str(row.get("player_id") or ""),
            str(row.get("cohort_kind") or ""),
            str(row.get("cohort_value") or ""),
            _coerce_int(row.get("plate_appearances")),
            _coerce_int(row.get("at_bats")),
            _coerce_int(row.get("hits")),
            _coerce_int(row.get("doubles")),
            _coerce_int(row.get("triples")),
            _coerce_int(row.get("home_runs")),
            _coerce_int(row.get("walks")),
            _coerce_int(row.get("intentional_walks")),
            _coerce_int(row.get("hit_by_pitch")),
            _coerce_int(row.get("sacrifice_flies")),
            _coerce_int(row.get("strikeouts")),
            _coerce_int(row.get("runs_batted_in")),
            _coerce_int(row.get("pitchers_faced")),
            _coerce_int(row.get("first_season")),
            _coerce_int(row.get("last_season")),
        )
        for row in rows
        if row.get("player_id") and row.get("cohort_kind") and row.get("cohort_value")
    ]
    if not normalized_rows:
        return 0
    connection.executemany(
        """
        INSERT INTO retrosheet_player_opponent_pitcher_cohorts (
            player_id,
            cohort_kind,
            cohort_value,
            plate_appearances,
            at_bats,
            hits,
            doubles,
            triples,
            home_runs,
            walks,
            intentional_walks,
            hit_by_pitch,
            sacrifice_flies,
            strikeouts,
            runs_batted_in,
            pitchers_faced,
            first_season,
            last_season
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_id, cohort_kind, cohort_value) DO UPDATE SET
            plate_appearances = excluded.plate_appearances,
            at_bats = excluded.at_bats,
            hits = excluded.hits,
            doubles = excluded.doubles,
            triples = excluded.triples,
            home_runs = excluded.home_runs,
            walks = excluded.walks,
            intentional_walks = excluded.intentional_walks,
            hit_by_pitch = excluded.hit_by_pitch,
            sacrifice_flies = excluded.sacrifice_flies,
            strikeouts = excluded.strikeouts,
            runs_batted_in = excluded.runs_batted_in,
            pitchers_faced = excluded.pitchers_faced,
            first_season = excluded.first_season,
            last_season = excluded.last_season,
            imported_at = CURRENT_TIMESTAMP
        """,
        normalized_rows,
    )
    connection.commit()
    return len(normalized_rows)


def upsert_retrosheet_player_opponent_pitchers(
    connection: sqlite3.Connection,
    rows: Iterable[dict[str, Any]],
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            str(row.get("player_id") or ""),
            str(row.get("pitcher_id") or ""),
            _coerce_int(row.get("plate_appearances")),
            _coerce_int(row.get("at_bats")),
            _coerce_int(row.get("hits")),
            _coerce_int(row.get("doubles")),
            _coerce_int(row.get("triples")),
            _coerce_int(row.get("home_runs")),
            _coerce_int(row.get("walks")),
            _coerce_int(row.get("intentional_walks")),
            _coerce_int(row.get("hit_by_pitch")),
            _coerce_int(row.get("sacrifice_flies")),
            _coerce_int(row.get("strikeouts")),
            _coerce_int(row.get("runs_batted_in")),
            _coerce_int(row.get("batter_birthday_plate_appearances")),
            _coerce_int(row.get("batter_birthday_at_bats")),
            _coerce_int(row.get("batter_birthday_hits")),
            _coerce_int(row.get("batter_birthday_doubles")),
            _coerce_int(row.get("batter_birthday_triples")),
            _coerce_int(row.get("batter_birthday_home_runs")),
            _coerce_int(row.get("batter_birthday_walks")),
            _coerce_int(row.get("batter_birthday_intentional_walks")),
            _coerce_int(row.get("batter_birthday_hit_by_pitch")),
            _coerce_int(row.get("batter_birthday_sacrifice_flies")),
            _coerce_int(row.get("batter_birthday_strikeouts")),
            _coerce_int(row.get("batter_birthday_runs_batted_in")),
            _coerce_int(row.get("pitcher_birthday_plate_appearances")),
            _coerce_int(row.get("pitcher_birthday_at_bats")),
            _coerce_int(row.get("pitcher_birthday_hits")),
            _coerce_int(row.get("pitcher_birthday_doubles")),
            _coerce_int(row.get("pitcher_birthday_triples")),
            _coerce_int(row.get("pitcher_birthday_home_runs")),
            _coerce_int(row.get("pitcher_birthday_walks")),
            _coerce_int(row.get("pitcher_birthday_intentional_walks")),
            _coerce_int(row.get("pitcher_birthday_hit_by_pitch")),
            _coerce_int(row.get("pitcher_birthday_sacrifice_flies")),
            _coerce_int(row.get("pitcher_birthday_strikeouts")),
            _coerce_int(row.get("pitcher_birthday_runs_batted_in")),
            _coerce_int(row.get("first_season")),
            _coerce_int(row.get("last_season")),
        )
        for row in rows
        if row.get("player_id") and row.get("pitcher_id")
    ]
    if not normalized_rows:
        return 0
    connection.executemany(
        """
        INSERT INTO retrosheet_player_opponent_pitchers (
            player_id,
            pitcher_id,
            plate_appearances,
            at_bats,
            hits,
            doubles,
            triples,
            home_runs,
            walks,
            intentional_walks,
            hit_by_pitch,
            sacrifice_flies,
            strikeouts,
            runs_batted_in,
            batter_birthday_plate_appearances,
            batter_birthday_at_bats,
            batter_birthday_hits,
            batter_birthday_doubles,
            batter_birthday_triples,
            batter_birthday_home_runs,
            batter_birthday_walks,
            batter_birthday_intentional_walks,
            batter_birthday_hit_by_pitch,
            batter_birthday_sacrifice_flies,
            batter_birthday_strikeouts,
            batter_birthday_runs_batted_in,
            pitcher_birthday_plate_appearances,
            pitcher_birthday_at_bats,
            pitcher_birthday_hits,
            pitcher_birthday_doubles,
            pitcher_birthday_triples,
            pitcher_birthday_home_runs,
            pitcher_birthday_walks,
            pitcher_birthday_intentional_walks,
            pitcher_birthday_hit_by_pitch,
            pitcher_birthday_sacrifice_flies,
            pitcher_birthday_strikeouts,
            pitcher_birthday_runs_batted_in,
            first_season,
            last_season
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_id, pitcher_id) DO UPDATE SET
            plate_appearances = excluded.plate_appearances,
            at_bats = excluded.at_bats,
            hits = excluded.hits,
            doubles = excluded.doubles,
            triples = excluded.triples,
            home_runs = excluded.home_runs,
            walks = excluded.walks,
            intentional_walks = excluded.intentional_walks,
            hit_by_pitch = excluded.hit_by_pitch,
            sacrifice_flies = excluded.sacrifice_flies,
            strikeouts = excluded.strikeouts,
            runs_batted_in = excluded.runs_batted_in,
            batter_birthday_plate_appearances = excluded.batter_birthday_plate_appearances,
            batter_birthday_at_bats = excluded.batter_birthday_at_bats,
            batter_birthday_hits = excluded.batter_birthday_hits,
            batter_birthday_doubles = excluded.batter_birthday_doubles,
            batter_birthday_triples = excluded.batter_birthday_triples,
            batter_birthday_home_runs = excluded.batter_birthday_home_runs,
            batter_birthday_walks = excluded.batter_birthday_walks,
            batter_birthday_intentional_walks = excluded.batter_birthday_intentional_walks,
            batter_birthday_hit_by_pitch = excluded.batter_birthday_hit_by_pitch,
            batter_birthday_sacrifice_flies = excluded.batter_birthday_sacrifice_flies,
            batter_birthday_strikeouts = excluded.batter_birthday_strikeouts,
            batter_birthday_runs_batted_in = excluded.batter_birthday_runs_batted_in,
            pitcher_birthday_plate_appearances = excluded.pitcher_birthday_plate_appearances,
            pitcher_birthday_at_bats = excluded.pitcher_birthday_at_bats,
            pitcher_birthday_hits = excluded.pitcher_birthday_hits,
            pitcher_birthday_doubles = excluded.pitcher_birthday_doubles,
            pitcher_birthday_triples = excluded.pitcher_birthday_triples,
            pitcher_birthday_home_runs = excluded.pitcher_birthday_home_runs,
            pitcher_birthday_walks = excluded.pitcher_birthday_walks,
            pitcher_birthday_intentional_walks = excluded.pitcher_birthday_intentional_walks,
            pitcher_birthday_hit_by_pitch = excluded.pitcher_birthday_hit_by_pitch,
            pitcher_birthday_sacrifice_flies = excluded.pitcher_birthday_sacrifice_flies,
            pitcher_birthday_strikeouts = excluded.pitcher_birthday_strikeouts,
            pitcher_birthday_runs_batted_in = excluded.pitcher_birthday_runs_batted_in,
            first_season = excluded.first_season,
            last_season = excluded.last_season,
            imported_at = CURRENT_TIMESTAMP
        """,
        normalized_rows,
    )
    connection.commit()
    return len(normalized_rows)


def upsert_retrosheet_player_streak_records(
    connection: sqlite3.Connection,
    rows: Iterable[dict[str, Any]],
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            str(row.get("player_id") or ""),
            str(row.get("streak_key") or ""),
            _coerce_int(row.get("streak_length")),
            str(row.get("start_date") or ""),
            str(row.get("end_date") or ""),
            str(row.get("start_gid") or ""),
            str(row.get("end_gid") or ""),
            _coerce_int(row.get("first_season")),
            _coerce_int(row.get("last_season")),
        )
        for row in rows
        if row.get("player_id") and row.get("streak_key") and _coerce_int(row.get("streak_length")) > 0
    ]
    if not normalized_rows:
        return 0
    connection.executemany(
        """
        INSERT INTO retrosheet_player_streak_records (
            player_id,
            streak_key,
            streak_length,
            start_date,
            end_date,
            start_gid,
            end_gid,
            first_season,
            last_season
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_id, streak_key) DO UPDATE SET
            streak_length = excluded.streak_length,
            start_date = excluded.start_date,
            end_date = excluded.end_date,
            start_gid = excluded.start_gid,
            end_gid = excluded.end_gid,
            first_season = excluded.first_season,
            last_season = excluded.last_season,
            imported_at = CURRENT_TIMESTAMP
        """,
        normalized_rows,
    )
    connection.commit()
    return len(normalized_rows)


def replace_fielding_bible_player_drs(
    connection: sqlite3.Connection,
    *,
    season: int,
    rows: Iterable[dict[str, Any]],
    snapshot_at: str = "",
    source_name: str = "Fielding Bible / SIS DRS",
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            int(row.get("season") or season),
            snapshot_at,
            source_name,
            str(row.get("player") or "").strip(),
            _coerce_int(row.get("playerId")),
            _coerce_int(row.get("teamId")),
            _coerce_int(row.get("pos")),
            str(row.get("posAbbr") or "").strip(),
            _coerce_float(row.get("g")),
            _coerce_float(row.get("inn")),
            _coerce_float(row.get("total")),
            _coerce_float(row.get("art")),
            _coerce_float(row.get("gfpdm")),
            _coerce_float(row.get("gdp")),
            _coerce_float(row.get("bunt")),
            _coerce_float(row.get("ofArm")),
            _coerce_float(row.get("sb")),
            _coerce_float(row.get("sz")),
            _coerce_float(row.get("adjER")),
        )
        for row in rows
        if str(row.get("player") or "").strip()
    ]
    connection.execute(
        "DELETE FROM fielding_bible_player_drs WHERE season = ? AND snapshot_at = ?",
        (season, snapshot_at),
    )
    if normalized_rows:
        connection.executemany(
            """
            INSERT INTO fielding_bible_player_drs (
                season,
                snapshot_at,
                source_name,
                player,
                player_id,
                team_id,
                pos,
                pos_abbr,
                games,
                innings,
                total,
                art,
                gfpdm,
                gdp,
                bunt,
                of_arm,
                sb,
                sz,
                adj_er
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            normalized_rows,
        )
    connection.commit()
    return len(normalized_rows)


def replace_fielding_bible_team_drs(
    connection: sqlite3.Connection,
    *,
    season: int,
    rows: Iterable[dict[str, Any]],
    snapshot_at: str = "",
    source_name: str = "Fielding Bible / SIS Team DRS",
) -> int:
    initialize_database(connection)
    normalized_rows = [
        (
            int(row.get("season") or season),
            snapshot_at,
            source_name,
            _coerce_int(row.get("teamId")),
            str(row.get("nickname") or "").strip(),
            _coerce_int(row.get("rank")),
            _coerce_float(row.get("g")),
            _coerce_float(row.get("p")),
            _coerce_float(row.get("c")),
            _coerce_float(row.get("firstBase")),
            _coerce_float(row.get("secondBase")),
            _coerce_float(row.get("thirdBase")),
            _coerce_float(row.get("ss")),
            _coerce_float(row.get("lf")),
            _coerce_float(row.get("cf")),
            _coerce_float(row.get("rf")),
            _coerce_float(row.get("outfieldPositioningRunsSaved")),
            _coerce_float(row.get("nonShift")),
            _coerce_float(row.get("shifts")),
            _coerce_float(row.get("total")),
        )
        for row in rows
        if str(row.get("nickname") or "").strip()
    ]
    connection.execute(
        "DELETE FROM fielding_bible_team_drs WHERE season = ? AND snapshot_at = ?",
        (season, snapshot_at),
    )
    if normalized_rows:
        connection.executemany(
            """
            INSERT INTO fielding_bible_team_drs (
                season,
                snapshot_at,
                source_name,
                team_id,
                nickname,
                rank,
                games,
                pitcher,
                catcher,
                first_base,
                second_base,
                third_base,
                shortstop,
                left_field,
                center_field,
                right_field,
                outfield_positioning_runs_saved,
                non_shift,
                shifts,
                total
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            normalized_rows,
        )
    connection.commit()
    return len(normalized_rows)


def latest_snapshot_at(connection: sqlite3.Connection, table_name: str, *, season: int | None = None) -> str | None:
    if not table_exists(connection, table_name):
        return None
    if season is None:
        row = connection.execute(
            f"SELECT MAX(snapshot_at) AS snapshot_at FROM {quote_identifier(table_name)} WHERE snapshot_at <> ''"
        ).fetchone()
    else:
        row = connection.execute(
            f"""
            SELECT MAX(snapshot_at) AS snapshot_at
            FROM {quote_identifier(table_name)}
            WHERE snapshot_at <> '' AND season = ?
            """,
            (season,),
        ).fetchone()
    snapshot_at = row["snapshot_at"] if row is not None else None
    return str(snapshot_at) if snapshot_at else None


def _coerce_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
