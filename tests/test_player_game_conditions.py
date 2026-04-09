from __future__ import annotations

import sqlite3
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.player_game_conditions import PlayerGameConditionResearcher
from mlb_history_bot.storage import initialize_database


TEST_SETTINGS = Settings.from_env(Path(__file__).resolve().parents[1])


def build_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    connection.execute(
        """
        CREATE TABLE lahman_people (
            retroid TEXT,
            namefirst TEXT,
            namelast TEXT,
            birthmonth TEXT,
            birthday TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE retrosheet_batting (
            gid TEXT,
            id TEXT,
            stattype TEXT,
            b_pa TEXT,
            b_ab TEXT,
            b_r TEXT,
            b_h TEXT,
            b_d TEXT,
            b_t TEXT,
            b_hr TEXT,
            b_rbi TEXT,
            b_sh TEXT,
            b_sf TEXT,
            b_hbp TEXT,
            b_w TEXT,
            b_k TEXT,
            b_sb TEXT,
            b_cs TEXT,
            date TEXT,
            gametype TEXT
        )
        """
    )
    connection.executemany(
        "INSERT INTO lahman_people (retroid, namefirst, namelast, birthmonth, birthday) VALUES (?, ?, ?, ?, ?)",
        [
            ("alpha001", "Alpha", "Slugger", "7", "4"),
            ("beta001", "Beta", "Slugger", "7", "4"),
            ("gamma001", "Gamma", "Slugger", "7", "5"),
            ("delta001", "Delta", "Slugger", "7", "4"),
        ],
    )
    connection.executemany(
        """
        INSERT INTO retrosheet_batting (
            gid, id, stattype, b_pa, b_ab, b_r, b_h, b_d, b_t, b_hr, b_rbi,
            b_sh, b_sf, b_hbp, b_w, b_k, b_sb, b_cs, date, gametype
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("g1", "alpha001", "value", "5", "4", "1", "2", "1", "0", "0", "1", "0", "0", "0", "1", "0", "0", "0", "20210704", "regular"),
            ("g2", "alpha001", "value", "4", "4", "2", "2", "0", "0", "1", "2", "0", "0", "0", "0", "1", "0", "0", "20220704", "regular"),
            ("g3", "beta001", "value", "4", "4", "1", "1", "0", "0", "1", "1", "0", "0", "0", "0", "1", "0", "0", "20210704", "regular"),
            ("g4", "beta001", "value", "5", "4", "0", "1", "0", "0", "0", "0", "0", "1", "0", "1", "2", "0", "0", "20220704", "regular"),
            ("g5", "gamma001", "value", "4", "4", "1", "3", "1", "0", "0", "1", "0", "0", "0", "0", "0", "0", "0", "20220705", "regular"),
            ("g6", "delta001", "value", "5", "4", "1", "2", "1", "0", "0", "1", "0", "0", "0", "1", "0", "0", "0", "20180704", "regular"),
            ("g7", "delta001", "value", "5", "4", "1", "2", "0", "0", "1", "2", "0", "0", "0", "1", "0", "0", "0", "20190704", "regular"),
            ("g8", "delta001", "value", "5", "4", "1", "2", "1", "0", "0", "1", "0", "0", "0", "1", "0", "0", "0", "20200704", "regular"),
            ("g9", "delta001", "value", "5", "4", "1", "2", "1", "0", "0", "1", "0", "0", "0", "1", "0", "0", "0", "20210704", "regular"),
            ("g10", "delta001", "value", "5", "4", "1", "2", "1", "0", "0", "1", "0", "0", "0", "1", "0", "0", "0", "20220704", "regular"),
        ],
    )
    connection.commit()
    return connection


def test_birthday_condition_ops_leaderboard() -> None:
    connection = build_connection()
    researcher = PlayerGameConditionResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(connection, "which hitter has the highest OPS when playing on their birthday")
    connection.close()
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "player_game_condition_leaderboard"
    assert snippet.payload["rows"][0]["player_name"] == "Gamma Slugger"
    assert "games played on a player's birthday" in snippet.summary


def test_birthday_condition_home_run_leaderboard_supports_specific_season() -> None:
    connection = build_connection()
    researcher = PlayerGameConditionResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(connection, "who hit the most home runs on their birthday in 2021")
    connection.close()
    assert snippet is not None
    assert snippet.payload["rows"][0]["player_name"] == "Beta Slugger"
    assert snippet.payload["rows"][0]["home_runs"] == 1


def test_birthday_condition_payload_marks_full_leaderboard_metadata() -> None:
    connection = build_connection()
    researcher = PlayerGameConditionResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(connection, "which hitter has the highest OPS when playing on their birthday")
    connection.close()
    assert snippet is not None
    assert snippet.payload["leaderboard_complete"] is True
    assert snippet.payload["displayed_row_count"] == len(snippet.payload["rows"])
    assert snippet.payload["total_row_count"] >= snippet.payload["displayed_row_count"]
    assert snippet.payload["max_plate_appearances"] == 25
    assert "top display slice" in snippet.payload["leaderboard_scope_note"]


def test_birthday_condition_respects_explicit_minimum_plate_appearances() -> None:
    connection = build_connection()
    researcher = PlayerGameConditionResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(
        connection,
        "which hitter has the highest OPS when playing on their birthday with a minimum of 20 PA",
    )
    connection.close()
    assert snippet is not None
    assert snippet.payload["minimum_value"] == 20
    assert snippet.payload["minimum_basis"] == "plate_appearances"
    assert snippet.payload["rows"][0]["player_name"] == "Delta Slugger"
    assert snippet.payload["rows"][0]["plate_appearances"] == 25
    assert "at least 20 PA" in snippet.summary
