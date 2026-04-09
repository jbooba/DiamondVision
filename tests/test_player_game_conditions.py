from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import Mock, patch

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
    connection.execute(
        """
        CREATE TABLE retrosheet_pitching (
            gid TEXT,
            id TEXT,
            team TEXT,
            p_seq TEXT,
            stattype TEXT,
            p_ipouts TEXT,
            p_h TEXT,
            p_hr TEXT,
            p_r TEXT,
            p_er TEXT,
            p_w TEXT,
            p_iw TEXT,
            p_k TEXT,
            p_hbp TEXT,
            wp TEXT,
            lp TEXT,
            save TEXT,
            p_gs TEXT,
            date TEXT,
            gametype TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE retrosheet_allplayers (
            id TEXT,
            last TEXT,
            first TEXT
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
        "INSERT INTO retrosheet_allplayers (id, last, first) VALUES (?, ?, ?)",
        [
            ("alpha001", "Slugger", "Alpha"),
            ("beta001", "Slugger", "Beta"),
            ("gamma001", "Slugger", "Gamma"),
            ("delta001", "Slugger", "Delta"),
            ("ace001", "Ace", "Monday"),
            ("ace002", "Ace", "Tuesday"),
            ("ace003", "Ace", "Wednesday"),
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
    connection.executemany(
        """
        INSERT INTO retrosheet_pitching (
            gid, id, team, p_seq, stattype, p_ipouts, p_h, p_hr, p_r, p_er, p_w, p_iw, p_k, p_hbp, wp, lp, save, p_gs, date, gametype
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("p1", "ace001", "NYA", "1", "value", "27", "5", "0", "1", "1", "2", "0", "8", "0", "1", "", "", "1", "20210705", "regular"),
            ("p2", "ace001", "NYA", "1", "value", "24", "6", "1", "2", "2", "3", "0", "7", "0", "1", "", "", "1", "20220704", "regular"),
            ("p3", "ace001", "NYA", "1", "value", "18", "7", "1", "3", "3", "4", "0", "5", "0", "", "1", "", "1", "20210706", "regular"),
            ("p4", "ace002", "BOS", "1", "value", "27", "4", "0", "1", "1", "1", "0", "9", "0", "1", "", "", "1", "20210706", "regular"),
            ("p5", "ace002", "BOS", "1", "value", "27", "5", "0", "2", "2", "2", "0", "6", "0", "1", "", "", "1", "20220705", "regular"),
            ("p6", "ace003", "ATL", "1", "value", "21", "6", "0", "2", "2", "3", "0", "4", "0", "1", "", "", "1", "20210707", "regular"),
        ],
    )
    connection.commit()
    return connection


def test_birthday_condition_ops_leaderboard() -> None:
    connection = build_connection()
    researcher = PlayerGameConditionResearcher(TEST_SETTINGS)
    with patch("mlb_history_bot.player_game_conditions.is_supported_birthday_index_query", return_value=False):
        snippet = researcher.build_snippet(connection, "which hitter has the highest OPS when playing on their birthday")
    connection.close()
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "player_game_condition_leaderboard"
    assert snippet.payload["rows"][0]["player_name"] == "Gamma Slugger"
    assert "games played on a player's birthday" in snippet.summary


def test_birthday_condition_home_run_leaderboard_supports_specific_season() -> None:
    connection = build_connection()
    researcher = PlayerGameConditionResearcher(TEST_SETTINGS)
    with patch("mlb_history_bot.player_game_conditions.is_supported_birthday_index_query", return_value=False):
        snippet = researcher.build_snippet(connection, "who hit the most home runs on their birthday in 2021")
    connection.close()
    assert snippet is not None
    assert snippet.payload["rows"][0]["player_name"] == "Beta Slugger"
    assert snippet.payload["rows"][0]["home_runs"] == 1


def test_birthday_condition_payload_marks_full_leaderboard_metadata() -> None:
    connection = build_connection()
    researcher = PlayerGameConditionResearcher(TEST_SETTINGS)
    with patch("mlb_history_bot.player_game_conditions.is_supported_birthday_index_query", return_value=False):
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
    with patch("mlb_history_bot.player_game_conditions.is_supported_birthday_index_query", return_value=False):
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


def test_weekday_pitching_condition_breakdown_returns_top_pitcher_per_day() -> None:
    connection = build_connection()
    researcher = PlayerGameConditionResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(connection, "Which pitcher has the most wins on each day of the week")
    connection.close()
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "player_game_condition_leaderboard"
    assert snippet.payload["role"] == "pitcher"
    assert snippet.payload["breakdown_all_values"] is True
    assert snippet.payload["rows"][0]["condition_value"] == "Monday"
    assert snippet.payload["rows"][0]["player_name"] == "Monday Ace"
    assert snippet.payload["rows"][0]["wins"] == 2
    assert any(row["condition_value"] == "Tuesday" and row["player_name"] == "Tuesday Ace" for row in snippet.payload["rows"])


def test_weekday_pitching_condition_supports_specific_weekday() -> None:
    connection = build_connection()
    researcher = PlayerGameConditionResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(connection, "Which pitcher has the most wins on Tuesdays?")
    connection.close()
    assert snippet is not None
    assert snippet.payload["role"] == "pitcher"
    assert snippet.payload["breakdown_all_values"] is False
    assert snippet.payload["rows"][0]["player_name"] == "Tuesday Ace"
    assert snippet.payload["rows"][0]["wins"] == 2


def test_birthday_condition_can_use_savant_birthday_index_provider() -> None:
    connection = build_connection()
    researcher = PlayerGameConditionResearcher(TEST_SETTINGS)
    html = """
    <html><body><script>
    const serverParams = {"minGames":"10"};
    const birthdayData = [
      {
        "player_name":"Frank Thomas",
        "birthday_games":14,
        "birthday_pa":60,
        "birthday_BA":0.468,
        "birthday_OPS":1.349,
        "birthday_wOBA":0.566,
        "birthday_hits":22,
        "birthday_hit_hr":2,
        "birthday_strikeout":11,
        "birthday_walk":13,
        "actual_birthday":"1968-05-27T00:00:00.000Z"
      },
      {
        "player_name":"Someone Else",
        "birthday_games":10,
        "birthday_pa":47,
        "birthday_BA":0.308,
        "birthday_OPS":1.093,
        "birthday_wOBA":0.438,
        "birthday_hits":12,
        "birthday_hit_hr":4,
        "birthday_strikeout":4,
        "birthday_walk":8,
        "actual_birthday":"1992-09-17T00:00:00.000Z"
      }
    ];
    </script></body></html>
    """
    response = Mock()
    response.text = html
    response.raise_for_status = Mock()
    with patch("mlb_history_bot.birthday_index.requests.get", return_value=response):
        snippet = researcher.build_snippet(
            connection,
            "which hitter has the highest OPS when playing on their birthday with at least 10 games",
        )
    connection.close()
    assert snippet is not None
    assert snippet.source == "Baseball Savant Birthday Index"
    assert snippet.payload["rows"][0]["player_name"] == "Frank Thomas"
    assert snippet.payload["rows"][0]["condition_games"] == 14
    assert snippet.payload["rows"][0]["ops"] == 1.349
