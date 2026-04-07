import sqlite3
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.special_leaderboards import SpecialLeaderboardResearcher


def test_birthday_home_run_snippet() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
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
            id TEXT,
            date TEXT,
            gametype TEXT,
            hr TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE retrosheet_allplayers (
            id TEXT,
            first TEXT,
            last TEXT
        )
        """
    )
    connection.executemany(
        "INSERT INTO lahman_people (retroid, namefirst, namelast, birthmonth, birthday) VALUES (?, ?, ?, ?, ?)",
        [
            ("slug001", "Slugger", "One", "4", "6"),
            ("slug002", "Slugger", "Two", "4", "6"),
        ],
    )
    connection.executemany(
        "INSERT INTO retrosheet_batting (id, date, gametype, hr) VALUES (?, ?, ?, ?)",
        [
            ("slug001", "20240406", "R", "2"),
            ("slug001", "20250406", "R", "1"),
            ("slug002", "20240406", "R", "1"),
            ("slug002", "20250406", "R", "1"),
        ],
    )
    settings = Settings.from_env()
    researcher = SpecialLeaderboardResearcher(settings)
    snippet = researcher.build_snippet(connection, "Which player has hit the most home runs on their birthday?")
    connection.close()
    assert snippet is not None
    assert snippet.payload["leaders"][0]["player_name"] == "Slugger One"
    assert snippet.payload["leaders"][0]["total"] == 3
