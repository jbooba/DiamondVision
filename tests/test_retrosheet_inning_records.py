from __future__ import annotations

import sqlite3
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.retrosheet_inning_records import RetrosheetInningRecordResearcher
from mlb_history_bot.storage import initialize_database


TEST_SETTINGS = Settings.from_env(Path(__file__).resolve().parents[1])


def build_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    connection.execute(
        """
        CREATE TABLE retrosheet_teamstats (
            gid TEXT,
            team TEXT,
            stattype TEXT,
            gametype TEXT,
            inn1 TEXT,
            inn2 TEXT,
            inn3 TEXT,
            inn4 TEXT,
            inn5 TEXT,
            inn6 TEXT,
            inn7 TEXT,
            inn8 TEXT,
            inn9 TEXT,
            inn10 TEXT,
            inn11 TEXT,
            inn12 TEXT,
            inn13 TEXT,
            inn14 TEXT,
            inn15 TEXT,
            inn16 TEXT,
            inn17 TEXT,
            inn18 TEXT,
            inn19 TEXT,
            inn20 TEXT,
            inn21 TEXT,
            inn22 TEXT,
            inn23 TEXT,
            inn24 TEXT,
            inn25 TEXT,
            inn26 TEXT,
            inn27 TEXT,
            inn28 TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE retrosheet_gameinfo (
            gid TEXT,
            date TEXT,
            season TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE lahman_teams (
            yearid TEXT,
            teamid TEXT,
            name TEXT
        )
        """
    )
    connection.executemany(
        "INSERT INTO lahman_teams(yearid, teamid, name) VALUES (?, ?, ?)",
        [
            ("2024", "BOS", "Boston Red Sox"),
            ("2024", "NYY", "New York Yankees"),
            ("2025", "MIA", "Miami Marlins"),
            ("2025", "ATL", "Atlanta Braves"),
        ],
    )
    connection.executemany(
        "INSERT INTO retrosheet_gameinfo(gid, date, season) VALUES (?, ?, ?)",
        [
            ("g1", "20240401", "2024"),
            ("g2", "20250402", "2025"),
        ],
    )
    zeroes = tuple("0" for _ in range(27))
    connection.executemany(
        """
        INSERT INTO retrosheet_teamstats(
            gid, team, stattype, gametype,
            inn1, inn2, inn3, inn4, inn5, inn6, inn7, inn8, inn9, inn10, inn11, inn12, inn13, inn14,
            inn15, inn16, inn17, inn18, inn19, inn20, inn21, inn22, inn23, inn24, inn25, inn26, inn27, inn28
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("g1", "BOS", "value", "regular", "11", *zeroes),
            ("g1", "NYY", "value", "regular", "0", *zeroes),
            ("g2", "MIA", "value", "regular", "0", "2", *tuple("0" for _ in range(26))),
            ("g2", "ATL", "value", "regular", "3", "0", *tuple("0" for _ in range(26))),
        ],
    )
    connection.commit()
    return connection


def test_inning_record_researcher_answers_runs_allowed_query() -> None:
    connection = build_connection()
    researcher = RetrosheetInningRecordResearcher(TEST_SETTINGS)
    try:
        snippet = researcher.build_snippet(connection, "most runs given up in a single inning")
    finally:
        connection.close()
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "inning_record_leaderboard"
    assert snippet.payload["rows"][0]["team_name"] == "New York Yankees"
    assert snippet.payload["rows"][0]["metric_value"] == 11
    assert snippet.payload["rows"][0]["inning"] == 1


def test_inning_record_researcher_answers_runs_scored_query() -> None:
    connection = build_connection()
    researcher = RetrosheetInningRecordResearcher(TEST_SETTINGS)
    try:
        snippet = researcher.build_snippet(connection, "most runs scored in a single inning")
    finally:
        connection.close()
    assert snippet is not None
    assert snippet.payload["rows"][0]["team_name"] == "Boston Red Sox"
    assert snippet.payload["rows"][0]["metric_value"] == 11
