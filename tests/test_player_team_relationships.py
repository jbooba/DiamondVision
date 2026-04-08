from __future__ import annotations

from pathlib import Path
import sqlite3

from mlb_history_bot.config import Settings
from mlb_history_bot.player_team_relationships import PlayerTeamRelationshipResearcher
from mlb_history_bot.storage import initialize_database


TEST_SETTINGS = Settings.from_env(Path(__file__).resolve().parents[1])


def build_test_connection() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    initialize_database(con)
    con.execute(
        """
        CREATE TABLE lahman_people (
            playerid TEXT PRIMARY KEY,
            namefirst TEXT,
            namelast TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE lahman_batting (
            playerid TEXT,
            yearid TEXT,
            teamid TEXT,
            hr TEXT,
            h TEXT,
            rbi TEXT,
            bb TEXT,
            so TEXT,
            c_2b TEXT,
            c_3b TEXT
        )
        """
    )
    con.executemany(
        "INSERT INTO lahman_people(playerid, namefirst, namelast) VALUES (?, ?, ?)",
        [
            ("mcgrifr01", "Fred", "McGriff"),
            ("jacksed01", "Edwin", "Jackson"),
            ("alonspe01", "Pete", "Alonso"),
        ],
    )
    con.executemany(
        """
        INSERT INTO lahman_batting(playerid, yearid, teamid, hr, h, rbi, bb, so, c_2b, c_3b)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("mcgrifr01", "1987", "TOR", "20", "140", "64", "53", "97", "21", "0"),
            ("mcgrifr01", "1993", "ATL", "36", "165", "104", "91", "106", "37", "1"),
            ("mcgrifr01", "1997", "TBA", "19", "135", "81", "73", "113", "30", "0"),
            ("mcgrifr01", "2001", "CHN", "31", "146", "88", "93", "118", "24", "0"),
            ("mcgrifr01", "2004", "LAN", "16", "108", "64", "63", "70", "18", "1"),
            ("mcgrifr01", "1986", "SDN", "3", "23", "9", "10", "10", "2", "0"),
            ("jacksed01", "2011", "CHA", "0", "0", "0", "0", "0", "0", "0"),
            ("jacksed01", "2012", "SLN", "0", "0", "0", "0", "0", "0", "0"),
            ("alonspe01", "2019", "NYN", "53", "155", "120", "72", "183", "30", "2"),
        ],
    )
    con.commit()
    return con


def test_home_runs_for_most_teams_resolves_from_historical_team_spans() -> None:
    con = build_test_connection()
    researcher = PlayerTeamRelationshipResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "who has hit a home run for the most teams")
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "player_team_span_leaderboard"
    assert snippet.payload["rows"][0]["player_name"] == "Fred McGriff"
    assert snippet.payload["rows"][0]["team_count"] == 6
    con.close()


def test_home_runs_for_most_teams_supports_minimum_total_filter() -> None:
    con = build_test_connection()
    researcher = PlayerTeamRelationshipResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "who has hit a home run for the most teams with at least 50 home runs")
    assert snippet is not None
    assert snippet.payload["rows"][0]["player_name"] == "Fred McGriff"
    con.close()
