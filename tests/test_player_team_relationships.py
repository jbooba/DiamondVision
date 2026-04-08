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
    con.execute(
        """
        CREATE TABLE lahman_pitching (
            playerid TEXT,
            yearid TEXT,
            teamid TEXT,
            g TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE lahman_fielding (
            playerid TEXT,
            yearid TEXT,
            teamid TEXT,
            pos TEXT,
            g TEXT
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
    con.executemany(
        """
        INSERT INTO lahman_pitching(playerid, yearid, teamid, g)
        VALUES (?, ?, ?, ?)
        """,
        [
            ("jacksed01", "2003", "LAN", "8"),
            ("jacksed01", "2004", "CHN", "31"),
            ("jacksed01", "2005", "TBA", "35"),
            ("jacksed01", "2006", "HOU", "33"),
            ("jacksed01", "2007", "TBA", "32"),
            ("jacksed01", "2008", "DET", "31"),
            ("jacksed01", "2009", "DET", "33"),
            ("jacksed01", "2010", "ARI", "34"),
            ("jacksed01", "2011", "CHA", "12"),
            ("jacksed01", "2011", "SLN", "19"),
            ("jacksed01", "2012", "WAS", "31"),
            ("jacksed01", "2013", "CHN", "31"),
            ("jacksed01", "2014", "MIA", "32"),
            ("jacksed01", "2015", "SDN", "30"),
            ("jacksed01", "2016", "MIA", "33"),
            ("jacksed01", "2017", "BAL", "8"),
            ("jacksed01", "2018", "OAK", "17"),
            ("jacksed01", "2019", "TOR", "28"),
            ("jacksed01", "2019", "DET", "4"),
        ],
    )
    con.executemany(
        """
        INSERT INTO lahman_fielding(playerid, yearid, teamid, pos, g)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ("mcgrifr01", "1993", "ATL", "1B", "148"),
            ("mcgrifr01", "2004", "LAN", "1B", "109"),
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


def test_game_appearances_for_most_teams_resolves_from_team_span_warehouse() -> None:
    con = build_test_connection()
    researcher = PlayerTeamRelationshipResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "which player has appeared in a game for the most teams")
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "player_team_span_leaderboard"
    assert snippet.payload["metric"] == "Games"
    assert snippet.payload["rows"][0]["player_name"] == "Edwin Jackson"
    assert snippet.payload["rows"][0]["team_count"] == 14
    con.close()
