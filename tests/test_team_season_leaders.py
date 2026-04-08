from __future__ import annotations

import sqlite3
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.storage import initialize_database
from mlb_history_bot.team_season_leaders import TeamSeasonLeaderResearcher


TEST_SETTINGS = Settings.from_env(Path(__file__).resolve().parents[1])


def build_test_connection() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    initialize_database(con)
    con.execute("CREATE TABLE lahman_people (playerid TEXT PRIMARY KEY, namefirst TEXT, namelast TEXT)")
    con.execute("CREATE TABLE lahman_teams (yearid TEXT, name TEXT, teamid TEXT, teamidretro TEXT, franchid TEXT, w TEXT, l TEXT)")
    con.execute(
        """
        CREATE TABLE lahman_batting (
            playerid TEXT,
            yearid TEXT,
            teamid TEXT,
            g TEXT,
            ab TEXT,
            r TEXT,
            h TEXT,
            c_2b TEXT,
            c_3b TEXT,
            hr TEXT,
            rbi TEXT,
            sb TEXT,
            cs TEXT,
            bb TEXT,
            so TEXT,
            hbp TEXT,
            sh TEXT,
            sf TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE lahman_pitching (
            playerid TEXT,
            yearid TEXT,
            teamid TEXT,
            w TEXT,
            l TEXT,
            g TEXT,
            gs TEXT,
            sv TEXT,
            ipouts TEXT,
            h TEXT,
            er TEXT,
            hr TEXT,
            bb TEXT,
            so TEXT
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
            g TEXT,
            po TEXT,
            a TEXT,
            e TEXT,
            dp TEXT
        )
        """
    )
    con.executemany(
        "INSERT INTO lahman_people(playerid, namefirst, namelast) VALUES (?, ?, ?)",
        [
            ("alpha01", "Alex", "Alpha"),
            ("bravo01", "Ben", "Bravo"),
            ("pitch01", "Pete", "Pitch"),
            ("field01", "Fran", "Field"),
        ],
    )
    con.execute(
        "INSERT INTO lahman_teams(yearid, name, teamid, teamidretro, franchid, w, l) VALUES ('2024', 'Boston Red Sox', 'BOS', 'BOS', 'BOS', '81', '81')"
    )
    con.executemany(
        """
        INSERT INTO lahman_batting(
            playerid, yearid, teamid, g, ab, r, h, c_2b, c_3b, hr, rbi, sb, cs, bb, so, hbp, sh, sf
        ) VALUES (?, '2024', 'BOS', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("alpha01", "140", "500", "90", "160", "30", "2", "28", "95", "12", "2", "65", "110", "4", "0", "5"),
            ("bravo01", "120", "220", "20", "40", "8", "0", "2", "18", "1", "1", "15", "55", "0", "0", "2"),
        ],
    )
    con.executemany(
        """
        INSERT INTO lahman_pitching(
            playerid, yearid, teamid, w, l, g, gs, sv, ipouts, h, er, hr, bb, so
        ) VALUES (?, '2024', 'BOS', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("pitch01", "12", "8", "30", "30", "0", "540", "150", "55", "18", "45", "180"),
            ("bravo01", "2", "4", "20", "0", "2", "120", "70", "40", "10", "25", "60"),
        ],
    )
    con.executemany(
        """
        INSERT INTO lahman_fielding(
            playerid, yearid, teamid, pos, g, po, a, e, dp
        ) VALUES (?, '2024', 'BOS', ?, ?, ?, ?, ?, ?)
        """,
        [
            ("field01", "SS", "100", "110", "240", "8", "50"),
            ("bravo01", "1B", "75", "620", "40", "2", "60"),
        ],
    )
    con.commit()
    return con


def test_historical_team_season_leader_builds_best_hitter_snippet() -> None:
    con = build_test_connection()
    researcher = TeamSeasonLeaderResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "who was the best hitter on the red sox in 2024?")
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "team_season_leaderboard"
    assert snippet.payload["mode"] == "historical"
    assert snippet.payload["rows"][0]["player_name"] == "Alex Alpha"
    con.close()


def test_historical_team_season_leader_builds_worst_hitter_snippet() -> None:
    con = build_test_connection()
    researcher = TeamSeasonLeaderResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "who was the worst hitter on the red sox in 2024?")
    assert snippet is not None
    assert snippet.payload["rows"][0]["player_name"] == "Ben Bravo"
    con.close()


def test_historical_team_season_leader_supports_pitching_metric() -> None:
    con = build_test_connection()
    researcher = TeamSeasonLeaderResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "which pitcher had the highest strikeouts on the red sox in 2024?")
    assert snippet is not None
    assert snippet.payload["rows"][0]["player_name"] == "Pete Pitch"
    con.close()
