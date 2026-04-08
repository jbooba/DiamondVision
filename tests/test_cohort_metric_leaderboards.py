from __future__ import annotations

import sqlite3
from pathlib import Path

from mlb_history_bot.cohort_metric_leaderboards import CohortMetricLeaderboardResearcher
from mlb_history_bot.config import Settings
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
            retroid TEXT,
            namefirst TEXT,
            namelast TEXT,
            birthcountry TEXT,
            bats TEXT,
            throws TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE lahman_halloffame (
            playerid TEXT,
            inducted TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE lahman_managers (
            playerid TEXT,
            yearid TEXT,
            teamid TEXT,
            w TEXT,
            l TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE lahman_teams (
            yearid TEXT,
            teamid TEXT,
            name TEXT,
            teamidbr TEXT,
            teamidretro TEXT,
            franchid TEXT
        )
        """
    )
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
        "INSERT INTO lahman_people(playerid, retroid, namefirst, namelast, birthcountry, bats, throws) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("buck01", "showb001", "Buck", "Showalter", "USA", "R", "R"),
            ("alonsp01", "alonp001", "Pete", "Alonso", "USA", "R", "R"),
            ("nimmbr01", "nimmb001", "Brandon", "Nimmo", "USA", "L", "R"),
            ("devers01", "dever001", "Rafael", "Devers", "Dominican Republic", "L", "R"),
            ("sotoju01", "sotoj001", "Juan", "Soto", "Dominican Republic", "L", "L"),
        ],
    )
    con.executemany(
        "INSERT INTO lahman_halloffame(playerid, inducted) VALUES (?, ?)",
        [
            ("alonsp01", "N"),
            ("sotoju01", "N"),
            ("devers01", "Y"),
        ],
    )
    con.executemany(
        "INSERT INTO lahman_managers(playerid, yearid, teamid, w, l) VALUES (?, ?, ?, ?, ?)",
        [
            ("buck01", "2022", "NYN", "101", "61"),
            ("buck01", "2023", "NYN", "75", "87"),
        ],
    )
    con.executemany(
        "INSERT INTO lahman_teams(yearid, teamid, name, teamidbr, teamidretro, franchid) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("2022", "NYN", "New York Mets", "NYM", "NYN", "NYM"),
            ("2023", "NYN", "New York Mets", "NYM", "NYN", "NYM"),
            ("2024", "BOS", "Boston Red Sox", "BOS", "BOS", "BOS"),
            ("2024", "NYY", "New York Yankees", "NYY", "NYA", "NYY"),
        ],
    )
    con.executemany(
        """
        INSERT INTO lahman_batting(
            playerid, yearid, teamid, g, ab, r, h, c_2b, c_3b, hr, rbi, sb, cs, bb, so, hbp, sh, sf
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("alonsp01", "2022", "NYN", "160", "597", "95", "162", "27", "0", "40", "131", "1", "0", "67", "126", "4", "0", "3"),
            ("alonsp01", "2023", "NYN", "154", "568", "92", "123", "21", "0", "46", "118", "0", "0", "66", "151", "3", "0", "6"),
            ("nimmbr01", "2022", "NYN", "151", "580", "102", "159", "30", "3", "16", "64", "3", "0", "60", "148", "4", "0", "4"),
            ("nimmbr01", "2023", "NYN", "152", "560", "102", "154", "25", "4", "24", "68", "3", "0", "72", "166", "2", "0", "5"),
            ("devers01", "2024", "BOS", "138", "516", "87", "140", "34", "0", "28", "83", "3", "0", "67", "166", "6", "0", "5"),
            ("sotoju01", "2024", "NYY", "157", "576", "128", "166", "31", "4", "41", "109", "7", "2", "129", "119", "3", "0", "5"),
        ],
    )
    con.commit()
    return con


def test_manager_era_best_hitter_uses_shared_cohort_layer() -> None:
    con = build_test_connection()
    researcher = CohortMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "who was the best hitter for the Mets under Buck Showalter?")
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "cohort_metric_leaderboard"
    assert snippet.payload["rows"][0]["player_name"] == "Pete Alonso"
    con.close()


def test_manager_era_worst_hitter_by_average() -> None:
    con = build_test_connection()
    researcher = CohortMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "who was the worst hitter for the Mets under Buck Showalter by batting average?")
    assert snippet is not None
    assert snippet.payload["rows"][0]["player_name"] == "Pete Alonso"
    con.close()


def test_birth_country_highest_ops_by_season() -> None:
    con = build_test_connection()
    researcher = CohortMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "which Dominican-born player had the highest OPS in 2024?")
    assert snippet is not None
    assert snippet.payload["cohort_kind"] == "birth_country"
    assert snippet.payload["rows"][0]["player_name"] == "Juan Soto"
    con.close()


def test_left_handed_hitter_cohort_resolves() -> None:
    con = build_test_connection()
    researcher = CohortMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "which left-handed hitter had the highest OPS in 2024?")
    assert snippet is not None
    assert snippet.payload["cohort_kind"] == "bat_handedness"
    assert snippet.payload["rows"][0]["player_name"] == "Juan Soto"
    con.close()


def test_hall_of_fame_cohort_resolves() -> None:
    con = build_test_connection()
    researcher = CohortMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "which Hall of Famer had the highest OPS in 2024?")
    assert snippet is not None
    assert snippet.payload["cohort_kind"] == "hall_of_fame"
    assert snippet.payload["rows"][0]["player_name"] == "Rafael Devers"
    con.close()
