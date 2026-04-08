from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import patch

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
            ("ramirjo01", "ramij001", "Jose", "Ramirez", "Dominican Republic", "B", "R"),
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
            ("ramirjo01", "2021", "CLE", "152", "552", "111", "147", "32", "5", "36", "103", "27", "7", "72", "83", "7", "0", "4"),
        ],
    )
    con.executemany(
        """
        INSERT INTO statcast_batter_games(
            season, game_date, game_pk, batter_id, batter_name, team, team_name, opponent, opponent_name,
            plate_appearances, at_bats, hits, singles, doubles, triples, home_runs, walks, strikeouts,
            runs_batted_in, batted_ball_events, xba_numerator, xwoba_numerator, xwoba_denom, xslg_numerator,
            hard_hit_bbe, barrel_bbe, launch_speed_sum, launch_speed_count, max_launch_speed, avg_bat_speed,
            max_bat_speed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (2021, "2021-07-01", 10, 608070, "Jose Ramirez", "CLE", "Cleveland Guardians", "DET", "Detroit Tigers", 40, 36, 12, 8, 2, 0, 2, 4, 8, 9, 24, 9.1, 13.0, 36.0, 16.0, 10, 3, 2290.0, 24, 111.4, 73.8, 76.2),
            (2021, "2021-07-01", 11, 665742, "Juan Soto", "WSN", "Washington Nationals", "ATL", "Atlanta Braves", 42, 35, 11, 7, 2, 0, 2, 7, 6, 8, 23, 8.4, 13.4, 35.0, 15.2, 8, 2, 2208.0, 23, 110.7, 72.0, 73.5),
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


def test_switch_hitter_statcast_cohort_resolves_by_average_exit_velocity() -> None:
    con = build_test_connection()
    researcher = CohortMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "highest average exit velocity by a switch hitter in 2021")
    assert snippet is not None
    assert snippet.payload["source_family"] == "statcast"
    assert snippet.payload["cohort_kind"] == "bat_handedness"
    assert snippet.payload["rows"][0]["player_name"] == "Jose Ramirez"
    con.close()


def test_switch_hitter_statcast_cohort_falls_back_to_statcast_events() -> None:
    con = build_test_connection()
    con.execute("DELETE FROM statcast_batter_games")
    jose_rows = [
        (
            2021,
            "2021-07-01",
            100,
            at_bat_number,
            3,
            608070,
            "Jose Ramirez",
            1,
            "Pitcher A",
            "CLE",
            "DET",
            "CLE",
            "DET",
            "S",
            "R",
            "FF",
            "4-Seam Fastball",
            "fastball",
            "single" if at_bat_number % 2 else "home_run",
            1,
            1,
            1 if at_bat_number % 2 == 0 else 0,
            1 if at_bat_number % 2 == 0 else 0,
            0,
            at_bat_number % 3,
            1,
            1,
            "1-1",
            1,
            1 if at_bat_number % 2 == 0 else 0,
            "middle",
            "middle",
            "center",
            95.0,
            2300.0,
            108.0 + (at_bat_number % 2),
            18.0,
            402.0,
            74.0,
            0.88,
            0.91,
            1.20,
        )
        for at_bat_number in range(1, 11)
    ]
    soto_rows = [
        (
            2021,
            "2021-07-02",
            101,
            at_bat_number,
            2,
            665742,
            "Juan Soto",
            2,
            "Pitcher B",
            "WSN",
            "ATL",
            "WSN",
            "ATL",
            "L",
            "R",
            "FF",
            "4-Seam Fastball",
            "fastball",
            "single",
            1,
            1,
            0,
            0,
            0,
            0,
            0,
            1,
            "0-1",
            0,
            0,
            "middle",
            "middle",
            "center",
            94.1,
            2250.0,
            101.0,
            14.0,
            370.0,
            72.0,
            0.80,
            0.82,
            1.05,
        )
        for at_bat_number in range(1, 11)
    ]
    con.executemany(
        """
        INSERT INTO statcast_events(
            season, game_date, game_pk, at_bat_number, pitch_number, batter_id, batter_name, pitcher_id, pitcher_name,
            batting_team, pitching_team, home_team, away_team, stand, p_throws, pitch_type, pitch_name, pitch_family,
            event, is_ab, is_hit, is_xbh, is_home_run, is_strikeout, has_risp, balls, strikes, count_key,
            outs_when_up, runs_batted_in, horizontal_location, vertical_location, field_direction, release_speed,
            release_spin_rate, launch_speed, launch_angle, hit_distance, bat_speed, estimated_ba, estimated_woba,
            estimated_slg
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        jose_rows + soto_rows,
    )
    con.commit()
    researcher = CohortMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "highest average exit velocity by a switch hitter in 2021")
    assert snippet is not None
    assert snippet.payload["rows"][0]["player_name"] == "Jose Ramirez"
    con.close()


def test_all_star_cohort_uses_dynamic_player_set_for_career_queries() -> None:
    con = build_test_connection()
    researcher = CohortMetricLeaderboardResearcher(TEST_SETTINGS)
    with patch("mlb_history_bot.cohort_timeline.load_all_star_full", return_value=[]), patch(
        "mlb_history_bot.cohort_timeline.load_all_star_game_logs",
        return_value=[
            {"home_1_name": "Pete Alonso", "visiting_1_name": "Jose Ramirez", "home_manager_name": "Buck Showalter"},
        ],
    ):
        snippet = researcher.build_snippet(con, "what is the lowest career BA of a player who made an all star team")
    assert snippet is not None
    assert snippet.payload["cohort_kind"] == "all_star"
    assert snippet.payload["rows"][0]["player_name"] == "Pete Alonso"
    con.close()
