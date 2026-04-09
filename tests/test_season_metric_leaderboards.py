from __future__ import annotations

import sqlite3
from pathlib import Path

from mlb_history_bot.config import Settings
import mlb_history_bot.season_metric_leaderboards as season_metric_leaderboards
from mlb_history_bot.season_metric_leaderboards import SeasonMetricLeaderboardResearcher
from mlb_history_bot.storage import initialize_database


TEST_SETTINGS = Settings.from_env(Path(__file__).resolve().parents[1])


def build_test_connection() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    initialize_database(con)
    con.execute("CREATE TABLE lahman_people (playerid TEXT PRIMARY KEY, namefirst TEXT, namelast TEXT)")
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
            so TEXT,
            hbp TEXT
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
    con.execute(
        """
        CREATE TABLE lahman_teams (
            yearid TEXT,
            teamid TEXT,
            franchid TEXT,
            name TEXT,
            g TEXT,
            w TEXT,
            l TEXT,
            r TEXT,
            ab TEXT,
            h TEXT,
            c_2b TEXT,
            c_3b TEXT,
            hr TEXT,
            bb TEXT,
            hbp TEXT,
            sf TEXT,
            ra TEXT,
            era TEXT,
            fp TEXT
        )
        """
    )
    con.executemany(
        "INSERT INTO lahman_people(playerid, namefirst, namelast) VALUES (?, ?, ?)",
        [
            ("alpha01", "Alex", "Alpha"),
            ("bravo01", "Ben", "Bravo"),
            ("pitch01", "Paula", "Pitcher"),
            ("pitch02", "Rita", "Rotation"),
            ("pitch03", "Ivan", "Nova"),
        ],
    )
    con.executemany(
        """
        INSERT INTO lahman_batting(
            playerid, yearid, teamid, g, ab, r, h, c_2b, c_3b, hr, rbi, sb, cs, bb, so, hbp, sh, sf
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("alpha01", "2024", "BOS", "140", "500", "90", "160", "30", "2", "28", "95", "12", "2", "65", "110", "4", "0", "5"),
            ("bravo01", "2024", "NYY", "120", "220", "20", "40", "8", "0", "2", "18", "1", "1", "15", "55", "0", "0", "2"),
            ("pitch03", "2024", "MIA", "25", "109", "1", "2", "0", "0", "0", "0", "0", "0", "0", "65", "0", "0", "0"),
        ],
    )
    con.executemany(
        """
        INSERT INTO lahman_pitching(
            playerid, yearid, teamid, w, l, g, gs, sv, ipouts, h, er, hr, bb, so, hbp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("pitch01", "2022", "BOS", "12", "8", "20", "20", "0", "540", "120", "40", "12", "25", "140", "4"),
            ("pitch01", "2023", "BOS", "13", "9", "24", "24", "0", "621", "135", "48", "15", "30", "155", "3"),
            ("pitch01", "2024", "BOS", "10", "7", "18", "18", "0", "510", "118", "36", "11", "22", "133", "2"),
            ("pitch01", "2025", "BOS", "12", "8", "20", "20", "0", "540", "120", "40", "12", "25", "140", "4"),
            ("pitch01", "2026", "BOS", "2", "1", "3", "3", "0", "81", "16", "7", "2", "5", "20", "1"),
            ("pitch02", "2022", "BOS", "9", "9", "18", "18", "0", "486", "116", "51", "17", "28", "118", "3"),
            ("pitch02", "2023", "BOS", "11", "10", "21", "21", "0", "552", "128", "57", "18", "36", "130", "4"),
            ("pitch02", "2024", "BOS", "10", "11", "17", "17", "0", "447", "110", "52", "19", "24", "99", "2"),
            ("pitch02", "2025", "BOS", "8", "10", "18", "18", "0", "495", "118", "53", "17", "30", "115", "3"),
            ("pitch02", "2026", "BOS", "1", "2", "3", "3", "0", "72", "19", "10", "4", "7", "15", "1"),
            ("pitch03", "2024", "MIA", "5", "12", "25", "25", "0", "600", "150", "70", "20", "35", "120", "2"),
        ],
    )
    con.executemany(
        """
        INSERT INTO lahman_teams(
            yearid, teamid, franchid, name, g, w, l, r, ab, h, c_2b, c_3b, hr, bb, hbp, sf, ra, era, fp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("1900", "SPR", "SPR", "Springfield Pioneers", "140", "75", "65", "500", "1000", "400", "80", "15", "30", "120", "6", "12", "460", "3.80", ".970"),
            ("1901", "SPR", "SPR", "Springfield Pilots", "140", "65", "75", "420", "1000", "200", "60", "10", "20", "110", "5", "10", "510", "4.20", ".968"),
            ("2024", "BOS", "BOS", "Boston Red Sox", "162", "81", "81", "751", "5500", "1450", "300", "25", "210", "550", "60", "45", "720", "4.10", ".985"),
            ("2024", "NYY", "NYY", "New York Yankees", "162", "90", "72", "720", "5525", "1400", "250", "20", "180", "500", "40", "40", "650", "3.85", ".983"),
            ("2023", "MIA", "MIA", "Miami Marlins", "162", "84", "78", "666", "5450", "1360", "260", "18", "170", "480", "42", "38", "723", "4.22", ".984"),
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
            (2025, "2025-04-01", 1, 10, "Pete Example", "NYM", "New York Mets", "ATL", "Atlanta Braves", 40, 40, 20, 10, 2, 0, 8, 4, 10, 24, 24, 12.800, 18.200, 40.0, 27.500, 12, 6, 2450.0, 24, 112.0, 74.0, 76.0),
            (2025, "2025-04-01", 1, 20, "Carl Example", "ATL", "Atlanta Braves", "NYM", "New York Mets", 38, 38, 11, 8, 1, 0, 2, 2, 12, 8, 22, 8.100, 12.700, 38.0, 14.300, 7, 1, 2165.0, 22, 104.0, 70.0, 71.0),
        ],
    )
    con.executemany(
        """
        INSERT INTO statcast_team_games(
            season, game_date, game_pk, team, team_name, opponent, opponent_name, is_home,
            plate_appearances, at_bats, hits, strikeouts, batted_ball_events, xba_numerator,
            xwoba_numerator, xwoba_denom, xslg_numerator, hard_hit_bbe, barrel_bbe, launch_speed_sum,
            launch_speed_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (2025, "2025-04-01", 1, "NYM", "New York Mets", "ATL", "Atlanta Braves", 1, 36, 34, 10, 8, 18, 8.500, 12.100, 36.0, 15.300, 9, 3, 1775.0, 18),
            (2025, "2025-04-01", 1, "ATL", "Atlanta Braves", "NYM", "New York Mets", 0, 34, 32, 5, 11, 16, 5.100, 7.500, 34.0, 8.400, 4, 1, 1490.0, 16),
        ],
    )
    con.commit()
    return con


def test_historical_player_season_leaderboard_builds() -> None:
    con = build_test_connection()
    researcher = SeasonMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "who had the highest OPS in 2024?")
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "season_metric_leaderboard"
    assert snippet.payload["rows"][0]["player_name"] == "Alex Alpha"
    con.close()


def test_historical_team_season_leaderboard_builds() -> None:
    con = build_test_connection()
    researcher = SeasonMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "which team had the highest batting average in 2024?")
    assert snippet is not None
    assert snippet.payload["rows"][0]["team_name"] == "Boston Red Sox"
    con.close()


def test_historical_team_history_run_differential_with_record_filter_builds() -> None:
    con = build_test_connection()
    researcher = SeasonMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "which team in MLB history had the worst run differential with a winning record")
    assert snippet is not None
    assert snippet.payload["source_family"] == "historical"
    assert snippet.payload["entity_scope"] == "team"
    assert snippet.payload["rows"][0]["team_name"] == "Miami Marlins"
    assert snippet.payload["rows"][0]["run_differential"] == -57
    assert snippet.payload["rows"][0]["scope_label"] == "2023"
    con.close()


def test_historical_team_history_combined_all_time_batting_average_aggregates_by_franchise() -> None:
    con = build_test_connection()
    researcher = SeasonMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(
        con,
        "which team in major league baseball history has the lowest combined all-time batting average",
    )
    assert snippet is not None
    assert snippet.payload["source_family"] == "historical"
    assert snippet.payload["entity_scope"] == "team"
    assert snippet.payload["scope_label"] == "MLB history"
    assert snippet.payload["rows"][0]["team_name"] == "Miami Marlins"
    assert round(snippet.payload["rows"][0]["avg"], 3) == 0.250
    con.close()


def test_statcast_player_season_leaderboard_builds() -> None:
    con = build_test_connection()
    researcher = SeasonMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "who had the highest xBA in 2025?")
    assert snippet is not None
    assert snippet.payload["source_family"] == "statcast"
    assert snippet.payload["rows"][0]["player_name"] == "Pete Example"
    con.close()


def test_statcast_player_avg_leaderboard_builds() -> None:
    con = build_test_connection()
    researcher = SeasonMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(con, "who had the highest batting average in 2025?")
    assert snippet is not None
    assert snippet.payload["source_family"] == "statcast"
    assert snippet.payload["rows"][0]["player_name"] == "Pete Example"
    con.close()


def test_provider_pitching_metric_leaderboard_builds() -> None:
    con = build_test_connection()
    researcher = SeasonMetricLeaderboardResearcher(TEST_SETTINGS)
    original = season_metric_leaderboards.fetch_provider_group_rows

    def fake_fetch_provider_group_rows(group, column_name, season, qualified_only, cache, *, team_filter=None, minimum_starts=None):
        assert group == "pitching"
        assert column_name == "xFIP"
        assert season == 2024
        assert minimum_starts == 20
        return [
            {"name": "Ace Example", "team": "BOS", "metric_value": 2.61, "starts": 31},
            {"name": "Other Example", "team": "NYY", "metric_value": 2.95, "starts": 30},
        ]

    season_metric_leaderboards.fetch_provider_group_rows = fake_fetch_provider_group_rows
    try:
        snippet = researcher.build_snippet(con, "which pitcher had the lowest xFIP in 2024 with a minimum of 20 starts?")
    finally:
        season_metric_leaderboards.fetch_provider_group_rows = original
    assert snippet is not None
    assert snippet.payload["source_family"] == "provider"
    assert snippet.payload["rows"][0]["player_name"] == "Ace Example"
    con.close()


def test_historical_pitcher_range_leaderboard_aggregates_last_five_years() -> None:
    con = build_test_connection()
    researcher = SeasonMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(
        con,
        "show me the Red Sox leader in ERA over the last 5 years, with a minimum of 35 starts",
    )
    assert snippet is not None
    assert snippet.payload["source_family"] == "historical"
    assert snippet.payload["rows"][0]["player_name"] == "Paula Pitcher"
    assert snippet.payload["rows"][0]["scope_label"] == "2022-2026"
    con.close()


def test_historical_pitcher_range_leaderboard_uses_requested_metric_not_starts() -> None:
    con = build_test_connection()
    researcher = SeasonMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(
        con,
        "show me the Red Sox leader in FIP over the last 5 years, with a minimum of 35 starts",
    )
    assert snippet is not None
    assert snippet.payload["metric"] == "FIP"
    assert snippet.payload["rows"][0]["player_name"] == "Paula Pitcher"
    con.close()


def test_historical_pitcher_all_time_bb9_leaderboard_aggregates_career() -> None:
    con = build_test_connection()
    researcher = SeasonMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(
        con,
        "which pitcher with a minimum of 50 starts has the highest bb/9 of all time?",
    )
    assert snippet is not None
    assert snippet.payload["source_family"] == "historical"
    assert snippet.payload["rows"][0]["player_name"] == "Rita Rotation"
    assert snippet.payload["rows"][0]["scope_label"] == "2022-2026"
    con.close()


def test_historical_pitcher_walks_per_game_query_maps_to_career_leaderboard() -> None:
    con = build_test_connection()
    researcher = SeasonMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(
        con,
        "which pitcher with at least 50 starts walked the most batters per game",
    )
    assert snippet is not None
    assert snippet.payload["source_family"] == "historical"
    assert snippet.payload["metric"] == "BB/G"
    assert snippet.payload["rows"][0]["player_name"] == "Rita Rotation"
    assert snippet.payload["rows"][0]["scope_label"] == "2022-2026"
    con.close()


def test_explicit_hitter_query_excludes_pitcher_only_batting_rows() -> None:
    con = build_test_connection()
    researcher = SeasonMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(
        con,
        "which hitter has the lowest BA over the last 10 seasons?",
    )
    assert snippet is not None
    assert snippet.payload["rows"][0]["player_name"] == "Ben Bravo"
    assert round(snippet.payload["rows"][0]["avg"], 3) == 0.182
    con.close()


def test_historical_hitter_struck_out_most_times_wording_maps_to_strikeouts() -> None:
    con = build_test_connection()
    researcher = SeasonMetricLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(
        con,
        "which hitter has struck out the most times in mlb history",
    )
    assert snippet is not None
    assert snippet.payload["metric"] == "SO"
    assert snippet.payload["rows"][0]["player_name"] == "Alex Alpha"
    con.close()


def test_initialize_database_migrates_existing_statcast_events_columns_before_indexes() -> None:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        """
        CREATE TABLE statcast_events (
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
        )
        """
    )
    initialize_database(con)
    columns = [row["name"] for row in con.execute("PRAGMA table_info(statcast_events)").fetchall()]
    assert "count_key" in columns
    assert "balls" in columns
    assert "strikes" in columns
    con.close()
