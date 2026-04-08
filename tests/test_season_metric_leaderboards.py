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
    con.execute(
        """
        CREATE TABLE lahman_teams (
            yearid TEXT,
            teamid TEXT,
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
        ],
    )
    con.executemany(
        """
        INSERT INTO lahman_teams(
            yearid, teamid, name, g, w, l, r, ab, h, c_2b, c_3b, hr, bb, hbp, sf, ra, era, fp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("2024", "BOS", "Boston Red Sox", "162", "81", "81", "751", "5500", "1450", "300", "25", "210", "550", "60", "45", "720", "4.10", ".985"),
            ("2024", "NYY", "New York Yankees", "162", "90", "72", "720", "5525", "1350", "250", "20", "180", "500", "40", "40", "650", "3.85", ".983"),
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
        assert column_name == "FIP"
        assert season == 2024
        assert minimum_starts == 20
        return [
            {"name": "Ace Example", "team": "BOS", "metric_value": 2.61, "starts": 31},
            {"name": "Other Example", "team": "NYY", "metric_value": 2.95, "starts": 30},
        ]

    season_metric_leaderboards.fetch_provider_group_rows = fake_fetch_provider_group_rows
    try:
        snippet = researcher.build_snippet(con, "which pitcher had the lowest FIP in 2024 with a minimum of 20 starts?")
    finally:
        season_metric_leaderboards.fetch_provider_group_rows = original
    assert snippet is not None
    assert snippet.payload["source_family"] == "provider"
    assert snippet.payload["rows"][0]["player_name"] == "Ace Example"
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
