from __future__ import annotations

import sqlite3
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.statcast_event_leaderboards import StatcastEventResearcher
from mlb_history_bot.storage import initialize_database


TEST_SETTINGS = Settings.from_env(Path(__file__).resolve().parents[1])


def build_test_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    connection.execute(
        """
        CREATE TABLE lahman_teams (
            yearid TEXT,
            teamid TEXT,
            name TEXT,
            park TEXT
        )
        """
    )
    connection.executemany(
        "INSERT INTO lahman_teams(yearid, teamid, name, park) VALUES (?, ?, ?, ?)",
        [
            ("2024", "SFN", "San Francisco Giants", "Oracle Park"),
            ("2025", "SFN", "San Francisco Giants", "Oracle Park"),
        ],
    )
    rows = []
    rows.extend(
        [
            build_event_row(
                season=2017,
                game_date="2017-07-23",
                game_pk=1,
                at_bat_number=1,
                pitch_number=3,
                batter_id=11,
                batter_name="Line Drive Guy",
                pitcher_id=101,
                pitcher_name="Pitcher A",
                event="single",
                launch_speed=104.2,
                hit_distance=255.0,
                launch_angle=14.0,
            ),
            build_event_row(
                season=2017,
                game_date="2017-07-23",
                game_pk=1,
                at_bat_number=2,
                pitch_number=4,
                batter_id=12,
                batter_name="Gap Power",
                pitcher_id=101,
                pitcher_name="Pitcher A",
                event="double",
                launch_speed=108.7,
                hit_distance=301.0,
                launch_angle=21.0,
            ),
        ]
    )
    for index in range(10):
        rows.append(
            build_event_row(
                season=2024,
                game_date=f"2024-06-{index + 1:02d}",
                game_pk=100 + index,
                at_bat_number=1,
                pitch_number=2,
                batter_id=21,
                batter_name="Short Porch",
                pitcher_id=201,
                pitcher_name="Pitcher B",
                event="home_run",
                launch_speed=101.0 + index * 0.2,
                hit_distance=360.0 + index,
                launch_angle=29.0,
                runs_batted_in=1,
            )
        )
    for index in range(12):
        rows.append(
            build_event_row(
                season=2024,
                game_date=f"2024-07-{index + 1:02d}",
                game_pk=200 + index,
                at_bat_number=1,
                pitch_number=2,
                batter_id=22,
                batter_name="Moonshot",
                pitcher_id=202,
                pitcher_name="Pitcher C",
                event="home_run",
                launch_speed=105.0 + index * 0.3,
                hit_distance=401.0 + index,
                launch_angle=31.0,
                runs_batted_in=2,
            )
        )
    for index in range(9):
        rows.append(
            build_event_row(
                season=2024,
                game_date=f"2024-08-{index + 1:02d}",
                game_pk=300 + index,
                at_bat_number=1,
                pitch_number=2,
                batter_id=23,
                batter_name="Tiny Sample",
                pitcher_id=203,
                pitcher_name="Pitcher D",
                event="home_run",
                launch_speed=99.0 + index * 0.4,
                hit_distance=330.0 + index,
                launch_angle=28.0,
                runs_batted_in=1,
            )
        )
    for index in range(6):
        rows.append(
            build_event_row(
                season=2025,
                game_date=f"2025-05-{index + 1:02d}",
                game_pk=400 + index,
                at_bat_number=1,
                pitch_number=2,
                batter_id=31,
                batter_name="Pull Lefty",
                pitcher_id=301,
                pitcher_name="Pitcher E",
                event="single" if index < 4 else "double",
                launch_speed=96.0 + index,
                hit_distance=250.0 + index * 5,
                launch_angle=16.0,
                batting_team="SFG",
                pitching_team="LAD",
                home_team="SFG",
                away_team="LAD",
                field_direction="left field",
            )
        )
    for index in range(4):
        rows.append(
            build_event_row(
                season=2025,
                game_date=f"2025-06-{index + 1:02d}",
                game_pk=500 + index,
                at_bat_number=1,
                pitch_number=2,
                batter_id=32,
                batter_name="Oppo Guy",
                pitcher_id=302,
                pitcher_name="Pitcher F",
                event="single",
                launch_speed=94.0 + index,
                hit_distance=240.0 + index * 4,
                launch_angle=14.0,
                batting_team="SFG",
                pitching_team="SDP",
                home_team="SFG",
                away_team="SDP",
                field_direction="left field",
            )
        )
    connection.executemany(
        """
        INSERT INTO statcast_events (
            season, game_date, game_pk, at_bat_number, pitch_number,
            batter_id, batter_name, pitcher_id, pitcher_name,
            batting_team, pitching_team, home_team, away_team,
            pitch_name, pitch_family, event, is_ab, is_hit, is_home_run, is_strikeout,
            has_risp, count_key, runs_batted_in, horizontal_location, vertical_location, field_direction,
            release_speed, release_spin_rate, launch_speed, launch_angle, hit_distance,
            bat_speed, estimated_ba, estimated_woba, estimated_slg
        ) VALUES (
            :season, :game_date, :game_pk, :at_bat_number, :pitch_number,
            :batter_id, :batter_name, :pitcher_id, :pitcher_name,
            :batting_team, :pitching_team, :home_team, :away_team,
            :pitch_name, :pitch_family, :event, :is_ab, :is_hit, :is_home_run, :is_strikeout,
            :has_risp, :count_key, :runs_batted_in, :horizontal_location, :vertical_location, :field_direction,
            :release_speed, :release_spin_rate, :launch_speed, :launch_angle, :hit_distance,
            :bat_speed, :estimated_ba, :estimated_woba, :estimated_slg
        )
        """,
        rows,
    )
    connection.commit()
    return connection


def build_event_row(
    *,
    season: int,
    game_date: str,
    game_pk: int,
    at_bat_number: int,
    pitch_number: int,
    batter_id: int,
    batter_name: str,
    pitcher_id: int,
    pitcher_name: str,
    event: str,
    launch_speed: float,
    hit_distance: float,
    launch_angle: float,
    runs_batted_in: int = 0,
    batting_team: str = "NYM",
    pitching_team: str = "ATL",
    home_team: str = "NYM",
    away_team: str = "ATL",
    field_direction: str = "center field",
) -> dict[str, object]:
    return {
        "season": season,
        "game_date": game_date,
        "game_pk": game_pk,
        "at_bat_number": at_bat_number,
        "pitch_number": pitch_number,
        "batter_id": batter_id,
        "batter_name": batter_name,
        "pitcher_id": pitcher_id,
        "pitcher_name": pitcher_name,
        "batting_team": batting_team,
        "pitching_team": pitching_team,
        "home_team": home_team,
        "away_team": away_team,
        "pitch_name": "4-Seam Fastball",
        "pitch_family": "fastball",
        "event": event,
        "is_ab": 1,
        "is_hit": 1 if event in {"single", "double", "triple", "home_run"} else 0,
        "is_home_run": 1 if event == "home_run" else 0,
        "is_strikeout": 0,
        "has_risp": 0,
        "count_key": "1-2",
        "runs_batted_in": runs_batted_in,
        "horizontal_location": "middle",
        "vertical_location": "middle",
        "field_direction": field_direction,
        "release_speed": 96.0,
        "release_spin_rate": 2430.0,
        "launch_speed": launch_speed,
        "launch_angle": launch_angle,
        "hit_distance": hit_distance,
        "bat_speed": 73.5,
        "estimated_ba": 0.710,
        "estimated_woba": 0.650,
        "estimated_slg": 1.450,
    }


def test_event_leaderboard_supports_short_slash_dates() -> None:
    connection = build_test_connection()
    researcher = StatcastEventResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(connection, "show me hits with the highest EV from 07/23/17")
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "statcast_event_leaderboard"
    assert snippet.payload["leaders"][0]["player_name"] == "Gap Power"
    assert snippet.payload["leaders"][0]["game_date"] == "2017-07-23"
    connection.close()


def test_player_average_home_run_distance_with_minimum() -> None:
    connection = build_test_connection()
    researcher = StatcastEventResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(
        connection,
        "player with the lowest average career home run distance in the statcast era with at least 10 home runs",
    )
    assert snippet is not None
    assert snippet.payload["aggregate_mode"] == "player_avg"
    assert snippet.payload["leaders"][0]["player_name"] == "Short Porch"
    assert snippet.payload["leaders"][0]["event_count"] == 10
    connection.close()


def test_player_event_count_leaderboard_supports_park_and_direction_filters() -> None:
    connection = build_test_connection()
    researcher = StatcastEventResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(
        connection,
        "who has the most hits to left field at Oracle Park in the Statcast era?",
    )
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "statcast_event_leaderboard"
    assert snippet.payload["aggregate_mode"] == "player_max"
    assert snippet.payload["leaders"][0]["player_name"] == "Pull Lefty"
    assert snippet.payload["leaders"][0]["event_count"] == 6
    connection.close()
