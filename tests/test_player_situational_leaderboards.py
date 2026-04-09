from __future__ import annotations

import sqlite3
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.player_situational_leaderboards import PlayerSituationalLeaderboardResearcher
from mlb_history_bot.storage import initialize_database


TEST_SETTINGS = Settings.from_env(Path(__file__).resolve().parents[1])


def build_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    rows = []
    for index in range(13):
        rows.append(build_event_row(2020, 100 + index, "Alpha Bat", "NYM", 1, 1, 1, 0, 0, 0))
    for index in range(12):
        rows.append(build_event_row(2021, 200 + index, "Alpha Bat", "NYM", 1, 0, 0, 0, 1, 0))
    for index in range(10):
        rows.append(build_event_row(2021, 300 + index, "Beta Bat", "ATL", 1, 1, 1, 0, 0, 0))
    for index in range(16):
        rows.append(build_event_row(2022, 400 + index, "Beta Bat", "ATL", 1, 1, 0, 0, 0, 1))
    connection.executemany(
        """
        INSERT INTO statcast_events (
            season, game_date, game_pk, at_bat_number, pitch_number,
            batter_id, batter_name, pitcher_id, pitcher_name,
            batting_team, pitching_team, home_team, away_team, stand, p_throws,
            pitch_type, pitch_name, pitch_family, event, is_ab, is_hit, is_xbh,
            is_home_run, is_strikeout, has_risp, balls, strikes, count_key,
            outs_when_up, runs_batted_in, horizontal_location, vertical_location,
            field_direction, release_speed, release_spin_rate, launch_speed,
            launch_angle, hit_distance, bat_speed, estimated_ba, estimated_woba,
            estimated_slg
        ) VALUES (
            :season, :game_date, :game_pk, :at_bat_number, :pitch_number,
            :batter_id, :batter_name, :pitcher_id, :pitcher_name,
            :batting_team, :pitching_team, :home_team, :away_team, :stand, :p_throws,
            :pitch_type, :pitch_name, :pitch_family, :event, :is_ab, :is_hit, :is_xbh,
            :is_home_run, :is_strikeout, :has_risp, :balls, :strikes, :count_key,
            :outs_when_up, :runs_batted_in, :horizontal_location, :vertical_location,
            :field_direction, :release_speed, :release_spin_rate, :launch_speed,
            :launch_angle, :hit_distance, :bat_speed, :estimated_ba, :estimated_woba,
            :estimated_slg
        )
        """,
        rows,
    )
    connection.commit()
    return connection


def build_event_row(
    season: int,
    game_pk: int,
    batter_name: str,
    batting_team: str,
    has_risp: int,
    is_ab: int,
    is_hit: int,
    home_run: int,
    walk: int,
    strikeout: int,
) -> dict[str, object]:
    batter_id = 1 if batter_name == "Alpha Bat" else 2
    event = "single" if is_hit and not home_run else "home_run" if home_run else "walk" if walk else "strikeout" if strikeout else "field_out"
    return {
        "season": season,
        "game_date": f"{season}-06-01",
        "game_pk": game_pk,
        "at_bat_number": 1,
        "pitch_number": 1,
        "batter_id": batter_id,
        "batter_name": batter_name,
        "pitcher_id": 100 + batter_id,
        "pitcher_name": "Pitcher",
        "batting_team": batting_team,
        "pitching_team": "LAD",
        "home_team": batting_team,
        "away_team": "LAD",
        "stand": "L",
        "p_throws": "R",
        "pitch_type": "FF",
        "pitch_name": "4-Seam Fastball",
        "pitch_family": "fastball",
        "event": event,
        "is_ab": is_ab,
        "is_hit": is_hit,
        "is_xbh": home_run,
        "is_home_run": home_run,
        "is_strikeout": strikeout,
        "has_risp": has_risp,
        "balls": 0,
        "strikes": 0,
        "count_key": "0-0",
        "outs_when_up": 0,
        "runs_batted_in": home_run,
        "horizontal_location": "middle",
        "vertical_location": "middle",
        "field_direction": "center field",
        "release_speed": 95.0,
        "release_spin_rate": 2200.0,
        "launch_speed": 100.0 if is_hit else None,
        "launch_angle": 20.0 if is_hit else None,
        "hit_distance": 390.0 if home_run else 250.0 if is_hit else None,
        "bat_speed": 73.0,
        "estimated_ba": 0.5 if is_hit else 0.0,
        "estimated_woba": 0.8 if is_hit else 0.0,
        "estimated_slg": 1.0 if is_hit else 0.0,
    }


def test_player_situational_uses_local_statcast_events_for_statcast_era_risp() -> None:
    connection = build_connection()
    researcher = PlayerSituationalLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(connection, "which hitter has the highest OPS with RISP in the Statcast era?")
    connection.close()
    assert snippet is not None
    assert snippet.payload["scope_label"] == "Statcast era"
    assert snippet.payload["leaders"][0]["player_name"] == "Alpha Bat"
    assert snippet.payload["leaders"][0]["plate_appearances"] == 25


def test_player_situational_supports_multi_season_span_with_local_events() -> None:
    connection = build_connection()
    researcher = PlayerSituationalLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(connection, "which hitter has the lowest batting average with RISP from 2021 to 2022?")
    connection.close()
    assert snippet is not None
    assert snippet.payload["scope_label"] == "2021-2022"
    assert snippet.payload["leaders"][0]["player_name"] == "Beta Bat"
