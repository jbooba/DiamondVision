import sqlite3
from datetime import date

import pandas as pd

from mlb_history_bot.statcast_sync import (
    aggregate_statcast_batter_games,
    aggregate_statcast_events,
    aggregate_statcast_pitch_type_games,
    aggregate_statcast_team_games,
    is_barrel,
    resolve_daily_statcast_window,
)
from mlb_history_bot.storage import initialize_database, set_metadata_value


class FakeLiveClient:
    def fetch_json(self, _url: str) -> dict:
        return {
            "dates": [
                {"date": "2026-03-25"},
                {"date": "2026-03-26"},
                {"date": "2026-04-05"},
                {"date": "2026-09-27"},
            ]
        }


def test_is_barrel_public_definition_examples() -> None:
    assert is_barrel(98.0, 26.0) is True
    assert is_barrel(98.0, 20.0) is False
    assert is_barrel(110.0, 20.0) is True


def test_aggregate_statcast_team_games() -> None:
    frame = pd.DataFrame(
        [
            {
                "events": "single",
                "game_type": "R",
                "inning_topbot": "Top",
                "away_team": "ATL",
                "home_team": "NYM",
                "launch_speed": 100.0,
                "launch_angle": 27.0,
                "estimated_ba_using_speedangle": 0.800,
                "estimated_woba_using_speedangle": 0.700,
                "estimated_slg_using_speedangle": 1.300,
                "woba_denom": 1.0,
                "game_date": "2026-04-04",
                "game_pk": 12345,
            },
            {
                "events": "strikeout",
                "game_type": "R",
                "inning_topbot": "Top",
                "away_team": "ATL",
                "home_team": "NYM",
                "launch_speed": None,
                "launch_angle": None,
                "estimated_ba_using_speedangle": None,
                "estimated_woba_using_speedangle": 0.0,
                "estimated_slg_using_speedangle": None,
                "woba_denom": 1.0,
                "game_date": "2026-04-04",
                "game_pk": 12345,
            },
            {
                "events": "walk",
                "game_type": "R",
                "inning_topbot": "Top",
                "away_team": "ATL",
                "home_team": "NYM",
                "launch_speed": None,
                "launch_angle": None,
                "estimated_ba_using_speedangle": None,
                "estimated_woba_using_speedangle": 0.690,
                "estimated_slg_using_speedangle": None,
                "woba_denom": 1.0,
                "game_date": "2026-04-04",
                "game_pk": 12345,
            },
        ]
    )
    rows = aggregate_statcast_team_games(frame)
    assert len(rows) == 1
    row = rows[0]
    assert row["team"] == "ATL"
    assert row["opponent"] == "NYM"
    assert row["plate_appearances"] == 3
    assert row["at_bats"] == 2
    assert row["hits"] == 1
    assert row["strikeouts"] == 1
    assert row["batted_ball_events"] == 1
    assert round(row["xba_numerator"], 3) == 0.800
    assert round(row["xwoba_numerator"], 3) == 1.390
    assert round(row["xslg_numerator"], 3) == 1.300
    assert row["hard_hit_bbe"] == 1
    assert row["barrel_bbe"] == 1


def test_aggregate_statcast_v2_layers() -> None:
    frame = pd.DataFrame(
        [
            {
                "events": "single",
                "description": "hit_into_play",
                "des": "Pete Alonso singles on a line drive to right field.",
                "game_type": "R",
                "inning_topbot": "Bottom",
                "away_team": "ATL",
                "home_team": "NYM",
                "pitcher": 101,
                "player_name": "Sale, Chris",
                "batter": 202,
                "pitch_type": "FF",
                "pitch_name": "4-Seam Fastball",
                "release_speed": 97.5,
                "release_spin_rate": 2450,
                "launch_speed": 101.0,
                "launch_angle": 18.0,
                "bat_speed": 75.5,
                "estimated_ba_using_speedangle": 0.720,
                "estimated_woba_using_speedangle": 0.650,
                "estimated_slg_using_speedangle": 1.100,
                "woba_denom": 1.0,
                "game_date": "2026-04-04",
                "game_pk": 55555,
                "at_bat_number": 1,
                "pitch_number": 3,
                "stand": "R",
                "p_throws": "L",
                "plate_x": -0.6,
                "plate_z": 3.3,
                "sz_top": 3.5,
                "sz_bot": 1.5,
                "on_2b": None,
                "on_3b": None,
                "rbi": 1,
                "hit_distance_sc": 220,
            },
            {
                "events": "",
                "description": "swinging_strike",
                "des": "Pete Alonso swinging strike.",
                "game_type": "R",
                "inning_topbot": "Bottom",
                "away_team": "ATL",
                "home_team": "NYM",
                "pitcher": 101,
                "player_name": "Sale, Chris",
                "batter": 202,
                "pitch_type": "SL",
                "pitch_name": "Slider",
                "release_speed": 86.1,
                "release_spin_rate": 2700,
                "launch_speed": None,
                "launch_angle": None,
                "bat_speed": None,
                "estimated_ba_using_speedangle": None,
                "estimated_woba_using_speedangle": None,
                "estimated_slg_using_speedangle": None,
                "woba_denom": 0.0,
                "game_date": "2026-04-04",
                "game_pk": 55555,
                "at_bat_number": 2,
                "pitch_number": 1,
                "stand": "R",
                "p_throws": "L",
                "plate_x": 0.1,
                "plate_z": 2.5,
                "sz_top": 3.5,
                "sz_bot": 1.5,
                "on_2b": 303,
                "on_3b": None,
                "rbi": 0,
                "hit_distance_sc": None,
            },
            {
                "events": "strikeout",
                "description": "swinging_strike",
                "des": "Pete Alonso strikes out swinging.",
                "game_type": "R",
                "inning_topbot": "Bottom",
                "away_team": "ATL",
                "home_team": "NYM",
                "pitcher": 101,
                "player_name": "Sale, Chris",
                "batter": 202,
                "pitch_type": "SL",
                "pitch_name": "Slider",
                "release_speed": 86.8,
                "release_spin_rate": 2725,
                "launch_speed": None,
                "launch_angle": None,
                "bat_speed": None,
                "estimated_ba_using_speedangle": None,
                "estimated_woba_using_speedangle": 0.0,
                "estimated_slg_using_speedangle": None,
                "woba_denom": 1.0,
                "game_date": "2026-04-04",
                "game_pk": 55555,
                "at_bat_number": 2,
                "pitch_number": 4,
                "stand": "R",
                "p_throws": "L",
                "plate_x": 0.2,
                "plate_z": 1.7,
                "sz_top": 3.5,
                "sz_bot": 1.5,
                "on_2b": 303,
                "on_3b": None,
                "rbi": 0,
                "hit_distance_sc": None,
            },
        ]
    )

    batter_games = aggregate_statcast_batter_games(frame)
    assert len(batter_games) == 1
    batter_row = batter_games[0]
    assert batter_row["batter_name"] == "Pete Alonso"
    assert batter_row["plate_appearances"] == 2
    assert batter_row["hits"] == 1
    assert batter_row["home_runs"] == 0
    assert batter_row["strikeouts"] == 1

    pitch_type_games = aggregate_statcast_pitch_type_games(frame)
    ff_row = next(row for row in pitch_type_games if row["pitch_type"] == "FF")
    sl_row = next(row for row in pitch_type_games if row["pitch_type"] == "SL")
    assert ff_row["pitches"] == 1
    assert ff_row["hits_allowed"] == 1
    assert sl_row["pitches"] == 2
    assert sl_row["strikeouts"] == 1
    assert sl_row["whiffs"] == 2

    event_rows = aggregate_statcast_events(frame)
    assert len(event_rows) == 2
    hit_row = next(row for row in event_rows if row["event"] == "single")
    strikeout_row = next(row for row in event_rows if row["event"] == "strikeout")
    assert hit_row["field_direction"] == "right field"
    assert hit_row["horizontal_location"] == "inside"
    assert hit_row["vertical_location"] == "high"
    assert strikeout_row["has_risp"] == 1


def test_resolve_daily_statcast_window_with_overlap() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    set_metadata_value(connection, "statcast_max_synced_date_2026", "2026-04-04")
    window = resolve_daily_statcast_window(
        connection,
        FakeLiveClient(),
        today=date(2026, 4, 5),
        season=2026,
        backfill_days=3,
    )
    assert window is not None
    assert window.start_date.isoformat() == "2026-04-01"
    assert window.end_date.isoformat() == "2026-04-05"
