import sqlite3
from datetime import date

import pandas as pd

from mlb_history_bot.statcast_sync import aggregate_statcast_team_games, is_barrel, resolve_daily_statcast_window
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
