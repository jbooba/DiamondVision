from datetime import date

from mlb_history_bot.daily_lookup import (
    parse_calendar_day_player_leaderboard_query,
    parse_daily_lookup_query,
    wants_historical_calendar_day_leaderboard,
)
from mlb_history_bot.query_utils import extract_date_window


def test_extract_date_window_accepts_ordinal_suffixes() -> None:
    window = extract_date_window("how many home runs were hit on april 3rd?", 2026, today=date(2026, 4, 5))
    assert window is not None
    assert window.start_date.isoformat() == "2026-04-03"
    assert window.end_date.isoformat() == "2026-04-03"


def test_parse_daily_lookup_query_for_home_runs() -> None:
    query = parse_daily_lookup_query("how many home runs were hit on april 3rd?", 2026)
    assert query is not None
    assert query.metric.key == "home_runs"
    assert query.date_window.start_date.isoformat() == "2026-04-03"


def test_parse_calendar_day_player_leaderboard_query() -> None:
    query = parse_calendar_day_player_leaderboard_query(
        "Which player has hit the most home runs on August 1st?",
        2026,
    )
    assert query is not None
    assert query.metric.key == "home_runs"
    assert query.month_day_key == "0801"


def test_detect_historical_calendar_day_leaderboard() -> None:
    assert wants_historical_calendar_day_leaderboard(
        "Historically, which player has hit the most home runs on August 1st?",
        2026,
    )
