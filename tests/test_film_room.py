from datetime import date

from mlb_history_bot.film_room_research import parse_story_query
from mlb_history_bot.query_utils import extract_date_window


def test_extract_date_window_for_last_night() -> None:
    window = extract_date_window("who hit the farthest home run last night?", 2026, today=date(2026, 4, 5))
    assert window is not None
    assert window.start_date.isoformat() == "2026-04-04"
    assert window.end_date.isoformat() == "2026-04-04"


def test_extract_date_window_for_this_week() -> None:
    window = extract_date_window("did anything weird happen this week?", 2026, today=date(2026, 4, 5))
    assert window is not None
    assert window.start_date.isoformat() == "2026-03-30"
    assert window.end_date.isoformat() == "2026-04-05"


def test_parse_story_query_for_farthest_home_run() -> None:
    query = parse_story_query("who hit the farthest home run last night?", 2026)
    assert query is not None
    assert query.kind == "home_run_distance"


def test_parse_story_query_for_coolest_plays() -> None:
    query = parse_story_query("show me the coolest plays that happened yesterday", 2026)
    assert query is not None
    assert query.kind == "coolest_plays"


def test_parse_story_query_for_weird_plays() -> None:
    query = parse_story_query("did anything weird or unusual happen this week?", 2026)
    assert query is not None
    assert query.kind == "weird_plays"


def test_parse_story_query_for_defensive_plays() -> None:
    query = parse_story_query("what were the best defensive plays yesterday?", 2026)
    assert query is not None
    assert query.kind == "defensive_plays"


def test_parse_story_query_for_lowercase_player_defensive_plays() -> None:
    query = parse_story_query("pete alonso defensive plays april 3rd 2026", 2026)
    assert query is not None
    assert query.kind == "defensive_plays"
    assert query.player_queries == ["Pete Alonso"]


def test_parse_story_query_for_defensive_performance() -> None:
    query = parse_story_query("who had the best defensive performance yesterday?", 2026)
    assert query is not None
    assert query.kind == "defensive_performance"
