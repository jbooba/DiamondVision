from __future__ import annotations

from mlb_history_bot.season_spread_leaderboards import SeasonSpreadLeaderboardResearcher
from tests.test_season_metric_leaderboards import TEST_SETTINGS, build_test_connection


def test_historical_pitcher_season_spread_builds() -> None:
    con = build_test_connection()
    researcher = SeasonSpreadLeaderboardResearcher(TEST_SETTINGS)
    snippet = researcher.build_snippet(
        con,
        "who has the highest delta between their highest ERA season and lowest ERA season",
    )
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "season_metric_spread_leaderboard"
    assert snippet.payload["rows"][0]["player_name"] == "Rita Rotation"
    assert round(snippet.payload["rows"][0]["spread_value"], 3) == 0.962
    assert snippet.payload["rows"][0]["high_scope_label"] == "2026"
    assert snippet.payload["rows"][0]["low_scope_label"] == "2023"
    con.close()
