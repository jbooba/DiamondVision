from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from mlb_history_bot.award_history import AwardHistoryResearcher, parse_award_history_query
from mlb_history_bot.config import Settings


TEST_SETTINGS = Settings.from_env(Path(__file__).resolve().parents[1])


def fake_award_recipients(award_id: str):
    if award_id == "ALCY":
        return [
            {
                "id": "ALCY",
                "season": "2025",
                "date": "2025-11-12",
                "player": {"id": 1, "nameFirstLast": "Tarik Skubal", "primaryPosition": {"abbreviation": "P"}},
            },
            {
                "id": "ALCY",
                "season": "2021",
                "date": "2021-11-17",
                "player": {"id": 2, "nameFirstLast": "Robbie Ray", "primaryPosition": {"abbreviation": "P"}},
            },
        ]
    if award_id == "NLCY":
        return [
            {
                "id": "NLCY",
                "season": "2025",
                "date": "2025-11-12",
                "player": {"id": 3, "nameFirstLast": "Paul Skenes", "primaryPosition": {"abbreviation": "P"}},
            },
            {
                "id": "NLCY",
                "season": "2021",
                "date": "2021-11-17",
                "player": {"id": 4, "nameFirstLast": "Corbin Burnes", "primaryPosition": {"abbreviation": "P"}},
            },
        ]
    return []


def test_parse_award_history_query_ignores_relational_prompt() -> None:
    query = parse_award_history_query("who has the highest walk rate against Cy Young winners?", TEST_SETTINGS)
    assert query is None


def test_award_history_lists_cy_young_winners() -> None:
    researcher = AwardHistoryResearcher(TEST_SETTINGS)
    with patch("mlb_history_bot.live.LiveStatsClient.award_recipients", side_effect=fake_award_recipients):
        snippet = researcher.build_snippet("which pitchers have won Cy Young Awards?")
    assert snippet is not None
    assert snippet.source == "Award History"
    assert snippet.payload["analysis_type"] == "award_history"
    assert snippet.payload["award_key"] == "cy_young"
    assert snippet.payload["total_row_count"] == 4
    assert snippet.payload["rows"][0]["player_name"] == "Tarik Skubal"


def test_award_history_filters_to_specific_season() -> None:
    researcher = AwardHistoryResearcher(TEST_SETTINGS)
    with patch("mlb_history_bot.live.LiveStatsClient.award_recipients", side_effect=fake_award_recipients):
        snippet = researcher.build_snippet("who won the Cy Young Award in 2021?")
    assert snippet is not None
    seasons = {row["season"] for row in snippet.payload["rows"]}
    winners = {row["player_name"] for row in snippet.payload["rows"]}
    assert seasons == {2021}
    assert winners == {"Robbie Ray", "Corbin Burnes"}
