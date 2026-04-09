from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.special_leaderboards import SpecialLeaderboardResearcher


def test_special_leaderboard_researcher_returns_award_gap_snippet() -> None:
    settings = Settings.from_env(Path(__file__).resolve().parents[1])
    researcher = SpecialLeaderboardResearcher(settings)
    snippet = researcher.build_snippet(
        None,
        "Which hitter has the best OPS against Cy Young Award winners?",
    )
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "contextual_source_gap"
    assert snippet.payload["context"] == "Cy Young Award winners"
