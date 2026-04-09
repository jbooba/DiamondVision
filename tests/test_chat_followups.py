from types import SimpleNamespace

from pathlib import Path

from mlb_history_bot.chat import extract_follow_up_seed, rewrite_contextual_follow_up_question, rewrite_follow_up_question, sanitize_answer_text
from mlb_history_bot.metrics import MetricCatalog


def test_rewrite_switch_to_metric_followup() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    rewritten = rewrite_follow_up_question(
        "switch to batting average",
        "what team has the worst xBA through the first 10 games of a season?",
        catalog,
    )
    assert rewritten == "what team has the worst batting average through the first 10 games of a season?"


def test_rewrite_what_about_metric_followup() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    rewritten = rewrite_follow_up_question(
        "what about OPS?",
        "what team has the worst batting average through the first 10 games of a season?",
        catalog,
    )
    assert rewritten == "what team has the worst OPS through the first 10 games of a season?"


def test_sanitize_answer_text_strips_markdown_emphasis_markers() -> None:
    cleaned = sanitize_answer_text(
        "The Red Sox did damage in the **3rd inning**.\n- **Wilyer Abreu:** 2-for-4\n- It was a *shutout*.\nSources: Test"
    )
    assert "**" not in cleaned
    assert "*shutout*" not in cleaned
    assert "3rd inning" in cleaned
    assert "Wilyer Abreu: 2-for-4" in cleaned
    assert "It was a shutout." in cleaned


def test_rewrite_contextual_followup_uses_previous_player_and_span() -> None:
    rewritten = rewrite_contextual_follow_up_question(
        "how many of those 993 hits are homeruns?",
        {
            "player_name": "Kyle Schwarber",
            "scope_start_season": 2017,
            "scope_end_season": 2026,
            "metric": "PA",
        },
    )
    assert rewritten == "how many home runs did Kyle Schwarber have between 2017 and 2026?"


def test_extract_follow_up_seed_prefers_player_named_in_answer() -> None:
    context = SimpleNamespace(
        historical_evidence=[
            SimpleNamespace(
                payload={
                    "rows": [
                        {
                            "player_name": "Francisco Lindor",
                            "scope_start_season": 2017,
                            "scope_end_season": 2025,
                            "metric_value": 5400,
                            "plate_appearances": 5400,
                        },
                        {
                            "player_name": "Kyle Schwarber",
                            "scope_start_season": 2017,
                            "scope_end_season": 2026,
                            "metric_value": 5106,
                            "plate_appearances": 5106,
                        },
                    ],
                    "metric": "PA",
                }
            )
        ],
        live_evidence=[],
    )
    seed = extract_follow_up_seed(
        "From the provided high-PA leaderboard, Kyle Schwarber has the lowest BA among the hitters with the most plate appearances.",
        context,
    )
    assert seed == {
        "player_name": "Kyle Schwarber",
        "scope_start_season": 2017,
        "scope_end_season": 2026,
        "metric": "PA",
    }
