from pathlib import Path

from mlb_history_bot.chat import rewrite_follow_up_question
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
