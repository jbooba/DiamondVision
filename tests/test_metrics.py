from pathlib import Path

from mlb_history_bot.fielding_bible_search import (
    extract_component_request,
    extract_position_request,
    is_drs_question,
    wants_current_drs,
    wants_drs_data_lookup,
)
from mlb_history_bot.home_run_robbery import extract_target_date, normalize_person_name
from mlb_history_bot.metrics import MetricCatalog
from mlb_history_bot.search import extract_name_candidates, extract_year
from mlb_history_bot.sporty_research import extract_replay_tags, wants_sporty_replay


def test_metric_catalog_finds_drs() -> None:
    project_root = Path(__file__).resolve().parents[1]
    catalog = MetricCatalog.load(project_root)
    match = catalog.find_exact("DRS")
    assert match is not None
    assert match.exact_formula_public is False


def test_extract_name_candidates() -> None:
    names = extract_name_candidates("Was Joe Adell better than Mike Trout tonight?")
    assert "Joe Adell" in names
    assert "Mike Trout" in names


def test_extract_name_candidates_from_lowercase_profile_prompt() -> None:
    names = extract_name_candidates("who is nathan church")
    assert names == ["Nathan Church"]


def test_extract_year() -> None:
    assert extract_year("Who led the AL in 1957?") == 1957


def test_extract_position_request_for_infielders() -> None:
    request = extract_position_request("Which infielders led DRS in 2013?")
    assert request is not None
    assert request.primary_labels == ("IF",)


def test_detects_drs_question() -> None:
    assert is_drs_question("Who leads MLB in Defensive Runs Saved?")


def test_detects_component_drs_question() -> None:
    assert is_drs_question("Who leads in rARM this season?")


def test_extract_component_request_for_rarm() -> None:
    request = extract_component_request("Who leads MLB in rARM right now?")
    assert request is not None
    assert request.metric_name == "rARM"


def test_metric_catalog_finds_def() -> None:
    project_root = Path(__file__).resolve().parents[1]
    catalog = MetricCatalog.load(project_root)
    match = catalog.find_exact("Def")
    assert match is not None
    assert match.name == "Def"


def test_definition_style_component_query_does_not_force_data_lookup() -> None:
    assert wants_drs_data_lookup("What is rARM?") is False


def test_wants_current_drs_without_explicit_year() -> None:
    assert wants_current_drs("Who leads MLB in DRS right now?", 2026)


def test_historical_year_overrides_current_drs_guess() -> None:
    assert wants_current_drs("Who led MLB in DRS in 2013?", 2026) is False


def test_extract_target_date_from_month_day_year() -> None:
    assert str(extract_target_date("What was Adolis Garcia's rHR on July 29, 2025?", 2026)) == "2025-07-29"


def test_normalize_person_name_removes_accents() -> None:
    assert normalize_person_name("Adolis García") == normalize_person_name("Adolis Garcia")


def test_extract_replay_tags_for_defensive_home_run_question() -> None:
    tags = extract_replay_tags("Did Jo Adell rob a home run tonight?")
    assert "home run" in tags
    assert "robbery" in tags


def test_wants_sporty_replay_for_player_date_query() -> None:
    assert wants_sporty_replay("Show me Jo Adell's defensive clips tonight.", 2026)


def test_wants_sporty_replay_false_without_play_context() -> None:
    assert wants_sporty_replay("Who leads MLB in DRS this season?", 2026) is False


def test_wants_sporty_replay_false_for_historical_leaderboard_question() -> None:
    assert wants_sporty_replay("Which player has hit the most home runs on August 1st?", 2026) is False
