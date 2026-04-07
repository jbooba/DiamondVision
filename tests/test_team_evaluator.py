from mlb_history_bot.team_evaluator import (
    assessment_label,
    infer_focus,
    infer_tone,
    looks_like_team_evaluation,
    rank_from_values,
    resolve_team_from_question,
)


FAKE_TEAMS = [
    {
        "id": 137,
        "name": "San Francisco Giants",
        "abbreviation": "SF",
        "shortName": "San Francisco",
        "clubName": "Giants",
        "franchiseName": "San Francisco",
        "locationName": "San Francisco",
        "league": {"name": "National League"},
        "division": {"name": "National League West"},
    },
    {
        "id": 119,
        "name": "Los Angeles Dodgers",
        "abbreviation": "LAD",
        "shortName": "LA Dodgers",
        "clubName": "Dodgers",
        "franchiseName": "Los Angeles",
        "locationName": "Los Angeles",
        "league": {"name": "National League"},
        "division": {"name": "National League West"},
    },
]


def test_detect_team_evaluation_question() -> None:
    assert looks_like_team_evaluation("How bad is the current Giants roster?")


def test_infer_tone_negative() -> None:
    assert infer_tone("How bad is the current Giants roster?") == "negative"


def test_infer_tone_positive() -> None:
    assert infer_tone("Are the current Dodgers legit?") == "positive"


def test_infer_focus_pitching() -> None:
    assert infer_focus("How good is the current Giants rotation?") == "rotation"


def test_resolve_team_from_question() -> None:
    team = resolve_team_from_question("How bad is the current Giants roster?", FAKE_TEAMS)
    assert team is not None
    assert team.name == "San Francisco Giants"


def test_rank_from_values_clamps_at_bottom() -> None:
    assert rank_from_values(-1.0, [1.0, 2.0, 3.0], True) == 3


def test_assessment_label_for_low_score() -> None:
    assert assessment_label(18.0) == "brutal"
