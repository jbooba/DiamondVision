from mlb_history_bot.relationship_ontology import (
    is_current_team_status_question,
    parse_team_leader_intent,
)
from mlb_history_bot.team_evaluator import TeamIdentity
from mlb_history_bot.team_roster_leaders import (
    TeamRosterLeaderQuery,
    build_team_leader_rows,
)


def test_current_team_status_question_detects_generic_phrasing() -> None:
    assert is_current_team_status_question("how are the Giants doing so far?") is True
    assert is_current_team_status_question("how is the Mets season going right now?") is True
    assert is_current_team_status_question("how good were the 1979 Indians?") is False


def test_parse_team_leader_intent_defaults_best_hitter_to_ops() -> None:
    intent = parse_team_leader_intent("who is the best hitter on the Mets right now?")
    assert intent is not None
    assert intent.direction == "best"
    assert intent.role == "hitter"
    assert intent.metric == "ops"


def test_build_team_leader_rows_ranks_hitters_by_ops() -> None:
    query = TeamRosterLeaderQuery(
        team=TeamIdentity(
            team_id=121,
            name="New York Mets",
            abbreviation="NYM",
            short_name="Mets",
            club_name="Mets",
            franchise_name="Mets",
            location_name="New York",
            league="National League",
            division="National League East",
        ),
        season=2026,
        intent=parse_team_leader_intent("who is the best hitter on the Mets right now?"),
    )
    assert query.intent is not None
    people = [
        {
            "fullName": "Player One",
            "currentAge": 29,
            "primaryPosition": {"type": "Infielder", "abbreviation": "1B"},
            "stats": [
                {
                    "group": {"displayName": "hitting"},
                    "splits": [
                        {"stat": {"plateAppearances": 40, "ops": ".900", "obp": ".410", "slg": ".490", "avg": ".300", "homeRuns": 3, "rbi": 10, "gamesPlayed": 11}}
                    ],
                }
            ],
        },
        {
            "fullName": "Player Two",
            "currentAge": 27,
            "primaryPosition": {"type": "Outfielder", "abbreviation": "LF"},
            "stats": [
                {
                    "group": {"displayName": "hitting"},
                    "splits": [
                        {"stat": {"plateAppearances": 38, "ops": ".780", "obp": ".350", "slg": ".430", "avg": ".265", "homeRuns": 2, "rbi": 8, "gamesPlayed": 10}}
                    ],
                }
            ],
        },
    ]
    rows = build_team_leader_rows(people, query)
    assert len(rows) == 2
    assert rows[0]["player_name"] == "Player One"
    assert rows[0]["metric_value"] == 0.9
    assert rows[1]["player_name"] == "Player Two"
