from pathlib import Path
from tempfile import TemporaryDirectory

from mlb_history_bot.manager_era_analysis import parse_manager_era_query
from mlb_history_bot.pitch_arsenal_leaderboards import parse_pitch_arsenal_query
from mlb_history_bot.salary_relationships import parse_career_earnings_query, parse_player_salary_query
from mlb_history_bot.statcast_relationships import (
    can_use_fast_velocity_pitcher_path,
    parse_statcast_relationship_query,
    query_precomputed_statcast_relationship,
)
from mlb_history_bot.storage import get_connection, initialize_database, replace_statcast_pitcher_games


def test_parse_manager_era_query_for_offense() -> None:
    query = parse_manager_era_query("who was the best offensive player for the mets under Buck Showalter?")
    assert query is not None
    assert query.focus == "offense"
    assert query.team_phrase.lower() == "mets"
    assert query.manager_name == "Buck Showalter"


def test_parse_manager_era_query_for_defense() -> None:
    query = parse_manager_era_query("who was the best defensive player for the mets under Buck Showalter?")
    assert query is not None
    assert query.focus == "defense"


def test_parse_manager_era_query_for_tenure_phrase() -> None:
    query = parse_manager_era_query("who was the best offensive player for the mets during Buck Showalter's tenure?")
    assert query is not None
    assert query.focus == "offense"
    assert query.manager_name == "Buck Showalter"


def test_parse_career_earnings_query() -> None:
    query = parse_career_earnings_query("Which Dominican born player has the highest career earnings?")
    assert query is not None
    assert "D.R." in query.country_filter


def test_parse_career_earnings_query_for_highest_paid_hyphenated() -> None:
    query = parse_career_earnings_query("Which Dominican-born player is the highest-paid?")
    assert query is not None
    assert "Dominican Republic" in query.country_filter


def test_parse_player_salary_query() -> None:
    query = parse_player_salary_query(
        "Use Mike Trout's contract information to analyze the relationship between his offensive production and how much money he makes. How much money is he making per game?"
    )
    assert query is not None
    assert query.player_name == "Mike Trout"


def test_parse_player_salary_query_for_per_homer() -> None:
    query = parse_player_salary_query("Analyze Pete Alonso's salary and how much money he made per home run.")
    assert query is not None
    assert query.player_name == "Pete Alonso"


def test_parse_statcast_relationship_query_for_100mph() -> None:
    query = parse_statcast_relationship_query("Who has thrown the most pitches over 100mph this year?", 2026)
    assert query is not None
    assert query.aggregate_by == "pitcher"
    assert query.metric_threshold_key == "release_speed"
    assert query.scope_label == "2026"


def test_parse_statcast_relationship_query_for_100mph_last_year() -> None:
    query = parse_statcast_relationship_query("Who threw the most pitches over 100mph last year?", 2026)
    assert query is not None
    assert query.aggregate_by == "pitcher"
    assert query.metric_threshold_key == "release_speed"
    assert query.scope_label == "2025"
    assert query.start_season == 2025
    assert query.end_season == 2025
    assert can_use_fast_velocity_pitcher_path(query) is True


def test_parse_statcast_relationship_query_for_changeup_strikeouts() -> None:
    query = parse_statcast_relationship_query("Which pitcher has the most strikeouts throwing changeups?", 2026)
    assert query is not None
    assert query.aggregate_by == "pitcher"
    assert query.pitch_family == "changeup"
    assert query.vertical_location is None


def test_parse_statcast_relationship_query_for_slider_spin() -> None:
    query = parse_statcast_relationship_query("Show me sliders with spin rates over 2500rpm", 2026)
    assert query is not None
    assert query.aggregate_by is None
    assert query.pitch_family == "slider"
    assert query.metric_threshold_key == "release_spin_rate"


def test_parse_statcast_relationship_query_for_inside_fastball_hits() -> None:
    query = parse_statcast_relationship_query("show me base hits on inside fastballs", 2026)
    assert query is not None
    assert query.pitch_family == "fastball"
    assert query.horizontal_location == "inside"


def test_parse_statcast_relationship_query_for_outside_fastball_hits() -> None:
    query = parse_statcast_relationship_query("show me base hits on outside fastballs", 2026)
    assert query is not None
    assert query.pitch_family == "fastball"
    assert query.horizontal_location == "outside"


def test_parse_statcast_relationship_query_for_high_fastball_hits() -> None:
    query = parse_statcast_relationship_query("show me base hits on high fastballs", 2026)
    assert query is not None
    assert query.pitch_family == "fastball"
    assert query.vertical_location == "high"


def test_parse_statcast_relationship_query_for_middle_middle_fastball_hits() -> None:
    query = parse_statcast_relationship_query("show me base hits on middle middle fastballs", 2026)
    assert query is not None
    assert query.pitch_family == "fastball"
    assert query.horizontal_location == "middle"
    assert query.vertical_location == "middle"


def test_parse_statcast_relationship_query_for_player_curveball_homers() -> None:
    query = parse_statcast_relationship_query("show me Pete Alonso home runs off curveballs", 2026)
    assert query is not None
    assert query.pitch_family == "curveball"
    assert query.batter_filter == "Pete Alonso"
    assert query.event_filter == {"home_run"}


def test_parse_statcast_relationship_query_for_player_curveball_homer_clips() -> None:
    query = parse_statcast_relationship_query("show me clips of Pete Alonso homeruns off curveballs in 2025", 2026)
    assert query is not None
    assert query.pitch_family == "curveball"
    assert query.batter_filter == "Pete Alonso"
    assert query.event_filter == {"home_run"}
    assert query.scope_label == "2025"
    assert query.wants_visuals is True


def test_parse_pitch_arsenal_query_for_slider_spin() -> None:
    query = parse_pitch_arsenal_query("which pitcher has the highest spin rate on their slider?", 2026)
    assert query is not None
    assert query.metric.key == "avg_spin"
    assert query.pitch_family.key == "slider"
    assert query.scope_label == "2026"
    assert query.sort_desc is True


def test_parse_pitch_arsenal_query_for_statcast_era() -> None:
    query = parse_pitch_arsenal_query("which pitcher has the highest spin rate on their slider in the Statcast era?", 2026)
    assert query is not None
    assert query.metric.key == "avg_spin"
    assert query.pitch_family.key == "slider"
    assert query.scope_label == "Statcast era"


def test_parse_pitch_arsenal_query_for_team_filtered_velocity() -> None:
    query = parse_pitch_arsenal_query("which Orioles pitcher has the highest velocity on their changeup this year?", 2026)
    assert query is not None
    assert query.metric.key == "avg_speed"
    assert query.pitch_family.key == "changeup"
    assert query.team_filter == "BAL"


def test_query_precomputed_statcast_relationship_for_velocity_threshold() -> None:
    with TemporaryDirectory() as temp_dir:
        database_path = Path(temp_dir) / "test.sqlite3"
        connection = get_connection(database_path)
        initialize_database(connection)
        replace_statcast_pitcher_games(
            connection,
            start_date="2025-03-27",
            end_date="2025-03-28",
            rows=[
                {
                    "season": 2025,
                    "game_date": "2025-03-27",
                    "game_pk": 1,
                    "pitcher_id": 100,
                    "pitcher_name": "Pitcher A",
                    "team": "ATH",
                    "team_name": "Athletics",
                    "opponent": "SEA",
                    "opponent_name": "Seattle Mariners",
                    "total_pitches": 30,
                    "max_release_speed": 101.5,
                    "pitches_100_plus": 12,
                    "pitches_101_plus": 3,
                },
                {
                    "season": 2025,
                    "game_date": "2025-03-28",
                    "game_pk": 2,
                    "pitcher_id": 200,
                    "pitcher_name": "Pitcher B",
                    "team": "SEA",
                    "team_name": "Seattle Mariners",
                    "opponent": "ATH",
                    "opponent_name": "Athletics",
                    "total_pitches": 25,
                    "max_release_speed": 100.7,
                    "pitches_100_plus": 9,
                    "pitches_101_plus": 0,
                },
            ],
        )
        query = parse_statcast_relationship_query("who threw the most pitches over 100mph last year?", 2026)
        assert query is not None
        rows = query_precomputed_statcast_relationship(connection, query)
        connection.close()
    assert rows is not None
    assert rows[0]["pitcher"] == "Pitcher A"
    assert rows[0]["count"] == 12


def test_query_precomputed_statcast_relationship_for_pitch_family_strikeouts() -> None:
    with TemporaryDirectory() as temp_dir:
        database_path = Path(temp_dir) / "test.sqlite3"
        connection = get_connection(database_path)
        initialize_database(connection)
        replace_statcast_pitcher_games(
            connection,
            start_date="2026-04-01",
            end_date="2026-04-02",
            rows=[
                {
                    "season": 2026,
                    "game_date": "2026-04-01",
                    "game_pk": 10,
                    "pitcher_id": 300,
                    "pitcher_name": "Pitcher C",
                    "team": "TOR",
                    "team_name": "Toronto Blue Jays",
                    "opponent": "BOS",
                    "opponent_name": "Boston Red Sox",
                    "total_pitches": 90,
                    "max_release_speed": 95.1,
                    "changeup_pitches": 22,
                    "changeup_strikeouts": 8,
                },
                {
                    "season": 2026,
                    "game_date": "2026-04-02",
                    "game_pk": 11,
                    "pitcher_id": 400,
                    "pitcher_name": "Pitcher D",
                    "team": "PHI",
                    "team_name": "Philadelphia Phillies",
                    "opponent": "ATL",
                    "opponent_name": "Atlanta Braves",
                    "total_pitches": 88,
                    "max_release_speed": 96.2,
                    "changeup_pitches": 18,
                    "changeup_strikeouts": 6,
                },
            ],
        )
        query = parse_statcast_relationship_query("Which pitcher has the most strikeouts throwing changeups this year?", 2026)
        assert query is not None
        rows = query_precomputed_statcast_relationship(connection, query)
        connection.close()
    assert rows is not None
    assert rows[0]["pitcher"] == "Pitcher C"
    assert rows[0]["count"] == 8
