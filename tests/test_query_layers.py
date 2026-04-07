import sqlite3
from pathlib import Path

from mlb_history_bot.chat import parse_fallback_query_response, should_attempt_nlp_fallback
from mlb_history_bot.contextual_performance import parse_count_split_query, parse_team_relationship_query
from mlb_history_bot.daily_lookup import parse_calendar_day_pitching_performance_query, parse_calendar_day_total_query
from mlb_history_bot.fielding_bible_search import extract_component_request, is_single_game_drs_question
from mlb_history_bot.home_run_robbery import extract_player_query
from mlb_history_bot.live_game_research import parse_player_day_home_run_query, parse_team_game_query
from mlb_history_bot.metric_gap import parse_metric_gap_query
from mlb_history_bot.metrics import MetricCatalog
from mlb_history_bot.models import CompiledContext, EvidenceSnippet
from mlb_history_bot.player_metric_lookup import parse_player_metric_query
from mlb_history_bot.player_situational_leaderboards import parse_player_situational_query
from mlb_history_bot.player_start_comparison import parse_player_start_comparison_query
from mlb_history_bot.player_season_analysis import parse_player_season_query
from mlb_history_bot.player_window_stats import parse_player_window_metric_query
from mlb_history_bot.provider_metrics import parse_provider_metric_query
from mlb_history_bot.query_utils import (
    extract_calendar_day_window,
    extract_date_window,
    extract_recent_window,
    extract_referenced_season,
    extract_first_n_games,
    question_requests_current_scope,
    question_mentions_specific_date_reference,
    question_mentions_yearless_month_day,
)
from mlb_history_bot.roster_comparison import parse_roster_comparison_query
from mlb_history_bot.season_comparison import parse_season_comparison_query
from mlb_history_bot.special_leaderboards import parse_award_opponent_gap_query, parse_birthday_home_run_query
from mlb_history_bot.statcast_event_leaderboards import parse_statcast_event_query
from mlb_history_bot.statcast_team_history import parse_statcast_team_window_query
from mlb_history_bot.team_season_compare import clean_team_phrase
from mlb_history_bot.team_history_rankings import parse_team_history_ranking_query


class FakeLiveClient:
    def teams(self, season: int):
        return [
            {
                "id": 121,
                "name": "New York Mets",
                "abbreviation": "NYM",
                "shortName": "NY Mets",
                "clubName": "Mets",
                "franchiseName": "New York",
                "locationName": "New York",
                "league": {"name": "National League"},
                "division": {"name": "National League East"},
            },
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
        ]

    def search_people(self, query: str):
        normalized = query.strip().casefold()
        if normalized == "cal raleigh":
            return [
                {
                    "fullName": "Cal Raleigh",
                    "active": True,
                    "isPlayer": True,
                    "isVerified": True,
                }
            ]
        if normalized == "pete alonso":
            return [
                {
                    "id": 624413,
                    "fullName": "Pete Alonso",
                    "active": True,
                    "isPlayer": True,
                    "isVerified": True,
                }
            ]
        if normalized == "yordan alvarez":
            return [
                {
                    "id": 670541,
                    "fullName": "Yordan Alvarez",
                    "active": True,
                    "isPlayer": True,
                    "isVerified": True,
                }
            ]
        return []


def test_extract_first_n_games() -> None:
    assert extract_first_n_games("show me the lowest team xBA through the first 10 games of a season") == 10


def test_extract_date_window_for_month_day_without_comma() -> None:
    window = extract_date_window("Show me Pete Alonso clips from June 1st 2022", 2026)
    assert window is not None
    assert window.start_date.isoformat() == "2022-06-01"


def test_extract_date_window_ignores_yearless_month_day_by_default() -> None:
    assert extract_date_window("which pitcher has the best stats on June 27th?", 2026) is None


def test_extract_calendar_day_window_for_yearless_month_day() -> None:
    window = extract_calendar_day_window("which pitcher has the best stats on June 27th?", 2026)
    assert window is not None
    assert window.label == "June 27"
    assert window.start_date.isoformat() == "2026-06-27"


def test_extract_recent_window_for_today() -> None:
    window = extract_recent_window("did the Red Sox win today?", 2026)
    assert window is not None
    assert window.label == "today"


def test_extract_date_window_for_slash_date() -> None:
    window = extract_date_window("what was Jo Adell's DRS on 04/03/2026", 2026)
    assert window is not None
    assert window.start_date.isoformat() == "2026-04-03"


def test_question_mentions_specific_date_reference() -> None:
    assert question_mentions_specific_date_reference("Show me Pete Alonso clips from June 1st 2022")
    assert question_mentions_specific_date_reference("what was Jo Adell's DRS on 04/03/2026")


def test_question_mentions_yearless_month_day() -> None:
    assert question_mentions_yearless_month_day("which pitcher has the best stats on June 27th?")
    assert not question_mentions_yearless_month_day("which pitcher had the best stats on June 27, 2025?")


def test_question_requests_current_scope() -> None:
    assert question_requests_current_scope("How is Cal Raleigh doing right now?")
    assert question_requests_current_scope("How has Cal Raleigh been this season?")
    assert not question_requests_current_scope("How good was Cal Raleigh in 2022?")


def test_extract_referenced_season_for_last_year() -> None:
    assert extract_referenced_season("who threw the most pitches over 100mph last year?", 2026) == 2025


def test_parse_season_comparison_query() -> None:
    query = parse_season_comparison_query(
        "how is the mets season looking so far compared to last year?",
        FakeLiveClient(),
        2026,
    )
    assert query is not None
    assert query.team.name == "New York Mets"
    assert query.current_season == 2026
    assert query.previous_season == 2025


def test_parse_metric_gap_query_for_xba() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_metric_gap_query(
        "show me the lowest team xBA through the first 10 games of a season",
        catalog,
    )
    assert query is not None
    assert query.metric_name == "xBA"
    assert query.first_n_games == 10
    assert query.wants_team_scope is True


def test_parse_metric_gap_query_for_war() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_metric_gap_query(
        "who's the current WAR leader and how do they compare to previous WAR leaders at this point in the season?",
        catalog,
    )
    assert query is not None
    assert query.metric_name == "WAR"
    assert query.wants_current_scope is True
    assert query.wants_comparison is True


def test_parse_metric_gap_query_for_fip_comparison() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_metric_gap_query(
        "compare Zack Wheeler FIP in 2024 to Jacob deGrom FIP in 2021",
        catalog,
    )
    assert query is not None
    assert query.metric_name == "FIP"
    assert query.wants_comparison is True


def test_parse_provider_metric_query_for_current_war() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_provider_metric_query(
        "who's the current WAR leader and how do they compare to previous WAR leaders at this point in the season?",
        catalog,
        2026,
    )
    assert query is not None
    assert query.metric.metric_name == "WAR"
    assert query.season == 2026
    assert query.wants_current is True
    assert query.wants_comparison is True


def test_parse_provider_metric_query_for_stuff_plus_with_min_starts() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_provider_metric_query(
        "Which Orioles pitcher has the highest Stuff+ through a minimum of 35 starts?",
        catalog,
        2026,
    )
    assert query is not None
    assert query.metric.metric_name == "Stuff+"
    assert query.group_preference == "pitching"
    assert query.team_filter == "BAL"
    assert query.minimum_starts == 35
    assert query.wants_current is False


def test_parse_provider_metric_query_for_fewest_hr_per_nine() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_provider_metric_query(
        "Which Orioles pitcher has the fewest HR/9 through a minimum of 35 starts?",
        catalog,
        2026,
    )
    assert query is not None
    assert query.metric.metric_name == "HR/9"
    assert query.sort_desc is False
    assert query.team_filter == "BAL"


def test_parse_count_split_query_for_on_count() -> None:
    query = parse_count_split_query("who has the lowest batting average ON 3-0 counts?")
    assert query is not None
    assert query.count_key == "3-0"
    assert query.relation == "on"
    assert query.metric_key == "ba"
    assert query.sort_desc is False
    assert query.is_valid_count is True


def test_parse_count_split_query_for_after_count() -> None:
    query = parse_count_split_query("who has the highest batting average after 0-2 counts?")
    assert query is not None
    assert query.count_key == "0-2"
    assert query.relation == "after"
    assert query.is_valid_count is True


def test_parse_count_split_query_for_following_count() -> None:
    query = parse_count_split_query("who has the lowest batting average following a 3-0 count?")
    assert query is not None
    assert query.count_key == "3-0"
    assert query.relation == "after"
    assert query.is_valid_count is True


def test_parse_count_split_query_for_invalid_count() -> None:
    query = parse_count_split_query("who has the lowest batting average on 0-3 counts?")
    assert query is not None
    assert query.count_key == "0-3"
    assert query.is_valid_count is False


def test_parse_team_relationship_query_for_former_team_homers() -> None:
    query = parse_team_relationship_query("Who has the most home runs against their former team?")
    assert query is not None
    assert query.relationship == "former"
    assert query.metric_key == "home_runs"
    assert query.sort_desc is True


def test_parse_team_relationship_query_for_future_team_offense() -> None:
    query = parse_team_relationship_query("Which player has performed the worst against a team they were later signed to?")
    assert query is not None
    assert query.relationship == "future"
    assert query.metric_key == "ops"
    assert query.sort_desc is False


def test_parse_player_window_metric_query_for_today_hits() -> None:
    query = parse_player_window_metric_query(
        "did Yordan Alvarez get any hits today?",
        FakeLiveClient(),
        2026,
    )
    assert query is not None
    assert query.player_name == "Yordan Alvarez"
    assert query.metric.key == "hits"
    assert query.date_window.label == "today"


def test_parse_player_situational_query_for_current_risp_ba() -> None:
    query = parse_player_situational_query(
        "which player has the worst batting average with RISP right now?",
        2026,
    )
    assert query is not None
    assert query.split.key == "risp"
    assert query.metric.key == "ba"
    assert query.season == 2026
    assert query.sort_desc is False


def test_parse_player_situational_query_for_offensive_summary() -> None:
    query = parse_player_situational_query(
        "which player has been the best offensively with runners on this season?",
        2026,
    )
    assert query is not None
    assert query.split.key == "men_on"
    assert query.metric.key == "ops"
    assert query.season == 2026


def test_parse_player_season_query_for_right_now_phrase() -> None:
    query = parse_player_season_query(
        "How is Cal Raleigh doing right now?",
        FakeLiveClient(),
        2026,
    )
    assert query is not None
    assert query.player_name == "Cal Raleigh"
    assert query.season == 2026


def test_parse_team_history_ranking_query_for_ba() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_team_history_ranking_query(
        "what team has the worst BA through the first 10 games of a season?",
        catalog,
    )
    assert query is not None
    assert query.metric.metric_name == "BA"
    assert query.first_n_games == 10
    assert query.descriptor == "worst"
    assert query.sort_desc is False


def test_parse_team_history_ranking_query_for_fewest_homers_per_game() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_team_history_ranking_query(
        "what team had the fewest home runs per game through the first 10 games of a season?",
        catalog,
    )
    assert query is not None
    assert query.metric.metric_name == "HR/Game"
    assert query.sort_desc is False
    assert query.descriptor == "lowest"


def test_parse_statcast_team_window_query_for_xba() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_statcast_team_window_query(
        "what team has the worst xBA through the first 10 games of a season?",
        catalog,
    )
    assert query is not None
    assert query.metric.metric_name == "xBA"
    assert query.first_n_games == 10
    assert query.descriptor == "worst"


def test_parse_statcast_team_window_query_for_greatest_xwoba() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_statcast_team_window_query(
        "what team had the greatest xwOBA through the first 10 games of a season?",
        catalog,
    )
    assert query is not None
    assert query.metric.metric_name == "xwOBA"
    assert query.sort_desc is True
    assert query.descriptor == "highest"


def test_parse_statcast_event_query_for_bat_speed_with_risp() -> None:
    query = parse_statcast_event_query(
        "Which player has the highest bat speed with RISP?",
        2026,
    )
    assert query is not None
    assert query.metric.key == "bat_speed"
    assert query.split_key == "risp"
    assert query.wants_player_aggregation is True
    assert query.scope_label == "2026"


def test_parse_statcast_event_query_for_oracle_park_hrs() -> None:
    query = parse_statcast_event_query(
        "what are the highest EV home runs to right field at Oracle Park in the Statcast era?",
        2026,
    )
    assert query is not None
    assert query.metric.key == "launch_speed"
    assert query.event_filter == "home_run"
    assert query.direction_filter == "right field"
    assert query.park_phrase == "Oracle Park"
    assert query.scope_label == "Statcast era"


def test_parse_statcast_relationship_query_for_last_year() -> None:
    from mlb_history_bot.statcast_relationships import parse_statcast_relationship_query

    query = parse_statcast_relationship_query("who threw the most pitches over 100mph last year?", 2026)
    assert query is not None
    assert query.start_season == 2025
    assert query.end_season == 2025
    assert query.scope_label == "2025"


def test_parse_statcast_event_query_for_greatest_ev_homers() -> None:
    query = parse_statcast_event_query(
        "show me the greatest EV home runs to right field at Oracle Park in the Statcast era",
        2026,
    )
    assert query is not None
    assert query.metric.key == "launch_speed"
    assert query.descriptor == "highest"
    assert query.sort_desc is True


def test_parse_calendar_day_total_query() -> None:
    query = parse_calendar_day_total_query("how many home runs were hit on August 1st?", 2026)
    assert query is not None
    assert query.metric.key == "home_runs"
    assert query.calendar_label == "August 1"


def test_parse_calendar_day_pitching_performance_query() -> None:
    query = parse_calendar_day_pitching_performance_query(
        "historically, which pitcher has the best stats on June 27th?",
        2026,
    )
    assert query is not None
    assert query.calendar_label == "June 27"
    assert query.sort_desc is True


def test_parse_team_game_query() -> None:
    query = parse_team_game_query("how did the giants play today", FakeLiveClient(), 2026)
    assert query is not None
    assert query.team.name == "San Francisco Giants"


def test_parse_player_day_home_run_query() -> None:
    query = parse_player_day_home_run_query("did Stanton homer today", 2026)
    assert query is not None
    assert query.player_query == "Stanton"


def test_parse_player_season_query_for_current_year_prompt() -> None:
    query = parse_player_season_query("how has Cal Raleigh been so far this year?", FakeLiveClient(), 2026)
    assert query is not None
    assert query.player_name == "Cal Raleigh"
    assert query.season == 2026


def test_parse_player_season_query_for_lowercase_stats_prompt() -> None:
    query = parse_player_season_query("cal raleigh 2026 stats", FakeLiveClient(), 2026)
    assert query is not None
    assert query.player_query == "Cal Raleigh"
    assert query.season == 2026


def test_parse_player_metric_query_for_oaa() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_player_metric_query("pete alonso OAA", FakeLiveClient(), catalog, 2026)
    assert query is not None
    assert query.player_name == "Pete Alonso"
    assert query.metric_name == "OAA"
    assert query.season == 2026


def test_parse_player_window_metric_query_for_home_runs() -> None:
    query = parse_player_window_metric_query("did yordan alvarez hit any home runs this week", FakeLiveClient(), 2026)
    assert query is not None
    assert query.player_name == "Yordan Alvarez"
    assert query.metric.key == "home_runs"
    assert query.date_window.label == "this week"


def test_parse_player_start_comparison_query_for_ops() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_player_start_comparison_query(
        "Compare Pete Alonso's OPS through the 2026 season with his previous season starts",
        FakeLiveClient(),
        catalog,
        2026,
    )
    assert query is not None
    assert query.player_name == "Pete Alonso"
    assert query.season == 2026
    assert query.metric.label == "OPS"


def test_parse_roster_comparison_query() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE lahman_teams (
            yearid TEXT,
            name TEXT,
            teamidretro TEXT,
            teamid TEXT,
            franchid TEXT,
            w TEXT,
            l TEXT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO lahman_teams (yearid, name, teamidretro, teamid, franchid, w, l)
        VALUES ('1979', 'Cleveland Indians', 'CLE', 'CLE', 'CLE', '81', '80')
        """
    )
    query = parse_roster_comparison_query(
        connection,
        "compare the 1979 indians roster to the 2026 giants roster",
        FakeLiveClient(),
        2026,
    )
    connection.close()
    assert query is not None
    assert query.left.display_name == "1979 Cleveland Indians"
    assert query.right.display_name == "2026 San Francisco Giants"


def test_clean_team_phrase_strips_pitching_staff_suffix() -> None:
    assert clean_team_phrase("Giants pitching staff") == "Giants"


def test_extract_player_query_for_lowercase_robbery_prompt() -> None:
    assert extract_player_query("show me jo adell home run robberies") == "Jo Adell"


def test_single_game_drs_question_detects_specific_date() -> None:
    assert is_single_game_drs_question("what was Jo Adell's DRS on 04/03/2026")


def test_home_run_robbery_alias_for_robbed_home_runs() -> None:
    component = extract_component_request("show me all robbed home runs so far in 2026")
    assert component is not None
    assert component.metric_name == "rHR"


def test_parse_birthday_home_run_query() -> None:
    query = parse_birthday_home_run_query("Which player has hit the most home runs on their birthday?")
    assert query is not None
    assert query.sort_desc is True


def test_parse_award_opponent_gap_query() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_award_opponent_gap_query(
        "Who has the best individual OPS against pitchers who have won the Cy Young Award?",
        catalog,
    )
    assert query is not None
    assert query.metric_name == "OPS"
    assert query.award_label == "Cy Young winners"


def test_parse_award_opponent_gap_query_for_mvp() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_award_opponent_gap_query(
        "Who has the best OPS against MVP winners?",
        catalog,
    )
    assert query is not None
    assert query.metric_name == "OPS"
    assert query.award_label == "MVP winners"


def test_parse_fallback_query_response() -> None:
    queries = parse_fallback_query_response(
        'Here you go: {"queries":["did Yordan Alvarez hit any home runs this week","Yordan Alvarez home runs this week"]}'
    )
    assert queries == [
        "did Yordan Alvarez hit any home runs this week",
        "Yordan Alvarez home runs this week",
    ]


def test_should_attempt_nlp_fallback_for_metric_gap_only() -> None:
    context = CompiledContext(
        classification="historical",
        question="unknown question",
        historical_evidence=[
            EvidenceSnippet(
                source="Metric Planner",
                title="xBA source gap",
                citation="test",
                summary="gap",
                payload={"analysis_type": "metric_source_gap"},
            )
        ],
    )
    assert should_attempt_nlp_fallback(context) is True


def test_should_not_attempt_nlp_fallback_when_grounded_evidence_exists() -> None:
    context = CompiledContext(
        classification="historical",
        question="known question",
        historical_evidence=[
            EvidenceSnippet(
                source="Retrosheet",
                title="daily total",
                citation="test",
                summary="grounded",
                payload={"analysis_type": "daily_lookup_total"},
            )
        ],
    )
    assert should_attempt_nlp_fallback(context) is False
