from __future__ import annotations

import re
from datetime import date
from typing import Any

from .config import Settings
from .cohort_metric_leaderboards import CohortMetricLeaderboardResearcher
from .cohort_timeline import parse_cohort_filter
from .contextual_performance import ContextualPerformanceResearcher
from .daily_lookup import DailyLookupResearcher, wants_historical_calendar_day_leaderboard
from .fielding_bible_search import (
    DrsResearchHelper,
    extract_component_request,
    is_drs_question,
    is_single_game_drs_question,
    wants_drs_data_lookup,
    wants_current_drs,
)
from .film_room_research import FilmRoomResearcher, parse_story_query
from .historical_team_analysis import HistoricalTeamAnalysisResearcher
from .historical_team_facts import HistoricalTeamFactsResearcher
from .home_run_robbery import HomeRunRobberyProxy
from .live import LiveStatsClient
from .live_game_research import LiveGameResearcher
from .manager_era_analysis import ManagerEraAnalysisResearcher
from .metric_gap import MetricGapResearcher
from .metrics import MetricCatalog
from .models import CompiledContext, EvidenceSnippet
from .pitching_staff_comparison import PitchingStaffComparisonResearcher
from .pitch_arsenal_leaderboards import PitchArsenalLeaderboardResearcher
from .player_metric_lookup import PlayerMetricLookupResearcher
from .player_team_relationships import PlayerTeamRelationshipResearcher
from .player_season_comparison import PlayerSeasonComparisonResearcher
from .player_start_comparison import PlayerStartComparisonResearcher
from .player_situational_leaderboards import PlayerSituationalLeaderboardResearcher
from .player_season_analysis import PlayerSeasonAnalysisResearcher
from .player_window_stats import PlayerWindowStatsResearcher
from .provider_metrics import ProviderMetricResearcher
from .query_utils import (
    extract_date_window,
    extract_name_candidates,
    question_mentions_specific_date_reference,
    question_mentions_yearless_month_day,
)
from .retrosheet_splits import RetrosheetSituationalResearcher
from .retrosheet_streaks import RetrosheetStreakResearcher
from .roster_comparison import RosterComparisonResearcher
from .salary_relationships import SalaryRelationshipResearcher
from .season_metric_leaderboards import SeasonMetricLeaderboardResearcher
from .season_comparison import SeasonComparisonResearcher
from .special_leaderboards import SpecialLeaderboardResearcher
from .sporty_research import SportyReplayFinder, wants_sporty_replay
from .statcast_event_leaderboards import StatcastEventResearcher
from .statcast_relationships import StatcastRelationshipResearcher
from .statcast_team_history import StatcastTeamHistoryResearcher
from .storage import (
    fetch_rows,
    get_connection,
    initialize_database,
    list_table_columns,
    resolve_column,
    search_document_chunks,
    table_exists,
)
from .team_evaluator import TeamEvaluator
from .team_evaluator import safe_int
from .team_history_rankings import TeamHistoryRankingResearcher
from .team_roster_leaders import TeamRosterLeaderResearcher
from .team_season_leaders import TeamSeasonLeaderResearcher
from .team_start_similarity import TeamStartSimilarityResearcher
from .team_season_compare import TeamSeasonComparisonResearcher


LIVE_HINTS = {
    "today",
    "tonight",
    "yesterday",
    "last night",
    "this week",
    "last week",
    "current",
    "latest",
    "right now",
    "this season",
    "this year",
    "so far",
    "standings",
    "score",
}
COMPARISON_HINTS = {"highest", "lowest", "best", "worst", "all time", "single-game", "single game", "career"}
VISUAL_HINTS = {"clip", "clips", "video", "videos", "replay", "replays", "highlight", "highlights", "watch"}

SUPPORTED_SINGLE_GAME_METRICS: dict[str, dict[str, Any]] = {
    "hits": {"table": "retrosheet_batting", "columns": ["h", "hits"], "label": "Hits"},
    "home runs": {"table": "retrosheet_batting", "columns": ["hr", "homeruns"], "label": "Home Runs"},
    "rbi": {"table": "retrosheet_batting", "columns": ["rbi"], "label": "RBI"},
    "strikeouts": {"table": "retrosheet_pitching", "columns": ["so", "strikeouts"], "label": "Pitcher Strikeouts"},
    "walks": {"table": "retrosheet_pitching", "columns": ["bb", "walks"], "label": "Pitcher Walks"},
    "putouts": {"table": "retrosheet_fielding", "columns": ["po", "putouts"], "label": "Putouts"},
    "assists": {"table": "retrosheet_fielding", "columns": ["a", "assists"], "label": "Assists"},
    "errors": {"table": "retrosheet_fielding", "columns": ["e", "errors"], "label": "Errors"},
}


class BaseballResearchEngine:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.catalog = MetricCatalog.load(settings.project_root)
        self.live_client = LiveStatsClient(settings)
        self.contextual_performance_researcher = ContextualPerformanceResearcher(settings)
        self.cohort_metric_researcher = CohortMetricLeaderboardResearcher(settings)
        self.drs_helper = DrsResearchHelper(settings)
        self.daily_lookup_researcher = DailyLookupResearcher(settings)
        self.film_room_researcher = FilmRoomResearcher(settings)
        self.historical_team_researcher = HistoricalTeamAnalysisResearcher(settings)
        self.historical_team_facts_researcher = HistoricalTeamFactsResearcher(settings)
        self.hr_robbery_proxy = HomeRunRobberyProxy(settings)
        self.live_game_researcher = LiveGameResearcher(settings)
        self.manager_era_researcher = ManagerEraAnalysisResearcher(settings)
        self.sporty_replay_finder = SportyReplayFinder(settings)
        self.salary_relationship_researcher = SalaryRelationshipResearcher(settings)
        self.season_metric_researcher = SeasonMetricLeaderboardResearcher(settings)
        self.team_evaluator = TeamEvaluator(settings)
        self.team_history_researcher = TeamHistoryRankingResearcher(settings)
        self.team_roster_leader_researcher = TeamRosterLeaderResearcher(settings)
        self.team_season_leader_researcher = TeamSeasonLeaderResearcher(settings)
        self.team_start_similarity_researcher = TeamStartSimilarityResearcher(settings)
        self.team_season_comparison_researcher = TeamSeasonComparisonResearcher(settings)
        self.retrosheet_situational_researcher = RetrosheetSituationalResearcher(settings)
        self.retrosheet_streak_researcher = RetrosheetStreakResearcher(settings)
        self.special_leaderboard_researcher = SpecialLeaderboardResearcher(settings)
        self.statcast_event_researcher = StatcastEventResearcher(settings)
        self.statcast_relationship_researcher = StatcastRelationshipResearcher(settings)
        self.statcast_team_history_researcher = StatcastTeamHistoryResearcher(settings)
        self.season_comparison_researcher = SeasonComparisonResearcher(settings)
        self.provider_metric_researcher = ProviderMetricResearcher(settings)
        self.metric_gap_researcher = MetricGapResearcher(settings)
        self.pitching_staff_comparison_researcher = PitchingStaffComparisonResearcher(settings)
        self.pitch_arsenal_leaderboard_researcher = PitchArsenalLeaderboardResearcher(settings)
        self.player_metric_lookup_researcher = PlayerMetricLookupResearcher(settings)
        self.player_team_relationship_researcher = PlayerTeamRelationshipResearcher(settings)
        self.player_season_comparison_researcher = PlayerSeasonComparisonResearcher(settings)
        self.player_start_comparison_researcher = PlayerStartComparisonResearcher(settings)
        self.player_situational_leaderboard_researcher = PlayerSituationalLeaderboardResearcher(settings)
        self.player_season_researcher = PlayerSeasonAnalysisResearcher(settings)
        self.player_window_stats_researcher = PlayerWindowStatsResearcher(settings)
        self.roster_comparison_researcher = RosterComparisonResearcher(settings)

    def compile_context(self, question: str) -> CompiledContext:
        classification = self._classify(question)
        context = CompiledContext(classification=classification, question=question)
        glossary_matches = self.catalog.search(question, limit=4)
        context.glossary_entries.extend(self._metric_snippets(glossary_matches))
        current_year = self.settings.live_season or date.today().year
        replay_focused = (
            parse_story_query(question, current_year) is not None
            or wants_sporty_replay(question, current_year)
            or (
                "home run robb" in question.lower()
                and any(token in question.lower() for token in ("show me", "clip", "clips", "video", "videos", "replay", "highlight"))
            )
        )
        date_specific_drs = is_drs_question(question) and question_mentions_specific_date_reference(question)
        comparison_focused = any(token in question.lower() for token in ("compare", "better than", "worse than", " vs ", " versus ", "how does"))
        yearless_month_day = question_mentions_yearless_month_day(question)
        visual_query = any(token in question.lower() for token in VISUAL_HINTS)
        cohort_filter_requested = parse_cohort_filter(question) is not None

        connection = get_connection(self.settings.database_path)
        initialize_database(connection)
        daily_lookup_snippet = None
        live_game_snippet = None
        historical_team_snippet = None
        historical_team_fact_snippet = None
        manager_era_snippet = None
        cohort_metric_snippet = None
        player_metric_snippet = None
        player_team_relationship_snippet = None
        player_season_comparison_snippet = None
        player_start_comparison_snippet = None
        player_season_snippet = None
        player_situational_snippet = None
        player_window_snippet = None
        team_eval_snippet = None
        team_roster_leader_snippet = None
        team_season_leader_snippet = None
        roster_comparison_snippet = None
        team_season_comparison_snippet = None
        team_start_similarity_snippet = None
        pitching_staff_comparison_snippet = None
        pitch_arsenal_snippet = None
        team_history_snippet = None
        team_split_history_snippet = None
        retrosheet_streak_snippet = None
        contextual_performance_snippet = None
        special_leaderboard_snippet = None
        salary_relationship_snippet = None
        season_metric_snippet = None
        statcast_event_snippet = None
        statcast_relationship_snippet = None
        statcast_team_history_snippet = None
        season_comparison_snippet = None
        provider_metric_snippet = None
        metric_gap_snippet = None
        try:
            season_comparison_snippet = self.season_comparison_researcher.build_snippet(connection, question)
            if season_comparison_snippet:
                context.live_evidence.append(season_comparison_snippet)
                context.classification = "hybrid"
            live_game_snippet = self.live_game_researcher.build_snippet(question)
            if live_game_snippet:
                context.live_evidence.append(live_game_snippet)
                context.classification = "live"
            player_metric_snippet = self.player_metric_lookup_researcher.build_snippet(question)
            if player_metric_snippet:
                target_collection = (
                    context.live_evidence
                    if player_metric_snippet.payload.get("mode") in {"live", "hybrid"}
                    else context.historical_evidence
                )
                target_collection.append(player_metric_snippet)
                context.classification = player_metric_snippet.payload.get("mode", context.classification)
            player_team_relationship_snippet = self.player_team_relationship_researcher.build_snippet(connection, question)
            if player_team_relationship_snippet:
                context.historical_evidence.append(player_team_relationship_snippet)
                context.classification = player_team_relationship_snippet.payload.get("mode", context.classification)
            player_window_snippet = self.player_window_stats_researcher.build_snippet(question)
            if player_window_snippet:
                context.live_evidence.append(player_window_snippet)
                context.classification = player_window_snippet.payload.get("mode", "live")
            player_situational_snippet = self.player_situational_leaderboard_researcher.build_snippet(question)
            if player_situational_snippet:
                target_collection = (
                    context.live_evidence
                    if player_situational_snippet.payload.get("mode") in {"live", "hybrid"}
                    else context.historical_evidence
                )
                target_collection.append(player_situational_snippet)
                context.classification = player_situational_snippet.payload.get("mode", context.classification)
            player_start_comparison_snippet = self.player_start_comparison_researcher.build_snippet(question)
            if player_start_comparison_snippet:
                target_collection = (
                    context.live_evidence
                    if player_start_comparison_snippet.payload.get("mode") in {"live", "hybrid"}
                    else context.historical_evidence
                )
                target_collection.append(player_start_comparison_snippet)
                context.classification = player_start_comparison_snippet.payload.get("mode", context.classification)
            player_season_comparison_snippet = self.player_season_comparison_researcher.build_snippet(connection, question)
            if player_season_comparison_snippet:
                target_collection = (
                    context.live_evidence
                    if player_season_comparison_snippet.payload.get("mode") in {"live", "hybrid"}
                    else context.historical_evidence
                )
                target_collection.append(player_season_comparison_snippet)
                context.classification = player_season_comparison_snippet.payload.get("mode", context.classification)
            player_season_snippet = (
                None
                if player_start_comparison_snippet is not None or player_season_comparison_snippet is not None
                else self.player_season_researcher.build_snippet(question)
            )
            if player_season_snippet:
                context.live_evidence.append(player_season_snippet)
                context.classification = player_season_snippet.payload.get("mode", "live")
            historical_team_snippet = self.historical_team_researcher.build_snippet(connection, question)
            if historical_team_snippet:
                context.historical_evidence.append(historical_team_snippet)
            historical_team_fact_snippet = self.historical_team_facts_researcher.build_snippet(connection, question)
            if historical_team_fact_snippet:
                context.historical_evidence.append(historical_team_fact_snippet)
                context.classification = historical_team_fact_snippet.payload.get("mode", context.classification)
            cohort_metric_snippet = self.cohort_metric_researcher.build_snippet(connection, question)
            if cohort_metric_snippet:
                context.historical_evidence.append(cohort_metric_snippet)
                context.classification = cohort_metric_snippet.payload.get("mode", context.classification)
            manager_era_snippet = None if cohort_metric_snippet is not None else self.manager_era_researcher.build_snippet(connection, question)
            if manager_era_snippet:
                context.historical_evidence.append(manager_era_snippet)
                context.classification = manager_era_snippet.payload.get("mode", context.classification)
            pitching_staff_comparison_snippet = self.pitching_staff_comparison_researcher.build_snippet(connection, question)
            if pitching_staff_comparison_snippet:
                target_collection = (
                    context.live_evidence
                    if pitching_staff_comparison_snippet.payload.get("mode") == "hybrid"
                    else context.historical_evidence
                )
                target_collection.append(pitching_staff_comparison_snippet)
                context.classification = pitching_staff_comparison_snippet.payload.get("mode", context.classification)
            pitch_arsenal_snippet = self.pitch_arsenal_leaderboard_researcher.build_snippet(question)
            if pitch_arsenal_snippet:
                target_collection = (
                    context.live_evidence
                    if pitch_arsenal_snippet.payload.get("mode") in {"live", "hybrid"}
                    else context.historical_evidence
                )
                target_collection.append(pitch_arsenal_snippet)
                context.classification = pitch_arsenal_snippet.payload.get("mode", context.classification)
            roster_comparison_snippet = self.roster_comparison_researcher.build_snippet(connection, question)
            if roster_comparison_snippet:
                target_collection = (
                    context.live_evidence
                    if roster_comparison_snippet.payload.get("mode") == "hybrid"
                    else context.historical_evidence
                )
                target_collection.append(roster_comparison_snippet)
                context.classification = roster_comparison_snippet.payload.get("mode", context.classification)
            team_roster_leader_snippet = self.team_roster_leader_researcher.build_snippet(question)
            if team_roster_leader_snippet:
                context.live_evidence.append(team_roster_leader_snippet)
                context.classification = team_roster_leader_snippet.payload.get("mode", "live")
            team_season_leader_snippet = self.team_season_leader_researcher.build_snippet(connection, question)
            if team_season_leader_snippet:
                target_collection = (
                    context.live_evidence
                    if team_season_leader_snippet.payload.get("mode") in {"live", "hybrid"}
                    else context.historical_evidence
                )
                target_collection.append(team_season_leader_snippet)
                context.classification = team_season_leader_snippet.payload.get("mode", context.classification)
            team_eval_snippet = None if manager_era_snippet is not None else self.team_evaluator.build_snippet(connection, question)
            if team_eval_snippet:
                context.live_evidence.append(team_eval_snippet)
                context.classification = "live"
            team_season_comparison_snippet = self.team_season_comparison_researcher.build_snippet(connection, question)
            if team_season_comparison_snippet:
                target_collection = (
                    context.live_evidence
                    if team_season_comparison_snippet.payload.get("mode") == "hybrid"
                    else context.historical_evidence
                )
                target_collection.append(team_season_comparison_snippet)
                context.classification = team_season_comparison_snippet.payload.get("mode", context.classification)
            team_start_similarity_snippet = self.team_start_similarity_researcher.build_snippet(connection, question)
            if team_start_similarity_snippet:
                target_collection = (
                    context.live_evidence
                    if team_start_similarity_snippet.payload.get("mode") == "hybrid"
                    else context.historical_evidence
                )
                target_collection.append(team_start_similarity_snippet)
                context.classification = team_start_similarity_snippet.payload.get("mode", context.classification)
            contextual_performance_snippet = self.contextual_performance_researcher.build_snippet(connection, question)
            if contextual_performance_snippet:
                context.historical_evidence.append(contextual_performance_snippet)
            special_leaderboard_snippet = self.special_leaderboard_researcher.build_snippet(connection, question)
            if special_leaderboard_snippet:
                context.historical_evidence.append(special_leaderboard_snippet)
            salary_relationship_snippet = self.salary_relationship_researcher.build_snippet(connection, question)
            if salary_relationship_snippet:
                context.historical_evidence.append(salary_relationship_snippet)
                context.classification = salary_relationship_snippet.payload.get("mode", context.classification)
            team_split_history_snippet = self.retrosheet_situational_researcher.build_snippet(connection, question)
            if team_split_history_snippet:
                context.historical_evidence.append(team_split_history_snippet)
            retrosheet_streak_snippet = self.retrosheet_streak_researcher.build_snippet(connection, question)
            if retrosheet_streak_snippet:
                context.historical_evidence.append(retrosheet_streak_snippet)
            team_history_snippet = self.team_history_researcher.build_snippet(connection, question)
            if team_history_snippet and team_split_history_snippet is None and retrosheet_streak_snippet is None:
                context.historical_evidence.append(team_history_snippet)
            if not (cohort_filter_requested and cohort_metric_snippet is None):
                season_metric_snippet = self.season_metric_researcher.build_snippet(connection, question)
                if season_metric_snippet:
                    target_collection = (
                        context.live_evidence
                        if season_metric_snippet.payload.get("mode") in {"live", "hybrid"}
                        else context.historical_evidence
                    )
                    target_collection.append(season_metric_snippet)
                    context.classification = season_metric_snippet.payload.get("mode", context.classification)
            statcast_event_snippet = self.statcast_event_researcher.build_snippet(connection, question)
            if statcast_event_snippet:
                target_collection = (
                    context.live_evidence
                    if statcast_event_snippet.payload.get("mode") in {"live", "hybrid"}
                    else context.historical_evidence
                )
                target_collection.append(statcast_event_snippet)
                context.classification = statcast_event_snippet.payload.get("mode", context.classification)
            statcast_relationship_snippet = self.statcast_relationship_researcher.build_snippet(question)
            if statcast_relationship_snippet:
                target_collection = (
                    context.live_evidence
                    if statcast_relationship_snippet.payload.get("mode") in {"live", "hybrid"}
                    else context.historical_evidence
                )
                target_collection.append(statcast_relationship_snippet)
                context.classification = statcast_relationship_snippet.payload.get("mode", context.classification)
            statcast_team_history_snippet = self.statcast_team_history_researcher.build_snippet(connection, question)
            if statcast_team_history_snippet:
                context.historical_evidence.append(statcast_team_history_snippet)
            daily_lookup_snippet = self.daily_lookup_researcher.build_snippet(connection, question)
            if daily_lookup_snippet:
                target_collection = (
                    context.live_evidence
                    if daily_lookup_snippet.payload.get("mode") == "live"
                    else context.historical_evidence
                )
                target_collection.append(daily_lookup_snippet)
                context.classification = daily_lookup_snippet.payload.get("mode", context.classification)
            provider_metric_snippet = (
                None
                if statcast_event_snippet is not None
                or statcast_relationship_snippet is not None
                or pitch_arsenal_snippet is not None
                or team_roster_leader_snippet is not None
                or team_season_leader_snippet is not None
                or player_team_relationship_snippet is not None
                or player_situational_snippet is not None
                or special_leaderboard_snippet is not None
                or contextual_performance_snippet is not None
                or cohort_metric_snippet is not None
                or manager_era_snippet is not None
                or season_metric_snippet is not None
                or salary_relationship_snippet is not None
                or retrosheet_streak_snippet is not None
                else self.provider_metric_researcher.build_snippet(question)
            )
            if provider_metric_snippet:
                target_collection = (
                    context.live_evidence
                    if provider_metric_snippet.payload.get("wants_current")
                    else context.historical_evidence
                )
                target_collection.append(provider_metric_snippet)
            metric_gap_snippet = (
                None
                if player_metric_snippet
                or provider_metric_snippet
                or statcast_team_history_snippet
                or statcast_event_snippet
                or statcast_relationship_snippet
                or pitch_arsenal_snippet
                or team_roster_leader_snippet
                or team_season_leader_snippet
                or player_team_relationship_snippet
                or season_metric_snippet
                or player_situational_snippet
                or team_split_history_snippet
                or retrosheet_streak_snippet
                or special_leaderboard_snippet
                or contextual_performance_snippet
                or salary_relationship_snippet
                or cohort_metric_snippet
                or manager_era_snippet
                else self.metric_gap_researcher.build_snippet(question)
            )
            if metric_gap_snippet:
                context.historical_evidence.append(metric_gap_snippet)
            if is_drs_question(question) and wants_drs_data_lookup(question) and not date_specific_drs:
                context.historical_evidence.extend(self.drs_helper.historical_snippets(connection, question))
            if not replay_focused and live_game_snippet is None and not date_specific_drs:
                if (
                    not comparison_focused
                    and
                    historical_team_snippet is None
                    and historical_team_fact_snippet is None
                    and manager_era_snippet is None
                    and player_metric_snippet is None
                    and player_team_relationship_snippet is None
                    and player_season_comparison_snippet is None
                    and player_start_comparison_snippet is None
                    and player_situational_snippet is None
                    and player_window_snippet is None
                    and player_season_snippet is None
                    and team_roster_leader_snippet is None
                    and team_season_leader_snippet is None
                    and roster_comparison_snippet is None
                    and pitch_arsenal_snippet is None
                    and retrosheet_streak_snippet is None
                    and contextual_performance_snippet is None
                    and cohort_metric_snippet is None
                    and special_leaderboard_snippet is None
                    and season_metric_snippet is None
                    and salary_relationship_snippet is None
                    and statcast_event_snippet is None
                    and statcast_relationship_snippet is None
                    and not (yearless_month_day and visual_query)
                ):
                    context.historical_evidence.extend(self._player_or_team_summaries(connection, question))
                context.historical_evidence.extend(self._single_game_leaderboard_snippets(connection, question))
                context.historical_evidence.extend(self._document_snippets(connection, question))
        finally:
            connection.close()

        component_request = extract_component_request(question)
        story_snippets = self.film_room_researcher.build_snippets(question)
        if story_snippets:
            context.replay_evidence.extend(story_snippets)
        hr_robbery_snippets: list[EvidenceSnippet] = []
        if component_request and component_request.metric_name == "rHR":
            hr_robbery_snippets = self.hr_robbery_proxy.build_snippets(question)
            context.replay_evidence.extend(hr_robbery_snippets)
        elif date_specific_drs and extract_name_candidates(question):
            hr_robbery_snippets = self.hr_robbery_proxy.build_snippets(question)
            context.replay_evidence.extend(hr_robbery_snippets)

        sporty_replay_snippets: list[EvidenceSnippet] = []
        if (
            not story_snippets
            and daily_lookup_snippet is None
            and live_game_snippet is None
            and historical_team_snippet is None
            and historical_team_fact_snippet is None
            and manager_era_snippet is None
            and player_metric_snippet is None
            and player_season_comparison_snippet is None
            and player_start_comparison_snippet is None
            and player_situational_snippet is None
            and player_window_snippet is None
            and player_season_snippet is None
            and team_roster_leader_snippet is None
            and team_season_leader_snippet is None
            and team_eval_snippet is None
            and pitch_arsenal_snippet is None
            and roster_comparison_snippet is None
            and team_season_comparison_snippet is None
            and team_start_similarity_snippet is None
            and pitching_staff_comparison_snippet is None
            and team_history_snippet is None
            and team_split_history_snippet is None
            and retrosheet_streak_snippet is None
            and contextual_performance_snippet is None
            and cohort_metric_snippet is None
            and season_metric_snippet is None
            and special_leaderboard_snippet is None
            and salary_relationship_snippet is None
            and statcast_event_snippet is None
            and statcast_relationship_snippet is None
            and statcast_team_history_snippet is None
            and season_comparison_snippet is None
            and provider_metric_snippet is None
            and metric_gap_snippet is None
            and not hr_robbery_snippets
        ):
            sporty_replay_snippets = self.sporty_replay_finder.build_snippets(question)
            context.replay_evidence.extend(sporty_replay_snippets)

        if (
            context.classification in {"live", "hybrid"}
            and not story_snippets
            and daily_lookup_snippet is None
            and live_game_snippet is None
            and historical_team_snippet is None
            and historical_team_fact_snippet is None
            and manager_era_snippet is None
            and player_metric_snippet is None
            and player_season_comparison_snippet is None
            and player_start_comparison_snippet is None
            and player_situational_snippet is None
            and player_window_snippet is None
            and player_season_snippet is None
            and team_roster_leader_snippet is None
            and team_season_leader_snippet is None
            and team_eval_snippet is None
            and pitch_arsenal_snippet is None
            and roster_comparison_snippet is None
            and team_season_comparison_snippet is None
            and team_start_similarity_snippet is None
            and pitching_staff_comparison_snippet is None
            and team_history_snippet is None
            and team_split_history_snippet is None
            and retrosheet_streak_snippet is None
            and contextual_performance_snippet is None
            and cohort_metric_snippet is None
            and season_metric_snippet is None
            and special_leaderboard_snippet is None
            and salary_relationship_snippet is None
            and statcast_event_snippet is None
            and statcast_relationship_snippet is None
            and statcast_team_history_snippet is None
            and season_comparison_snippet is None
            and provider_metric_snippet is None
            and metric_gap_snippet is None
            and not sporty_replay_snippets
            and not hr_robbery_snippets
            and not replay_focused
        ):
            if is_drs_question(question):
                if wants_drs_data_lookup(question):
                    context.live_evidence.extend(self.drs_helper.live_snippets(question))
            else:
                context.live_evidence.extend(self._live_snippets(question))

        if is_drs_question(question):
            if component_request and component_request.column_name is None and component_request.notes:
                context.warnings.append(component_request.notes)
            if component_request and component_request.metric_name == "rHR" and any(
                snippet.source == "rHR Proxy" for snippet in context.all_snippets()
            ):
                context.warnings.append(
                    "The rHR value below is a proxy, not official SIS rHR. It counts Savant-verified home run robberies "
                    f"at +{1.6:.1f} runs each based on public SIS commentary."
                )
            if is_single_game_drs_question(question):
                context.warnings.append(
                    "Exact single-game DRS is not exposed by the public Fielding Bible/SIS season leaderboards. "
                    "This bot can cite official season-level DRS, but single-game DRS still needs a clearly labeled proxy."
                )
            elif (
                wants_drs_data_lookup(question)
                and (component_request is None or component_request.metric_name != "rHR")
                and not any(
                snippet.source.startswith("Fielding Bible") for snippet in context.all_snippets()
                )
            ):
                context.warnings.append(
                    "Exact DRS answers require synced Fielding Bible/SIS data. Run "
                    "`python -m mlb_history_bot sync-drs` or `python -m mlb_history_bot prepare --with-drs` "
                    "to load official season leaderboards."
                )
        if not context.all_snippets():
            if yearless_month_day and visual_query:
                context.warnings.append(
                    "Yearless month-day replay requests are now treated as all-history calendar-date searches. "
                    "Replay aggregation across every season for a calendar date is not built yet, so add a year "
                    "if you want a single-date clip set."
                )
            context.warnings.append(
                "The local database does not yet contain enough evidence for a grounded answer. Bootstrap and ingest the datasets first."
            )
        return context

    def _classify(self, question: str) -> str:
        lowered = question.lower()
        current_year = self.settings.live_season or date.today().year
        if wants_historical_calendar_day_leaderboard(question, current_year):
            return "historical"
        wants_live = any(hint in lowered for hint in LIVE_HINTS)
        if extract_year(question) == current_year and any(
            token in lowered for token in ("stats", "stat line", "performance", "roster", "season")
        ):
            wants_live = True
        date_window = extract_date_window(question, current_year)
        if date_window is not None:
            if date_window.label in {"today", "yesterday", "this week", "last week"}:
                wants_live = True
            elif date_window.end_date.year >= current_year:
                wants_live = True
        if is_drs_question(question) and wants_current_drs(question, current_year):
            wants_live = True
        wants_comparison = any(hint in lowered for hint in COMPARISON_HINTS)
        if wants_live and wants_comparison:
            return "hybrid"
        if wants_live:
            return "live"
        return "historical"

    def _metric_snippets(self, metrics) -> list[EvidenceSnippet]:
        snippets: list[EvidenceSnippet] = []
        for metric in metrics:
            snippets.append(
                EvidenceSnippet(
                    source="Sabermetric Catalog",
                    title=metric.name,
                    citation="Curated sabermetric catalog",
                    summary=f"{metric.definition} Formula: {metric.formula} Notes: {metric.notes}",
                    payload={
                        "category": metric.category,
                        "historical_support": metric.historical_support,
                        "live_support": metric.live_support,
                        "exact_formula_public": metric.exact_formula_public,
                    },
                )
            )
        return snippets

    def _player_or_team_summaries(self, connection, question: str) -> list[EvidenceSnippet]:
        snippets: list[EvidenceSnippet] = []
        for candidate in extract_name_candidates(question):
            player_snippet = self._player_summary(connection, candidate)
            if player_snippet:
                snippets.append(player_snippet)
        year = extract_year(question)
        if year is not None:
            team_snippet = self._team_summary(connection, question, year)
            if team_snippet:
                snippets.append(team_snippet)
        return snippets[:3]

    def _player_summary(self, connection, player_name: str) -> EvidenceSnippet | None:
        if not table_exists(connection, "lahman_people"):
            return None
        candidate_rows = connection.execute(
            """
            SELECT *
            FROM lahman_people
            WHERE lower(trim(coalesce(namefirst, '') || ' ' || coalesce(namelast, ''))) LIKE ?
               OR lower(coalesce(namegiven, '')) LIKE ?
            ORDER BY
                CASE
                    WHEN lower(trim(coalesce(namefirst, '') || ' ' || coalesce(namelast, ''))) = ? THEN 0
                    ELSE 1
                END,
                finalgame DESC
            LIMIT 25
            """,
            (f"%{player_name.lower()}%", f"%{player_name.lower()}%", player_name.lower()),
        ).fetchall()
        if not candidate_rows:
            return None
        row = max(candidate_rows, key=lambda item: self._player_profile_rank(connection, item, player_name))

        player_id = row["playerid"]
        latest_team = self._latest_player_team(connection, player_id)
        primary_position = self._latest_player_position(connection, player_id)
        batting_summary = self._aggregate_player_totals(
            connection,
            "lahman_batting",
            player_id,
            {
                "hits": "h",
                "home_runs": "hr",
                "runs_batted_in": "rbi",
                "stolen_bases": "sb",
            },
        )
        pitching_summary = self._aggregate_player_totals(
            connection,
            "lahman_pitching",
            player_id,
            {
                "wins": "w",
                "losses": "l",
                "strikeouts": "so",
                "saves": "sv",
                "outs_recorded": "ipouts",
            },
        )
        parts: list[str] = []
        full_name = f"{row['namefirst']} {row['namelast']}".strip()
        handedness_bits: list[str] = []
        bats = str(row["bats"] or "").strip()
        throws = str(row["throws"] or "").strip()
        if bats:
            handedness_bits.append(
                {
                    "L": "left-handed hitter",
                    "R": "right-handed hitter",
                    "S": "switch-hitter",
                    "B": "switch-hitter",
                }.get(bats.upper(), f"bats {bats.upper()}")
            )
        if throws:
            handedness_bits.append(
                {
                    "L": "throws left",
                    "R": "throws right",
                }.get(throws.upper(), f"throws {throws.upper()}")
            )
        identity_parts: list[str] = []
        if handedness_bits:
            identity_parts.append(", ".join(filter(None, handedness_bits)))
        if primary_position:
            identity_parts.append(primary_position)
        if latest_team:
            identity_parts.append(f"latest imported team: {latest_team}")
        if identity_parts:
            parts.append(f"{full_name} is an MLB player listed in Lahman as a {'; '.join(identity_parts)}.")
        birth_parts: list[str] = []
        birth_city = str(row["birthcity"] or "").strip() if "birthcity" in row.keys() else ""
        birth_state = str(row["birthstate"] or "").strip() if "birthstate" in row.keys() else ""
        birth_country = str(row["birthcountry"] or "").strip() if "birthcountry" in row.keys() else ""
        birth_year = str(row["birthyear"] or "").strip() if "birthyear" in row.keys() else ""
        if birth_city:
            birth_parts.append(birth_city)
        if birth_state:
            birth_parts.append(birth_state)
        if birth_country:
            birth_parts.append(birth_country)
        if birth_parts or birth_year:
            location = ", ".join(part for part in birth_parts if part)
            if birth_year and location:
                parts.append(f"Born in {location} in {birth_year}.")
            elif location:
                parts.append(f"Born in {location}.")
            elif birth_year:
                parts.append(f"Born in {birth_year}.")
        debut = str(row["debut"] or "").strip() if "debut" in row.keys() else ""
        final_game = str(row["finalgame"] or "").strip() if "finalgame" in row.keys() else ""
        if debut:
            parts.append(f"MLB debut in imported records: {debut}.")
        if final_game:
            parts.append(f"Most recent game in imported records: {final_game}.")
        if batting_summary:
            parts.append(
                "Batting totals: "
                f"{int(batting_summary.get('hits', 0))} H, "
                f"{int(batting_summary.get('home_runs', 0))} HR, "
                f"{int(batting_summary.get('runs_batted_in', 0))} RBI."
            )
        if pitching_summary:
            innings = round(float(pitching_summary.get("outs_recorded", 0)) / 3.0, 1)
            parts.append(
                "Pitching totals: "
                f"{int(pitching_summary.get('wins', 0))}-{int(pitching_summary.get('losses', 0))}, "
                f"{int(pitching_summary.get('strikeouts', 0))} SO, "
                f"{innings} IP."
            )
        summary = " ".join(parts) if parts else "Player found in Lahman with no imported totals yet."
        return EvidenceSnippet(
            source="Lahman Database",
            title=f"{full_name} career summary",
            citation="Lahman People/Batting/Pitching tables",
            summary=summary,
            payload={
                "player_id": player_id,
                "debut": row["debut"] if "debut" in row.keys() else "",
                "final_game": row["finalgame"] if "finalgame" in row.keys() else "",
            },
        )

    def _player_profile_rank(self, connection, row, requested_name: str) -> tuple[int, int, int, int, str]:
        full_name = " ".join(
            part for part in (str(row["namefirst"] or "").strip(), str(row["namelast"] or "").strip()) if part
        ).lower()
        exact_match = 1 if full_name == requested_name.lower().strip() else 0
        latest_season, total_games = self._player_activity_score(connection, str(row["playerid"] or ""))
        active_hint = 1 if latest_season >= (self.settings.live_season or date.today().year) - 1 else 0
        final_game = str(row["finalgame"] or "")
        return (exact_match, active_hint, latest_season, total_games, final_game)

    def _latest_player_team(self, connection, player_id: str) -> str:
        candidate_tables = [
            ("lahman_batting", "yearid", "teamid"),
            ("lahman_pitching", "yearid", "teamid"),
            ("lahman_fielding", "yearid", "teamid"),
        ]
        best_year = -1
        best_team = ""
        for table_name, year_column, team_column in candidate_tables:
            if not table_exists(connection, table_name):
                continue
            row = connection.execute(
                f"""
                SELECT CAST({year_column} AS INTEGER) AS season, {team_column} AS team_id
                FROM {table_name}
                WHERE playerid = ?
                ORDER BY CAST({year_column} AS INTEGER) DESC
                LIMIT 1
                """,
                (player_id,),
            ).fetchone()
            if row is None:
                continue
            season = int(row["season"] or 0)
            if season > best_year:
                best_year = season
                best_team = str(row["team_id"] or "")
        if not best_team:
            return ""
        if table_exists(connection, "lahman_teams") and best_year > 0:
            team_row = connection.execute(
                """
                SELECT name
                FROM lahman_teams
                WHERE CAST(yearid AS INTEGER) = ?
                  AND lower(teamid) = ?
                LIMIT 1
                """,
                (best_year, best_team.lower()),
            ).fetchone()
            if team_row is not None and str(team_row["name"] or "").strip():
                return f"{team_row['name']} ({best_year})"
        return f"{best_team.upper()} ({best_year})" if best_year > 0 else best_team.upper()

    def _latest_player_position(self, connection, player_id: str) -> str:
        if not table_exists(connection, "lahman_fielding"):
            return ""
        row = connection.execute(
            """
            SELECT pos, SUM(CAST(COALESCE(g, '0') AS INTEGER)) AS games
            FROM lahman_fielding
            WHERE playerid = ?
            GROUP BY pos
            ORDER BY games DESC, pos ASC
            LIMIT 1
            """,
            (player_id,),
        ).fetchone()
        if row is None:
            if table_exists(connection, "lahman_pitching"):
                pitch_row = connection.execute(
                    """
                    SELECT SUM(CAST(COALESCE(g, '0') AS INTEGER)) AS games
                    FROM lahman_pitching
                    WHERE playerid = ?
                    """,
                    (player_id,),
                ).fetchone()
                if pitch_row is not None and float(pitch_row["games"] or 0) > 0:
                    return "pitcher"
            return ""
        position_code = str(row["pos"] or "").strip().upper()
        return {
            "P": "pitcher",
            "C": "catcher",
            "1B": "first baseman",
            "2B": "second baseman",
            "3B": "third baseman",
            "SS": "shortstop",
            "LF": "left fielder",
            "CF": "center fielder",
            "RF": "right fielder",
            "OF": "outfielder",
            "DH": "designated hitter",
        }.get(position_code, position_code.lower())

    def _aggregate_player_totals(
        self,
        connection,
        table_name: str,
        player_id: str,
        metric_columns: dict[str, str],
    ) -> dict[str, float]:
        if not table_exists(connection, table_name):
            return {}
        existing_columns = {column.lower() for column in list_table_columns(connection, table_name)}
        expressions = []
        aliases = []
        for alias, column in metric_columns.items():
            if column.lower() in existing_columns:
                expressions.append(f"SUM(CAST({column} AS REAL)) AS {alias}")
                aliases.append(alias)
        if not expressions:
            return {}
        row = connection.execute(
            f"SELECT {', '.join(expressions)} FROM {table_name} WHERE playerid = ?",
            (player_id,),
        ).fetchone()
        if row is None:
            return {}
        return {alias: float(row[alias] or 0) for alias in aliases}

    def _player_activity_score(self, connection, player_id: str) -> tuple[int, int]:
        latest_season = 0
        total_games = 0
        candidate_tables = [
            ("lahman_batting", "yearid", "g"),
            ("lahman_pitching", "yearid", "g"),
            ("lahman_fielding", "yearid", "g"),
        ]
        for table_name, year_column, games_column in candidate_tables:
            if not table_exists(connection, table_name):
                continue
            row = connection.execute(
                f"""
                SELECT
                    MAX(CAST(COALESCE({year_column}, '0') AS INTEGER)) AS latest_year,
                    SUM(CAST(COALESCE({games_column}, '0') AS INTEGER)) AS games
                FROM {table_name}
                WHERE playerid = ?
                """,
                (player_id,),
            ).fetchone()
            if row is None:
                continue
            latest_season = max(latest_season, safe_int(row["latest_year"]) or 0)
            total_games += safe_int(row["games"]) or 0
        return latest_season, total_games

    def _team_summary(self, connection, question: str, year: int) -> EvidenceSnippet | None:
        if not table_exists(connection, "lahman_teams"):
            return None
        lowered = question.lower()
        row = connection.execute(
            """
            SELECT *
            FROM lahman_teams
            WHERE yearid = ?
              AND (
                lower(coalesce(name, '')) LIKE ?
                OR lower(coalesce(teamid, '')) LIKE ?
                OR lower(coalesce(franchid, '')) LIKE ?
              )
            ORDER BY w DESC, l ASC
            LIMIT 1
            """,
            (year, f"%{lowered}%", f"%{lowered}%", f"%{lowered}%"),
        ).fetchone()
        if row is None:
            return None
        return EvidenceSnippet(
            source="Lahman Database",
            title=f"{row['name']} {row['yearid']}",
            citation="Lahman Teams table",
            summary=f"{row['name']} finished {row['w']}-{row['l']} in {row['yearid']} with division rank {row['rank']}.",
            payload={"teamid": row["teamid"], "franchid": row["franchid"]},
        )

    def _single_game_leaderboard_snippets(self, connection, question: str) -> list[EvidenceSnippet]:
        lowered = question.lower()
        snippets: list[EvidenceSnippet] = []
        for metric_name, metadata in SUPPORTED_SINGLE_GAME_METRICS.items():
            if metric_name not in lowered:
                continue
            table_name = metadata["table"]
            if not table_exists(connection, table_name):
                continue
            stat_column = resolve_column(connection, table_name, metadata["columns"])
            if stat_column is None:
                continue
            player_column = resolve_column(connection, table_name, ["playername", "name", "player", "playerid"])
            date_column = resolve_column(connection, table_name, ["date", "gamedate", "game_date"])
            team_column = resolve_column(connection, table_name, ["team", "teamid"])
            selected_columns = [stat_column]
            aliases = ["value"]
            if player_column:
                selected_columns.append(player_column)
                aliases.append("player_name")
            if date_column:
                selected_columns.append(date_column)
                aliases.append("game_date")
            if team_column:
                selected_columns.append(team_column)
                aliases.append("team_name")
            select_sql = ", ".join(
                f"{column} AS {alias}" for column, alias in zip(selected_columns, aliases, strict=False)
            )
            rows = fetch_rows(
                connection,
                f"""
                SELECT {select_sql}
                FROM {table_name}
                ORDER BY CAST({stat_column} AS REAL) DESC
                LIMIT 5
                """,
            )
            if not rows:
                continue
            lines = []
            for index, row in enumerate(rows, start=1):
                player_label = row["player_name"] if "player_name" in row.keys() else "Unknown player"
                date_label = row["game_date"] if "game_date" in row.keys() else "unknown date"
                team_label = row["team_name"] if "team_name" in row.keys() else "unknown team"
                lines.append(
                    f"{index}. {player_label} on {date_label} for {team_label}: {row['value']} {metadata['label']}"
                )
            snippets.append(
                EvidenceSnippet(
                    source="Retrosheet",
                    title=f"Single-game {metadata['label']} leaders",
                    citation=f"{table_name} imported from Retrosheet",
                    summary=" ".join(lines),
                    payload={"metric": metric_name},
                )
            )
        return snippets

    def _document_snippets(self, connection, question: str) -> list[EvidenceSnippet]:
        return [
            EvidenceSnippet(
                source="Document Search",
                title=row["title"],
                citation=row["citation"],
                summary=row["content"],
                payload={"source_name": row["source_name"]},
            )
            for row in search_document_chunks(connection, question, limit=3)
        ]

    def _live_snippets(self, question: str) -> list[EvidenceSnippet]:
        snippets: list[EvidenceSnippet] = []
        scoreboard = self.live_client.scoreboard()
        if scoreboard["games"]:
            summary = " ".join(
                f"{game['away']} {game['away_score']} at {game['home']} {game['home_score']} ({game['status']})."
                for game in scoreboard["games"][:6]
            )
            snippets.append(
                EvidenceSnippet(
                    source="MLB Stats API",
                    title=f"Scoreboard for {scoreboard['date']}",
                    citation="statsapi.mlb.com schedule endpoint",
                    summary=summary,
                    payload=scoreboard,
                )
            )
        standings = self.live_client.standings()
        if standings["standings"]:
            summary = " ".join(
                f"{row['team']} {row['wins']}-{row['losses']} ({row['pct']}) in {row['division']}."
                for row in standings["standings"][:6]
            )
            snippets.append(
                EvidenceSnippet(
                    source="MLB Stats API",
                    title=f"{standings['season']} standings snapshot",
                    citation="statsapi.mlb.com standings endpoint",
                    summary=summary,
                    payload=standings,
                )
            )
        for candidate in extract_name_candidates(question):
            player_snapshot = self.live_client.player_season_snapshot(candidate)
            if not player_snapshot:
                continue
            hitting = player_snapshot.get("hitting", {})
            pitching = player_snapshot.get("pitching", {})
            fielding = player_snapshot.get("fielding", {})
            pieces = []
            if hitting:
                pieces.append(
                    f"Hitting: {hitting.get('avg')} AVG, {hitting.get('homeRuns')} HR, {hitting.get('ops')} OPS."
                )
            if pitching:
                pieces.append(f"Pitching: {pitching.get('era')} ERA, {pitching.get('strikeOuts')} SO.")
            if fielding:
                pieces.append(
                    f"Fielding: {fielding.get('gamesPlayed')} games, {fielding.get('fielding')} fielding percentage."
                )
            snippets.append(
                EvidenceSnippet(
                    source="MLB Stats API",
                    title=f"{player_snapshot['name']} {player_snapshot['season']} snapshot",
                    citation="statsapi.mlb.com people/stats endpoints",
                    summary=" ".join(piece for piece in pieces if piece),
                    payload=player_snapshot,
                )
            )
            break
        return snippets


def extract_year(question: str) -> int | None:
    match = re.search(r"\b(18\d{2}|19\d{2}|20\d{2})\b", question)
    return int(match.group(1)) if match else None
