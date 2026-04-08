from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .config import Settings
from .metrics import MetricCatalog
from .models import EvidenceSnippet
from .provider_metrics import (
    extract_team_filter,
    fetch_provider_group_rows,
    find_provider_metric,
    infer_group_preference,
    ProviderMetricSpec,
)
from .pybaseball_adapter import load_team_ids
from .query_intent import detect_ranking_intent, looks_like_leaderboard_question, mentions_current_scope
from .query_utils import extract_minimum_qualifier, extract_referenced_season, extract_season_span
from .storage import list_table_columns, table_exists
from .team_evaluator import safe_float, safe_int
from .team_season_compare import resolve_team_season_reference
from .team_season_leaders import (
    build_person_name,
    extract_team_phrase_from_leader_question,
    outs_to_innings_notation,
    select_historical_fielding_metric,
    select_historical_hitting_metric,
    select_historical_pitching_metric,
)


HISTORY_HINTS = {
    "history",
    "historically",
    "all time",
    "all-time",
    "ever",
    "in mlb history",
    "in baseball history",
}
CAREER_HINTS = {
    "career",
    "careers",
    "career-spanning",
    "career spanning",
    "career wise",
    "career-wise",
}
SINGLE_SEASON_HINTS = {
    "single season",
    "single-season",
    "season record",
    "best season",
    "worst season",
    "in a season",
}
STATCAST_ERA_HINTS = {"statcast era", "since statcast", "in the statcast era"}
TEAM_SCOPE_HINTS = ("which team", "what team", "teams had", "team had", "team has")
QUALIFIER_CLAUSE_PATTERN = re.compile(
    r"\b(?:with|and)?\s*(?:a\s+)?(?:minimum|min|at\s+least)\s+(?:of\s+)?[a-z0-9-]+(?:\s+[a-z0-9-]+){0,3}\s+"
    r"(?:starts?|gs|games?|plate\s+appearances|pa|at\s+bats|ab|innings|ip|home\s+runs?|hr|hits?|walks?|strikeouts?|outs?)\b",
    re.IGNORECASE,
)
METRIC_NORMALIZATION_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(
            r"\bwalk(?:ed|s)?(?:\s+the\s+(?:most|least|fewest|highest|lowest))?\s+batters?\s+per\s+game\b",
            re.IGNORECASE,
        ),
        "walks per game",
    ),
    (
        re.compile(
            r"\bbatters?\s+walk(?:ed|s)?\s+per\s+game\b",
            re.IGNORECASE,
        ),
        "walks per game",
    ),
    (
        re.compile(
            r"\bstr(?:u|o)ck?\s+out(?:\s+the\s+(?:most|least|fewest|highest|lowest))?\s+batters?\s+per\s+game\b",
            re.IGNORECASE,
        ),
        "strikeouts per game",
    ),
    (
        re.compile(
            r"\bhits?\s+allowed\s+per\s+game\b",
            re.IGNORECASE,
        ),
        "hits allowed per game",
    ),
    (
        re.compile(
            r"\bhome\s+runs?\s+allowed\s+per\s+game\b",
            re.IGNORECASE,
        ),
        "home runs allowed per game",
    ),
)
TEAM_ROLE_HINT_WORDS = (" which team ", " what team ", " teams ", " team ")
PITCHING_ROLE_HINT_WORDS = (
    " pitcher ",
    " pitchers ",
    " starter ",
    " starters ",
    " reliever ",
    " relievers ",
    " bullpen ",
    " rotation ",
    " on the mound ",
    " games started ",
    " starts ",
    " era ",
    " fip ",
    " whip ",
    " allowed ",
)
FIELDING_ROLE_HINT_WORDS = (
    " defender ",
    " defenders ",
    " fielding ",
    " fielder ",
    " fielders ",
    " defensive ",
    " defense ",
    " glove ",
    " errors ",
    " assists ",
    " putouts ",
)
HITTING_ROLE_HINT_WORDS = (
    " hitter ",
    " hitters ",
    " batter ",
    " batters ",
    " batting ",
    " offense ",
    " offensive ",
    " lineup ",
    " hits ",
    " home runs ",
    " rbi ",
    " runs batted in ",
    " obp ",
    " slg ",
    " ops ",
)


@dataclass(slots=True, frozen=True)
class SeasonMetricSpec:
    key: str
    label: str
    aliases: tuple[str, ...]
    source_family: str
    role: str
    entity_scope: str
    higher_is_better: bool
    formatter: str
    sample_basis: str | None = None
    min_sample_size: int = 0
    provider_metric_name: str | None = None
    provider_batting_column: str | None = None
    provider_pitching_column: str | None = None
    provider_qualified_only: bool = False


@dataclass(slots=True)
class SeasonMetricQuery:
    metric: SeasonMetricSpec
    descriptor: str
    sort_desc: bool
    entity_scope: str
    role: str
    start_season: int
    end_season: int
    scope_label: str
    team_filter_code: str | None
    team_filter_name: str | None
    provider_group_preference: str | None
    minimum_starts: int | None
    aggregate_range: bool = False


SEASON_METRICS: tuple[SeasonMetricSpec, ...] = (
    SeasonMetricSpec("games", "Games", ("games", "games played"), "historical", "hitter", "player", True, ".0f", "games", 1),
    SeasonMetricSpec("plate_appearances", "PA", ("plate appearances", "pa"), "historical", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("at_bats", "AB", ("at bats", "ab"), "historical", "hitter", "player", True, ".0f", "at_bats", 5),
    SeasonMetricSpec("ops", "OPS", ("ops", "on-base plus slugging", "on base plus slugging"), "historical", "hitter", "player", True, ".3f", "plate_appearances", 25),
    SeasonMetricSpec("obp", "OBP", ("obp", "on-base percentage", "on base percentage"), "historical", "hitter", "player", True, ".3f", "plate_appearances", 25),
    SeasonMetricSpec("slg", "SLG", ("slg", "slugging percentage", "slugging"), "historical", "hitter", "player", True, ".3f", "at_bats", 25),
    SeasonMetricSpec("avg", "AVG", ("avg", "ba", "batting average"), "historical", "hitter", "player", True, ".3f", "at_bats", 25),
    SeasonMetricSpec("total_bases", "TB", ("total bases", "tb"), "historical", "hitter", "player", True, ".0f", "at_bats", 5),
    SeasonMetricSpec("extra_base_hits", "XBH", ("extra-base hits", "extra base hits", "xbh"), "historical", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("singles", "Singles", ("singles", "single"), "historical", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("doubles", "2B", ("doubles", "double", "2b"), "historical", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("triples", "3B", ("triples", "triple", "3b"), "historical", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("hits", "Hits", ("hits", "base hits", "base hit"), "historical", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("home_runs", "HR", ("home runs", "home run", "hr", "homers", "homeruns"), "historical", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("runs", "Runs", ("runs scored", "runs"), "historical", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("runs_per_game", "R/G", ("runs per game", "r/g", "runs/game"), "historical", "hitter", "player", True, ".2f", "games", 10),
    SeasonMetricSpec("rbi", "RBI", ("rbi", "runs batted in"), "historical", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("rbi_per_game", "RBI/G", ("rbi per game", "rbi/game", "runs batted in per game"), "historical", "hitter", "player", True, ".2f", "games", 10),
    SeasonMetricSpec("walks", "BB", ("walks", "walk"), "historical", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("walks_per_game", "BB/G", ("walks per game", "bb/g", "bb per game"), "historical", "hitter", "player", True, ".2f", "games", 10),
    SeasonMetricSpec("strikeouts", "SO", ("strikeouts", "strikeout"), "historical", "hitter", "player", False, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("strikeouts_per_game", "SO/G", ("strikeouts per game", "so/g", "so per game"), "historical", "hitter", "player", False, ".2f", "games", 10),
    SeasonMetricSpec("steals", "SB", ("stolen bases", "stolen base", "steals", "sb"), "historical", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("hit_by_pitch", "HBP", ("hit by pitch", "hbp"), "historical", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("hits_per_game", "H/G", ("hits per game", "h/g", "base hits per game"), "historical", "hitter", "player", True, ".2f", "games", 10),
    SeasonMetricSpec("home_runs_per_game", "HR/G", ("home runs per game", "hr/g", "homers per game"), "historical", "hitter", "player", True, ".2f", "games", 10),
    SeasonMetricSpec("era", "ERA", ("era", "earned run average"), "historical", "pitcher", "player", False, ".2f", "ipouts", 30),
    SeasonMetricSpec("fip", "FIP", ("fip", "fielding independent pitching"), "historical", "pitcher", "player", False, ".2f", "ipouts", 30),
    SeasonMetricSpec("whip", "WHIP", ("whip",), "historical", "pitcher", "player", False, ".3f", "ipouts", 30),
    SeasonMetricSpec("games", "Games", ("games", "games pitched"), "historical", "pitcher", "player", True, ".0f", "games", 1),
    SeasonMetricSpec("games_started", "GS", ("games started", "starts", "gs"), "historical", "pitcher", "player", True, ".0f", "games", 1),
    SeasonMetricSpec("wins", "Wins", ("wins",), "historical", "pitcher", "player", True, ".0f", "games", 1),
    SeasonMetricSpec("losses", "Losses", ("losses",), "historical", "pitcher", "player", False, ".0f", "games", 1),
    SeasonMetricSpec("saves", "Saves", ("saves",), "historical", "pitcher", "player", True, ".0f", "games", 1),
    SeasonMetricSpec("innings", "IP", ("innings pitched", "innings"), "historical", "pitcher", "player", True, ".1f", "ipouts", 30),
    SeasonMetricSpec("strikeouts", "SO", ("strikeouts", "strikeout"), "historical", "pitcher", "player", True, ".0f", "ipouts", 30),
    SeasonMetricSpec("strikeouts_per_game", "SO/G", ("strikeouts per game", "so/g", "strikeouts/game"), "historical", "pitcher", "player", True, ".2f", "games", 10),
    SeasonMetricSpec("walks", "BB", ("walks", "walks allowed", "bb"), "historical", "pitcher", "player", False, ".0f", "ipouts", 30),
    SeasonMetricSpec("walks_per_game", "BB/G", ("walks per game", "bb/g", "batters walked per game", "walked batters per game"), "historical", "pitcher", "player", False, ".2f", "games", 10),
    SeasonMetricSpec("earned_runs", "ER", ("earned runs", "earned run", "er"), "historical", "pitcher", "player", False, ".0f", "ipouts", 30),
    SeasonMetricSpec("earned_runs_per_game", "ER/G", ("earned runs per game", "er/g"), "historical", "pitcher", "player", False, ".2f", "games", 10),
    SeasonMetricSpec("strikeouts_per_9", "K/9", ("k/9", "strikeouts per 9", "strikeouts per nine"), "historical", "pitcher", "player", True, ".2f", "ipouts", 30),
    SeasonMetricSpec("walks_per_9", "BB/9", ("bb/9", "walks per 9", "walks per nine"), "historical", "pitcher", "player", False, ".2f", "ipouts", 30),
    SeasonMetricSpec("hits_per_9", "H/9", ("h/9", "hits per 9", "hits per nine"), "historical", "pitcher", "player", False, ".2f", "ipouts", 30),
    SeasonMetricSpec("home_runs_per_9", "HR/9", ("hr/9", "home runs per 9", "home runs per nine"), "historical", "pitcher", "player", False, ".2f", "ipouts", 30),
    SeasonMetricSpec("strikeout_to_walk", "K/BB", ("k/bb", "strikeout to walk ratio", "strikeouts to walks"), "historical", "pitcher", "player", True, ".2f", "ipouts", 30),
    SeasonMetricSpec("hits_allowed", "Hits Allowed", ("hits allowed",), "historical", "pitcher", "player", False, ".0f", "ipouts", 30),
    SeasonMetricSpec("hits_allowed_per_game", "H/G Allowed", ("hits allowed per game", "h/g allowed"), "historical", "pitcher", "player", False, ".2f", "games", 10),
    SeasonMetricSpec("home_runs_allowed", "HR Allowed", ("home runs allowed", "home run allowed"), "historical", "pitcher", "player", False, ".0f", "ipouts", 30),
    SeasonMetricSpec("home_runs_allowed_per_game", "HR/G Allowed", ("home runs allowed per game", "hr/g allowed"), "historical", "pitcher", "player", False, ".2f", "games", 10),
    SeasonMetricSpec("games", "Games", ("games", "games played"), "historical", "fielder", "player", True, ".0f", "games", 10),
    SeasonMetricSpec("fielding_pct", "Fld%", ("fielding percentage", "fielding pct", "fielding"), "historical", "fielder", "player", True, ".3f", "games", 10),
    SeasonMetricSpec("errors", "Errors", ("errors",), "historical", "fielder", "player", False, ".0f", "games", 10),
    SeasonMetricSpec("assists", "Assists", ("assists",), "historical", "fielder", "player", True, ".0f", "games", 10),
    SeasonMetricSpec("putouts", "Putouts", ("putouts",), "historical", "fielder", "player", True, ".0f", "games", 10),
    SeasonMetricSpec("double_plays", "Double Plays", ("double plays", "double-play turns"), "historical", "fielder", "player", True, ".0f", "games", 10),
    SeasonMetricSpec("wins", "Wins", ("wins",), "historical", "team", "team", True, ".0f", "games", 10),
    SeasonMetricSpec("losses", "Losses", ("losses",), "historical", "team", "team", False, ".0f", "games", 10),
    SeasonMetricSpec("win_pct", "Win%", ("winning percentage", "win percentage", "win pct", "win%"), "historical", "team", "team", True, ".3f", "games", 10),
    SeasonMetricSpec("runs", "Runs", ("runs scored", "runs"), "historical", "team", "team", True, ".0f", "games", 10),
    SeasonMetricSpec("runs_per_game", "R/G", ("runs per game", "r/g", "runs/game"), "historical", "team", "team", True, ".2f", "games", 10),
    SeasonMetricSpec("runs_allowed", "Runs Allowed", ("runs allowed", "ra"), "historical", "team", "team", False, ".0f", "games", 10),
    SeasonMetricSpec("runs_allowed_per_game", "RA/G", ("runs allowed per game", "ra/g"), "historical", "team", "team", False, ".2f", "games", 10),
    SeasonMetricSpec("doubles", "2B", ("doubles", "double", "2b"), "historical", "team", "team", True, ".0f", "games", 10),
    SeasonMetricSpec("triples", "3B", ("triples", "triple", "3b"), "historical", "team", "team", True, ".0f", "games", 10),
    SeasonMetricSpec("walks", "BB", ("walks", "walk"), "historical", "team", "team", True, ".0f", "games", 10),
    SeasonMetricSpec("walks_per_game", "BB/G", ("walks per game", "bb/g", "bb per game"), "historical", "team", "team", True, ".2f", "games", 10),
    SeasonMetricSpec("avg", "AVG", ("batting average", "avg", "ba"), "historical", "team", "team", True, ".3f", "games", 10),
    SeasonMetricSpec("obp", "OBP", ("obp", "on-base percentage", "on base percentage"), "historical", "team", "team", True, ".3f", "games", 10),
    SeasonMetricSpec("slg", "SLG", ("slg", "slugging percentage", "slugging"), "historical", "team", "team", True, ".3f", "games", 10),
    SeasonMetricSpec("ops", "OPS", ("ops", "on-base plus slugging", "on base plus slugging"), "historical", "team", "team", True, ".3f", "games", 10),
    SeasonMetricSpec("home_runs", "HR", ("home runs", "home run", "hr", "homers", "homeruns"), "historical", "team", "team", True, ".0f", "games", 10),
    SeasonMetricSpec("home_runs_per_game", "HR/G", ("home runs per game", "hr/g", "homers per game"), "historical", "team", "team", True, ".2f", "games", 10),
    SeasonMetricSpec("hits", "Hits", ("hits", "base hits", "base hit"), "historical", "team", "team", True, ".0f", "games", 10),
    SeasonMetricSpec("hits_per_game", "H/G", ("hits per game", "h/g", "base hits per game"), "historical", "team", "team", True, ".2f", "games", 10),
    SeasonMetricSpec("fielding_pct", "Fld%", ("fielding percentage", "fielding pct", "fielding"), "historical", "team", "team", True, ".3f", "games", 10),
    SeasonMetricSpec("plate_appearances", "PA", ("plate appearances", "pa"), "statcast", "hitter", "player", True, ".0f", "plate_appearances", 10),
    SeasonMetricSpec("at_bats", "AB", ("at bats", "ab"), "statcast", "hitter", "player", True, ".0f", "at_bats", 10),
    SeasonMetricSpec("avg", "AVG", ("avg", "ba", "batting average"), "statcast", "hitter", "player", True, ".3f", "at_bats", 20),
    SeasonMetricSpec("obp", "OBP", ("obp", "on-base percentage", "on base percentage"), "statcast", "hitter", "player", True, ".3f", "plate_appearances", 20),
    SeasonMetricSpec("slg", "SLG", ("slg", "slugging percentage", "slugging"), "statcast", "hitter", "player", True, ".3f", "at_bats", 20),
    SeasonMetricSpec("ops", "OPS", ("ops", "on-base plus slugging", "on base plus slugging"), "statcast", "hitter", "player", True, ".3f", "plate_appearances", 20),
    SeasonMetricSpec("xba", "xBA", ("xba", "expected batting average"), "statcast", "hitter", "player", True, ".3f", "at_bats", 20),
    SeasonMetricSpec("xwoba", "xwOBA", ("xwoba", "expected woba"), "statcast", "hitter", "player", True, ".3f", "xwoba_denom", 20),
    SeasonMetricSpec("xslg", "xSLG", ("xslg", "expected slugging"), "statcast", "hitter", "player", True, ".3f", "at_bats", 20),
    SeasonMetricSpec("hard_hit_rate", "Hard-Hit Rate", ("hard-hit rate", "hard hit rate"), "statcast", "hitter", "player", True, ".3f", "batted_ball_events", 15),
    SeasonMetricSpec("barrel_rate", "Barrel Rate", ("barrel rate",), "statcast", "hitter", "player", True, ".3f", "batted_ball_events", 15),
    SeasonMetricSpec("avg_exit_velocity", "Avg EV", ("average exit velocity", "avg exit velocity", "ev", "exit velocity"), "statcast", "hitter", "player", True, ".1f", "launch_speed_count", 10),
    SeasonMetricSpec("max_exit_velocity", "maxEV", ("max exit velocity", "maximum exit velocity", "maxev"), "statcast", "hitter", "player", True, ".1f", "launch_speed_count", 1),
    SeasonMetricSpec("avg_bat_speed", "Avg Bat Speed", ("average bat speed", "avg bat speed", "bat speed"), "statcast", "hitter", "player", True, ".1f", "plate_appearances", 10),
    SeasonMetricSpec("max_bat_speed", "Max Bat Speed", ("max bat speed", "maximum bat speed"), "statcast", "hitter", "player", True, ".1f", "plate_appearances", 10),
    SeasonMetricSpec("singles", "Singles", ("singles", "single"), "statcast", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("doubles", "2B", ("doubles", "double", "2b"), "statcast", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("triples", "3B", ("triples", "triple", "3b"), "statcast", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("home_runs", "HR", ("home runs", "home run", "hr", "homers", "homeruns"), "statcast", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("home_runs_per_game", "HR/G", ("home runs per game", "hr/g", "homers per game"), "statcast", "hitter", "player", True, ".2f", "games", 5),
    SeasonMetricSpec("hits", "Hits", ("hits", "base hits", "base hit"), "statcast", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("hits_per_game", "H/G", ("hits per game", "h/g", "base hits per game"), "statcast", "hitter", "player", True, ".2f", "games", 5),
    SeasonMetricSpec("walks", "BB", ("walks", "walk"), "statcast", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("walks_per_game", "BB/G", ("walks per game", "bb/g", "bb per game"), "statcast", "hitter", "player", True, ".2f", "games", 5),
    SeasonMetricSpec("strikeouts", "SO", ("strikeouts", "strikeout"), "statcast", "hitter", "player", False, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("strikeouts_per_game", "SO/G", ("strikeouts per game", "so/g", "so per game"), "statcast", "hitter", "player", False, ".2f", "games", 5),
    SeasonMetricSpec("rbi", "RBI", ("rbi", "runs batted in"), "statcast", "hitter", "player", True, ".0f", "plate_appearances", 5),
    SeasonMetricSpec("rbi_per_game", "RBI/G", ("rbi per game", "rbi/game", "runs batted in per game"), "statcast", "hitter", "player", True, ".2f", "games", 5),
    SeasonMetricSpec("plate_appearances", "PA", ("plate appearances", "pa"), "statcast", "team", "team", True, ".0f", "plate_appearances", 50),
    SeasonMetricSpec("xba", "xBA", ("xba", "expected batting average"), "statcast", "team", "team", True, ".3f", "at_bats", 100),
    SeasonMetricSpec("xwoba", "xwOBA", ("xwoba", "expected woba"), "statcast", "team", "team", True, ".3f", "xwoba_denom", 100),
    SeasonMetricSpec("xslg", "xSLG", ("xslg", "expected slugging"), "statcast", "team", "team", True, ".3f", "at_bats", 100),
    SeasonMetricSpec("hard_hit_rate", "Hard-Hit Rate", ("hard-hit rate", "hard hit rate"), "statcast", "team", "team", True, ".3f", "batted_ball_events", 50),
    SeasonMetricSpec("barrel_rate", "Barrel Rate", ("barrel rate",), "statcast", "team", "team", True, ".3f", "batted_ball_events", 50),
    SeasonMetricSpec("avg_exit_velocity", "Avg EV", ("average exit velocity", "avg exit velocity", "ev", "exit velocity"), "statcast", "team", "team", True, ".1f", "launch_speed_count", 30),
    SeasonMetricSpec("hits", "Hits", ("hits", "base hits", "base hit"), "statcast", "team", "team", True, ".0f", "plate_appearances", 30),
    SeasonMetricSpec("hits_per_game", "H/G", ("hits per game", "h/g", "base hits per game"), "statcast", "team", "team", True, ".2f", "games", 10),
    SeasonMetricSpec("strikeouts", "Strikeouts", ("strikeouts", "strikeout"), "statcast", "team", "team", False, ".0f", "plate_appearances", 30),
    SeasonMetricSpec("strikeouts_per_game", "SO/G", ("strikeouts per game", "so/g", "so per game"), "statcast", "team", "team", False, ".2f", "games", 10),
)


class SeasonMetricLeaderboardResearcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.catalog = MetricCatalog.load(settings.project_root)

    def build_snippet(self, connection, question: str) -> EvidenceSnippet | None:
        query = parse_season_metric_query(connection, self.settings, self.catalog, question)
        if query is None:
            return None
        if query.metric.source_family == "provider":
            rows = fetch_provider_season_rows(query)
        elif query.metric.source_family == "statcast":
            rows = fetch_statcast_season_rows(connection, query)
            if not rows and query.entity_scope == "player":
                provider_fallback = build_statcast_provider_fallback_query(query, self.catalog)
                if provider_fallback is not None:
                    fallback_rows = fetch_provider_season_rows(provider_fallback)
                    if fallback_rows:
                        query = provider_fallback
                        rows = fallback_rows
        else:
            rows = fetch_historical_season_rows(connection, query)
            if not rows:
                statcast_fallback = build_source_fallback_query(query, "statcast")
                if statcast_fallback is not None:
                    fallback_rows = fetch_statcast_season_rows(connection, statcast_fallback)
                    if not fallback_rows and statcast_fallback.entity_scope == "player":
                        provider_fallback = build_statcast_provider_fallback_query(statcast_fallback, self.catalog)
                        if provider_fallback is not None:
                            fallback_rows = fetch_provider_season_rows(provider_fallback)
                            if fallback_rows:
                                statcast_fallback = provider_fallback
                    if fallback_rows:
                        query = statcast_fallback
                        rows = fallback_rows
        if not rows:
            return None
        summary = build_season_metric_summary(query, rows)
        mode = "live" if query.scope_label == str(self.settings.live_season or date.today().year) and mentions_current_scope(question.lower()) else "historical"
        return EvidenceSnippet(
            source="Season Metric Leaderboards",
            title=f"{query.scope_label} {query.metric.label} leaderboard",
            citation=build_citation(query),
            summary=summary,
            payload={
                "analysis_type": "season_metric_leaderboard",
                "mode": mode,
                "metric": query.metric.label,
                "scope_label": query.scope_label,
                "entity_scope": query.entity_scope,
                "role": query.role,
                "source_family": query.metric.source_family,
                "team_filter": query.team_filter_name,
                "rows": rows[:25],
            },
        )


def parse_season_metric_query(connection, settings: Settings, catalog: MetricCatalog, question: str) -> SeasonMetricQuery | None:
    lowered = f" {question.lower()} "
    metric_search_text = normalize_metric_search_text(strip_qualifier_clauses(lowered))
    metric = find_season_metric(metric_search_text)
    provider_group_preference = infer_group_preference(lowered)
    minimum_starts = extract_minimum_qualifier(question, ("start", "starts", "gs"))
    if metric is None:
        provider_metric = find_provider_metric(metric_search_text, catalog)
        if provider_metric is not None:
            metric = build_provider_season_metric_spec(provider_metric, provider_group_preference)
            if provider_group_preference is None:
                if provider_metric.pitching_column and not provider_metric.batting_column:
                    provider_group_preference = "pitching"
                elif provider_metric.batting_column and not provider_metric.pitching_column:
                    provider_group_preference = "batting"
    if metric is None:
        return None
    if not looks_like_leaderboard_question(lowered):
        return None
    ranking_intent = detect_ranking_intent(lowered, higher_is_better=metric.higher_is_better, fallback_label="leader")
    if ranking_intent is None:
        return None
    current_season = settings.live_season or date.today().year
    start_season, end_season, scope_label, aggregate_range = resolve_season_scope(question, current_season, metric.source_family)
    if start_season is None or end_season is None:
        return None
    if aggregate_range and metric.source_family == "provider":
        historical_fallback = build_source_fallback_query(
            SeasonMetricQuery(
                metric=metric,
                descriptor=ranking_intent.descriptor,
                sort_desc=ranking_intent.sort_desc,
                entity_scope="player",
                role=metric.role,
                start_season=start_season,
                end_season=end_season,
                scope_label=scope_label,
                team_filter_code=None,
                team_filter_name=None,
                provider_group_preference=provider_group_preference,
                minimum_starts=minimum_starts,
                aggregate_range=aggregate_range,
            ),
            "historical",
        )
        if historical_fallback is not None:
            metric = historical_fallback.metric
    explicit_team_scope = any(token in lowered for token in TEAM_SCOPE_HINTS)
    if explicit_team_scope and metric.entity_scope != "team" and not metric_supports_team_scope(metric):
        return None
    entity_scope = "team" if metric.entity_scope == "team" or (explicit_team_scope and metric_supports_team_scope(metric)) else "player"
    role = metric.role if entity_scope == "player" else "team"
    team_filter_code = None
    team_filter_name = None
    if entity_scope == "player":
        team_filter_code, team_filter_name = resolve_question_team_filter(connection, settings, question, start_season, end_season)
    return SeasonMetricQuery(
        metric=metric,
        descriptor=ranking_intent.descriptor,
        sort_desc=ranking_intent.sort_desc,
        entity_scope=entity_scope,
        role=role,
        start_season=start_season,
        end_season=end_season,
        scope_label=scope_label,
        team_filter_code=team_filter_code,
        team_filter_name=team_filter_name,
        provider_group_preference=provider_group_preference,
        minimum_starts=minimum_starts,
        aggregate_range=aggregate_range,
    )


def find_season_metric(lowered_question: str) -> SeasonMetricSpec | None:
    best_match: tuple[int, SeasonMetricSpec] | None = None
    for metric in SEASON_METRICS:
        for alias in metric.aliases:
            alias_lower = alias.lower().strip()
            if not alias_lower:
                continue
            if alias_lower.isalnum() and len(alias_lower) <= 5:
                pattern = rf"(?<![a-z0-9]){re.escape(alias_lower)}(?![a-z0-9])"
                if re.search(pattern, lowered_question) is None:
                    continue
                score = len(alias_lower)
            else:
                pattern = rf"(?<![a-z0-9]){re.escape(alias_lower)}(?![a-z0-9])"
                if re.search(pattern, lowered_question) is None:
                    continue
                score = len(alias_lower)
            score += metric_match_bonus(metric, lowered_question)
            if best_match is None or score > best_match[0]:
                best_match = (score, metric)
    return best_match[1] if best_match else None


def strip_qualifier_clauses(lowered_question: str) -> str:
    return QUALIFIER_CLAUSE_PATTERN.sub(" ", lowered_question)


def normalize_metric_search_text(lowered_question: str) -> str:
    normalized = lowered_question
    for pattern, replacement in METRIC_NORMALIZATION_PATTERNS:
        normalized = pattern.sub(f" {replacement} ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def metric_match_bonus(metric: SeasonMetricSpec, lowered_question: str) -> int:
    score = 0
    if metric.entity_scope == "team" and any(token in lowered_question for token in TEAM_ROLE_HINT_WORDS):
        score += 40
    if metric.role == "pitcher" and any(token in lowered_question for token in PITCHING_ROLE_HINT_WORDS):
        score += 30
    if metric.role == "fielder" and any(token in lowered_question for token in FIELDING_ROLE_HINT_WORDS):
        score += 30
    if metric.role == "hitter" and any(token in lowered_question for token in HITTING_ROLE_HINT_WORDS):
        score += 20
    return score


def resolve_season_scope(question: str, current_season: int, source_family: str) -> tuple[int | None, int | None, str, bool]:
    lowered = question.lower()
    span = extract_season_span(question, current_season)
    if span is not None:
        if source_family == "statcast" and span.start_season < 2015:
            return 2015, span.end_season, f"2015-{span.end_season}", True
        return span.start_season, span.end_season, span.label, span.aggregate
    referenced = extract_referenced_season(question, current_season)
    if referenced is not None:
        return referenced, referenced, str(referenced), False
    if source_family == "provider":
        if any(token in lowered for token in HISTORY_HINTS):
            return None, None, "", False
        return current_season, current_season, str(current_season), False
    if any(token in lowered for token in STATCAST_ERA_HINTS) and source_family == "statcast":
        return 2015, current_season, "Statcast era", True
    history_requested = any(token in lowered for token in HISTORY_HINTS)
    career_requested = any(token in lowered for token in CAREER_HINTS)
    single_season_requested = any(token in lowered for token in SINGLE_SEASON_HINTS)
    if history_requested or career_requested:
        start = 2015 if source_family == "statcast" else 1871
        label = "Statcast era" if source_family == "statcast" else "MLB history"
        return start, current_season, label, not single_season_requested
    if source_family == "statcast" and mentions_current_scope(lowered):
        return current_season, current_season, str(current_season), False
    if source_family != "provider" and not mentions_current_scope(lowered):
        start = 2015 if source_family == "statcast" else 1871
        label = "Statcast era" if source_family == "statcast" else "MLB history"
        return start, current_season, label, True
    return None, None, "", False


def build_provider_season_metric_spec(metric: ProviderMetricSpec, group_preference: str | None) -> SeasonMetricSpec:
    if metric.pitching_column and metric.batting_column:
        inferred_role = "player"
    else:
        inferred_role = "pitcher" if metric.pitching_column and not metric.batting_column else "hitter"
    if group_preference == "pitching" and metric.pitching_column:
        inferred_role = "pitcher"
    elif group_preference == "batting" and metric.batting_column:
        inferred_role = "hitter"
    return SeasonMetricSpec(
        key=normalize_metric_key(metric.metric_name),
        label=metric.label,
        aliases=(metric.metric_name, *metric.aliases),
        source_family="provider",
        role=inferred_role,
        entity_scope="player",
        higher_is_better=metric.higher_is_better,
        formatter=provider_metric_formatter(metric.metric_name),
        provider_metric_name=metric.metric_name,
        provider_batting_column=metric.batting_column,
        provider_pitching_column=metric.pitching_column,
        provider_qualified_only=metric.qualified_only,
    )


def build_statcast_provider_fallback_query(query: SeasonMetricQuery, catalog: MetricCatalog) -> SeasonMetricQuery | None:
    provider_metric = find_provider_metric(f" {query.metric.label.lower()} ", catalog)
    if provider_metric is None:
        return None
    provider_spec = build_provider_season_metric_spec(provider_metric, "batting")
    return SeasonMetricQuery(
        metric=provider_spec,
        descriptor=query.descriptor,
        sort_desc=query.sort_desc,
        entity_scope="player",
        role="hitter",
        start_season=query.start_season,
        end_season=query.end_season,
        scope_label=query.scope_label,
        team_filter_code=query.team_filter_code,
        team_filter_name=query.team_filter_name,
        provider_group_preference="batting",
        minimum_starts=None,
    )


def build_source_fallback_query(query: SeasonMetricQuery, target_family: str) -> SeasonMetricQuery | None:
    if query.start_season is None or query.end_season is None:
        return None
    if target_family == "statcast" and query.start_season < 2015:
        return None
    candidate = next(
        (
            item
            for item in SEASON_METRICS
            if item.source_family == target_family
            and item.key == query.metric.key
            and item.entity_scope == query.entity_scope
        ),
        None,
    )
    if candidate is None:
        return None
    return SeasonMetricQuery(
        metric=candidate,
        descriptor=query.descriptor,
        sort_desc=query.sort_desc,
        entity_scope=query.entity_scope,
        role=candidate.role if query.entity_scope == "player" else "team",
        start_season=query.start_season,
        end_season=query.end_season,
        scope_label=query.scope_label,
        team_filter_code=query.team_filter_code,
        team_filter_name=query.team_filter_name,
        provider_group_preference=query.provider_group_preference,
        minimum_starts=query.minimum_starts,
        aggregate_range=query.aggregate_range,
    )


def normalize_metric_key(metric_name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", metric_name.lower()).strip("_")


def provider_metric_formatter(metric_name: str) -> str:
    if metric_name in {"WAR", "wRC+", "wOBA", "xwOBA", "xBA", "xSLG", "Clutch", "WPA", "RE24", "REW", "WPA/LI", "FIP", "xFIP", "tERA", "SIERA", "ERA", "WHIP", "K/9", "BB/9", "HR/9", "K/BB", "K-BB%", "ERA-", "FIP-", "xFIP-", "RA9-WAR", "xERA", "Stuff+", "Location+", "Pitching+", "AVG+", "OBP+", "SLG+", "ISO+", "BABIP+", "EV", "maxEV", "CSW%"}:
        return ".2f"
    return ".3f" if "%" in metric_name else ".1f"


def resolve_question_team_filter(connection, settings: Settings, question: str, start_season: int, end_season: int) -> tuple[str | None, str | None]:
    lowered = question.lower()
    if any(token in lowered for token in TEAM_SCOPE_HINTS):
        return None, None
    current_season = settings.live_season or date.today().year
    if start_season == end_season and start_season != current_season:
        team_phrase = extract_team_phrase_from_leader_question(question)
        if team_phrase:
            try:
                reference = resolve_team_season_reference(connection, team_phrase, start_season, None, current_season)
            except Exception:
                reference = None
            if reference is not None and reference.team_code:
                return reference.team_code.upper(), reference.display_name
    code = extract_team_filter(lowered)
    if code:
        return code, resolve_team_name(code, current_season)
    return None, None


def resolve_team_name(team_code: str, season: int) -> str:
    for row in load_team_ids(season):
        keys = ("teamIDBR", "teamIDfg", "teamIDretro", "teamIDlahman45")
        if any(str(row.get(key) or "").upper() == team_code.upper() for key in keys):
            return str(row.get("team_name") or row.get("team_name_fg") or row.get("teamIDBR") or team_code)
    return team_code


def metric_supports_team_scope(metric: SeasonMetricSpec) -> bool:
    if metric.source_family == "provider":
        return False
    return any(
        candidate.source_family == metric.source_family
        and candidate.key == metric.key
        and candidate.entity_scope == "team"
        for candidate in SEASON_METRICS
    )


def fetch_historical_season_rows(connection, query: SeasonMetricQuery) -> list[dict[str, Any]]:
    if query.entity_scope == "team":
        return fetch_historical_team_rows(connection, query)
    if query.role == "pitcher":
        return fetch_historical_pitcher_rows(connection, query)
    if query.role == "fielder":
        return fetch_historical_fielder_rows(connection, query)
    return fetch_historical_hitter_rows(connection, query)


def fetch_historical_hitter_rows(connection, query: SeasonMetricQuery) -> list[dict[str, Any]]:
    if not (table_exists(connection, "lahman_batting") and table_exists(connection, "lahman_people")):
        return []
    parameters: list[Any] = [query.start_season, query.end_season]
    team_filter_sql = ""
    if query.team_filter_code:
        team_filter_sql = "AND upper(b.teamid) = ?"
        parameters.append(query.team_filter_code.upper())
    season_select = (
        "MIN(CAST(b.yearid AS INTEGER)) AS start_season, MAX(CAST(b.yearid AS INTEGER)) AS end_season,"
        if query.aggregate_range
        else "CAST(b.yearid AS INTEGER) AS season,"
    )
    season_group = "" if query.aggregate_range else "CAST(b.yearid AS INTEGER), "
    rows = connection.execute(
        f"""
        SELECT
            {season_select}
            b.playerid,
            p.namefirst,
            p.namelast,
            CASE WHEN COUNT(DISTINCT upper(b.teamid)) = 1 THEN MIN(upper(b.teamid)) ELSE 'MULTI' END AS team,
            SUM(CAST(COALESCE(b.g, '0') AS INTEGER)) AS games,
            SUM(CAST(COALESCE(b.ab, '0') AS INTEGER)) AS at_bats,
            SUM(CAST(COALESCE(b.r, '0') AS INTEGER)) AS runs,
            SUM(CAST(COALESCE(b.h, '0') AS INTEGER)) AS hits,
            SUM(CAST(COALESCE(b.c_2b, '0') AS INTEGER)) AS doubles,
            SUM(CAST(COALESCE(b.c_3b, '0') AS INTEGER)) AS triples,
            SUM(CAST(COALESCE(b.hr, '0') AS INTEGER)) AS home_runs,
            SUM(CAST(COALESCE(b.rbi, '0') AS INTEGER)) AS rbi,
            SUM(CAST(COALESCE(b.sb, '0') AS INTEGER)) AS steals,
            SUM(CAST(COALESCE(b.cs, '0') AS INTEGER)) AS caught_stealing,
            SUM(CAST(COALESCE(b.bb, '0') AS INTEGER)) AS walks,
            SUM(CAST(COALESCE(b.so, '0') AS INTEGER)) AS strikeouts,
            SUM(CAST(COALESCE(b.hbp, '0') AS INTEGER)) AS hit_by_pitch,
            SUM(CAST(COALESCE(b.sh, '0') AS INTEGER)) AS sacrifice_hits,
            SUM(CAST(COALESCE(b.sf, '0') AS INTEGER)) AS sacrifice_flies
        FROM lahman_batting AS b
        JOIN lahman_people AS p
          ON p.playerid = b.playerid
        WHERE CAST(b.yearid AS INTEGER) BETWEEN ? AND ?
          {team_filter_sql}
        GROUP BY {season_group} b.playerid, p.namefirst, p.namelast
        """,
        tuple(parameters),
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        at_bats = safe_int(row["at_bats"]) or 0
        walks = safe_int(row["walks"]) or 0
        hit_by_pitch = safe_int(row["hit_by_pitch"]) or 0
        sacrifice_flies = safe_int(row["sacrifice_flies"]) or 0
        sacrifice_hits = safe_int(row["sacrifice_hits"]) or 0
        hits = safe_int(row["hits"]) or 0
        doubles = safe_int(row["doubles"]) or 0
        triples = safe_int(row["triples"]) or 0
        home_runs = safe_int(row["home_runs"]) or 0
        plate_appearances = at_bats + walks + hit_by_pitch + sacrifice_flies + sacrifice_hits
        if not passes_sample_threshold(query.metric, {"plate_appearances": plate_appearances, "at_bats": at_bats}):
            continue
        avg = (hits / at_bats) if at_bats else None
        obp_denom = at_bats + walks + hit_by_pitch + sacrifice_flies
        obp = ((hits + walks + hit_by_pitch) / obp_denom) if obp_denom else None
        singles = hits - doubles - triples - home_runs
        slg = ((singles + (2 * doubles) + (3 * triples) + (4 * home_runs)) / at_bats) if at_bats else None
        ops = (obp + slg) if obp is not None and slg is not None else None
        total_bases = singles + (2 * doubles) + (3 * triples) + (4 * home_runs)
        extra_base_hits = doubles + triples + home_runs
        metric_value = select_historical_hitting_metric(query.metric.key, at_bats, plate_appearances, avg, obp, slg, ops, row)
        if metric_value is None:
            continue
        scope_start, scope_end, scope_text = row_scope(row)
        candidates.append(
            {
                "season": scope_end,
                "scope_label": scope_text,
                "scope_start_season": scope_start,
                "scope_end_season": scope_end,
                "player_name": build_person_name(row["namefirst"], row["namelast"], row["playerid"]),
                "team": str(row["team"] or ""),
                "metric_value": float(metric_value),
                "sample_size": plate_appearances,
                "games": safe_int(row["games"]) or 0,
                "plate_appearances": plate_appearances,
                "at_bats": at_bats,
                "runs": safe_int(row["runs"]) or 0,
                "avg": avg,
                "obp": obp,
                "slg": slg,
                "ops": ops,
                "singles": singles,
                "doubles": doubles,
                "triples": triples,
                "total_bases": total_bases,
                "extra_base_hits": extra_base_hits,
                "home_runs": home_runs,
                "hits": hits,
                "steals": safe_int(row["steals"]) or 0,
                "hit_by_pitch": hit_by_pitch,
                "runs_batted_in": safe_int(row["rbi"]) or 0,
                "walks": walks,
                "strikeouts": safe_int(row["strikeouts"]) or 0,
            }
        )
    return rank_rows(candidates, query)


def fetch_historical_pitcher_rows(connection, query: SeasonMetricQuery) -> list[dict[str, Any]]:
    if not (table_exists(connection, "lahman_pitching") and table_exists(connection, "lahman_people")):
        return []
    pitching_columns = {column.lower() for column in list_table_columns(connection, "lahman_pitching")}
    hbp_select = (
        "SUM(CAST(COALESCE(pch.hbp, '0') AS INTEGER)) AS hit_by_pitch,"
        if "hbp" in pitching_columns
        else "0 AS hit_by_pitch,"
    )
    parameters: list[Any] = [query.start_season, query.end_season]
    team_filter_sql = ""
    if query.team_filter_code:
        team_filter_sql = "AND upper(pch.teamid) = ?"
        parameters.append(query.team_filter_code.upper())
    season_select = (
        "MIN(CAST(pch.yearid AS INTEGER)) AS start_season, MAX(CAST(pch.yearid AS INTEGER)) AS end_season,"
        if query.aggregate_range
        else "CAST(pch.yearid AS INTEGER) AS season,"
    )
    season_group = "" if query.aggregate_range else "CAST(pch.yearid AS INTEGER), "
    rows = connection.execute(
        f"""
        SELECT
            {season_select}
            pch.playerid,
            ppl.namefirst,
            ppl.namelast,
            CASE WHEN COUNT(DISTINCT upper(pch.teamid)) = 1 THEN MIN(upper(pch.teamid)) ELSE 'MULTI' END AS team,
            SUM(CAST(COALESCE(pch.w, '0') AS INTEGER)) AS wins,
            SUM(CAST(COALESCE(pch.l, '0') AS INTEGER)) AS losses,
            SUM(CAST(COALESCE(pch.g, '0') AS INTEGER)) AS games,
            SUM(CAST(COALESCE(pch.gs, '0') AS INTEGER)) AS games_started,
            SUM(CAST(COALESCE(pch.sv, '0') AS INTEGER)) AS saves,
            SUM(CAST(COALESCE(pch.ipouts, '0') AS INTEGER)) AS ipouts,
            SUM(CAST(COALESCE(pch.h, '0') AS INTEGER)) AS hits_allowed,
            SUM(CAST(COALESCE(pch.er, '0') AS INTEGER)) AS earned_runs,
            SUM(CAST(COALESCE(pch.hr, '0') AS INTEGER)) AS home_runs_allowed,
            SUM(CAST(COALESCE(pch.bb, '0') AS INTEGER)) AS walks,
            {hbp_select}
            SUM(CAST(COALESCE(pch.so, '0') AS INTEGER)) AS strikeouts
        FROM lahman_pitching AS pch
        JOIN lahman_people AS ppl
          ON ppl.playerid = pch.playerid
        WHERE CAST(pch.yearid AS INTEGER) BETWEEN ? AND ?
          {team_filter_sql}
        GROUP BY {season_group} pch.playerid, ppl.namefirst, ppl.namelast
        """,
        tuple(parameters),
    ).fetchall()
    fip_constant = compute_historical_fip_constant(connection, query.start_season, query.end_season)
    candidates: list[dict[str, Any]] = []
    for row in rows:
        ipouts = safe_int(row["ipouts"]) or 0
        if not passes_sample_threshold(
            query.metric,
            {
                "ipouts": ipouts,
                "games": safe_int(row["games"]) or 0,
                "games_started": safe_int(row["games_started"]) or 0,
            },
        ):
            continue
        games_started = safe_int(row["games_started"]) or 0
        if query.minimum_starts is not None and games_started < query.minimum_starts:
            continue
        metric_value = select_historical_pitching_metric(query.metric.key, ipouts, row, fip_constant=fip_constant)
        if metric_value is None:
            continue
        hits_allowed = safe_int(row["hits_allowed"]) or 0
        walks = safe_int(row["walks"]) or 0
        scope_start, scope_end, scope_text = row_scope(row)
        candidates.append(
            {
                "season": scope_end,
                "scope_label": scope_text,
                "scope_start_season": scope_start,
                "scope_end_season": scope_end,
                "player_name": build_person_name(row["namefirst"], row["namelast"], row["playerid"]),
                "team": str(row["team"] or ""),
                "metric_value": float(metric_value),
                "sample_size": ipouts,
                "games": safe_int(row["games"]) or 0,
                "innings": outs_to_innings_notation(ipouts),
                "games_started": games_started,
                "era": (27.0 * (safe_int(row["earned_runs"]) or 0) / ipouts) if ipouts else None,
                "whip": ((hits_allowed + walks) / (ipouts / 3.0)) if ipouts else None,
                "fip": metric_value if query.metric.key == "fip" else select_historical_pitching_metric("fip", ipouts, row, fip_constant=fip_constant),
                "wins": safe_int(row["wins"]) or 0,
                "losses": safe_int(row["losses"]) or 0,
                "saves": safe_int(row["saves"]) or 0,
                "hits_allowed": hits_allowed,
                "earned_runs": safe_int(row["earned_runs"]) or 0,
                "home_runs_allowed": safe_int(row["home_runs_allowed"]) or 0,
                "hit_by_pitch": safe_int(row["hit_by_pitch"]) or 0,
                "walks": walks,
                "strikeouts": safe_int(row["strikeouts"]) or 0,
                "strikeouts_per_9": ((27.0 * (safe_int(row["strikeouts"]) or 0)) / ipouts) if ipouts else None,
                "walks_per_9": ((27.0 * walks) / ipouts) if ipouts else None,
                "hits_per_9": ((27.0 * hits_allowed) / ipouts) if ipouts else None,
                "home_runs_per_9": ((27.0 * (safe_int(row["home_runs_allowed"]) or 0)) / ipouts) if ipouts else None,
                "strikeout_to_walk": ((safe_int(row["strikeouts"]) or 0) / walks) if walks else None,
            }
        )
    return rank_rows(candidates, query)


def fetch_historical_fielder_rows(connection, query: SeasonMetricQuery) -> list[dict[str, Any]]:
    if not (table_exists(connection, "lahman_fielding") and table_exists(connection, "lahman_people")):
        return []
    parameters: list[Any] = [query.start_season, query.end_season]
    team_filter_sql = ""
    if query.team_filter_code:
        team_filter_sql = "AND upper(fld.teamid) = ?"
        parameters.append(query.team_filter_code.upper())
    season_select = (
        "MIN(CAST(fld.yearid AS INTEGER)) AS start_season, MAX(CAST(fld.yearid AS INTEGER)) AS end_season,"
        if query.aggregate_range
        else "CAST(fld.yearid AS INTEGER) AS season,"
    )
    season_group = "" if query.aggregate_range else "CAST(fld.yearid AS INTEGER), "
    rows = connection.execute(
        f"""
        SELECT
            {season_select}
            fld.playerid,
            ppl.namefirst,
            ppl.namelast,
            CASE WHEN COUNT(DISTINCT upper(fld.teamid)) = 1 THEN MIN(upper(fld.teamid)) ELSE 'MULTI' END AS team,
            SUM(CAST(COALESCE(fld.g, '0') AS INTEGER)) AS games,
            SUM(CAST(COALESCE(fld.po, '0') AS INTEGER)) AS putouts,
            SUM(CAST(COALESCE(fld.a, '0') AS INTEGER)) AS assists,
            SUM(CAST(COALESCE(fld.e, '0') AS INTEGER)) AS errors,
            SUM(CAST(COALESCE(fld.dp, '0') AS INTEGER)) AS double_plays
        FROM lahman_fielding AS fld
        JOIN lahman_people AS ppl
          ON ppl.playerid = fld.playerid
        WHERE CAST(fld.yearid AS INTEGER) BETWEEN ? AND ?
          {team_filter_sql}
        GROUP BY {season_group} fld.playerid, ppl.namefirst, ppl.namelast
        """,
        tuple(parameters),
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        games = safe_int(row["games"]) or 0
        if not passes_sample_threshold(query.metric, {"games": games}):
            continue
        putouts = safe_int(row["putouts"]) or 0
        assists = safe_int(row["assists"]) or 0
        errors = safe_int(row["errors"]) or 0
        chances = putouts + assists + errors
        fielding_pct = ((putouts + assists) / chances) if chances else None
        metric_value = select_historical_fielding_metric(query.metric.key, fielding_pct, row)
        if metric_value is None:
            continue
        scope_start, scope_end, scope_text = row_scope(row)
        candidates.append(
            {
                "season": scope_end,
                "scope_label": scope_text,
                "scope_start_season": scope_start,
                "scope_end_season": scope_end,
                "player_name": build_person_name(row["namefirst"], row["namelast"], row["playerid"]),
                "team": str(row["team"] or ""),
                "metric_value": float(metric_value),
                "sample_size": games,
                "games": games,
                "fielding_pct": fielding_pct,
                "errors": errors,
                "assists": assists,
                "putouts": putouts,
                "double_plays": safe_int(row["double_plays"]) or 0,
            }
        )
    return rank_rows(candidates, query)


def fetch_historical_team_rows(connection, query: SeasonMetricQuery) -> list[dict[str, Any]]:
    if not table_exists(connection, "lahman_teams"):
        return []
    season_select = (
        "MIN(CAST(yearid AS INTEGER)) AS start_season, MAX(CAST(yearid AS INTEGER)) AS end_season,"
        if query.aggregate_range
        else "CAST(yearid AS INTEGER) AS season,"
    )
    season_group = "teamid, name" if query.aggregate_range else "CAST(yearid AS INTEGER), teamid, name"
    rows = connection.execute(
        f"""
        SELECT
            {season_select}
            teamid,
            SUM(CAST(COALESCE(g, '0') AS INTEGER)) AS g,
            SUM(CAST(COALESCE(w, '0') AS INTEGER)) AS w,
            SUM(CAST(COALESCE(l, '0') AS INTEGER)) AS l,
            SUM(CAST(COALESCE(r, '0') AS INTEGER)) AS r,
            SUM(CAST(COALESCE(ab, '0') AS INTEGER)) AS ab,
            SUM(CAST(COALESCE(h, '0') AS INTEGER)) AS h,
            SUM(CAST(COALESCE(c_2b, '0') AS INTEGER)) AS c_2b,
            SUM(CAST(COALESCE(c_3b, '0') AS INTEGER)) AS c_3b,
            SUM(CAST(COALESCE(hr, '0') AS INTEGER)) AS hr,
            SUM(CAST(COALESCE(bb, '0') AS INTEGER)) AS bb,
            SUM(CAST(COALESCE(hbp, '0') AS INTEGER)) AS hbp,
            SUM(CAST(COALESCE(sf, '0') AS INTEGER)) AS sf,
            SUM(CAST(COALESCE(ra, '0') AS INTEGER)) AS ra,
            AVG(CAST(NULLIF(era, '') AS REAL)) AS era,
            AVG(CAST(NULLIF(fp, '') AS REAL)) AS fp,
            name
        FROM lahman_teams
        WHERE CAST(yearid AS INTEGER) BETWEEN ? AND ?
        GROUP BY {season_group}
        """,
        (query.start_season, query.end_season),
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        games = safe_int(row["g"]) or 0
        at_bats = safe_int(row["ab"]) or 0
        hits = safe_int(row["h"]) or 0
        doubles = safe_int(row["c_2b"]) or 0
        triples = safe_int(row["c_3b"]) or 0
        home_runs = safe_int(row["hr"]) or 0
        walks = safe_int(row["bb"]) or 0
        hbp = safe_int(row["hbp"]) or 0
        sacrifice_flies = safe_int(row["sf"]) or 0
        if not passes_sample_threshold(query.metric, {"games": games, "at_bats": at_bats}):
            continue
        singles = hits - doubles - triples - home_runs
        obp_denom = at_bats + walks + hbp + sacrifice_flies
        wins = safe_int(row["w"]) or 0
        losses = safe_int(row["l"]) or 0
        avg = (hits / at_bats) if at_bats else None
        obp = ((hits + walks + hbp) / obp_denom) if obp_denom else None
        slg = ((singles + (2 * doubles) + (3 * triples) + (4 * home_runs)) / at_bats) if at_bats else None
        ops = (obp + slg) if obp is not None and slg is not None else None
        metric_value = {
            "wins": float(wins),
            "losses": float(losses),
            "win_pct": (wins / (wins + losses)) if (wins + losses) else None,
            "runs": safe_float(row["r"]),
            "runs_per_game": ((safe_int(row["r"]) or 0) / games) if games else None,
            "runs_allowed": safe_float(row["ra"]),
            "runs_allowed_per_game": ((safe_int(row["ra"]) or 0) / games) if games else None,
            "doubles": safe_float(row["c_2b"]),
            "triples": safe_float(row["c_3b"]),
            "walks": safe_float(row["bb"]),
            "walks_per_game": ((safe_int(row["bb"]) or 0) / games) if games else None,
            "avg": avg,
            "obp": obp,
            "slg": slg,
            "ops": ops,
            "home_runs": float(home_runs),
            "home_runs_per_game": (home_runs / games) if games else None,
            "hits": float(hits),
            "hits_per_game": (hits / games) if games else None,
            "fielding_pct": safe_float(row["fp"]),
        }.get(query.metric.key)
        if metric_value is None:
            continue
        scope_start, scope_end, scope_text = row_scope(row)
        candidates.append(
            {
                "season": scope_end,
                "scope_label": scope_text,
                "scope_start_season": scope_start,
                "scope_end_season": scope_end,
                "team_name": str(row["name"] or row["teamid"] or ""),
                "team": str(row["teamid"] or ""),
                "metric_value": float(metric_value),
                "sample_size": games,
                "games": games,
                "wins": wins,
                "losses": losses,
                "win_pct": (wins / (wins + losses)) if (wins + losses) else None,
                "runs": safe_int(row["r"]) or 0,
                "runs_allowed": safe_int(row["ra"]) or 0,
                "doubles": doubles,
                "triples": triples,
                "walks": walks,
                "avg": avg,
                "obp": obp,
                "slg": slg,
                "ops": ops,
                "home_runs": home_runs,
                "hits": hits,
                "fielding_pct": safe_float(row["fp"]),
            }
        )
    return rank_rows(candidates, query)


def fetch_statcast_season_rows(connection, query: SeasonMetricQuery) -> list[dict[str, Any]]:
    if query.entity_scope == "team":
        return fetch_statcast_team_rows(connection, query)
    return fetch_statcast_batter_rows(connection, query)


def fetch_provider_season_rows(query: SeasonMetricQuery) -> list[dict[str, Any]]:
    cache: dict[tuple[str, int, bool], list[dict[str, Any]]] = {}
    candidates: list[dict[str, Any]] = []
    if query.metric.provider_batting_column and query.provider_group_preference != "pitching":
        candidates.extend(
            fetch_provider_metric_group_rows(
                query,
                group="batting",
                column_name=query.metric.provider_batting_column,
                cache=cache,
            )
        )
    if query.metric.provider_pitching_column and query.provider_group_preference != "batting":
        candidates.extend(
            fetch_provider_metric_group_rows(
                query,
                group="pitching",
                column_name=query.metric.provider_pitching_column,
                cache=cache,
            )
        )
    return rank_rows(candidates, query)


def fetch_provider_metric_group_rows(
    query: SeasonMetricQuery,
    *,
    group: str,
    column_name: str,
    cache: dict[tuple[str, int, bool], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    rows = fetch_provider_group_rows(
        group,
        column_name,
        query.start_season,
        query.metric.provider_qualified_only,
        cache,
        team_filter=query.team_filter_code,
        minimum_starts=query.minimum_starts,
    )
    candidates: list[dict[str, Any]] = []
    for row in rows:
        metric_value = safe_float(row.get("metric_value"))
        if metric_value is None:
            continue
        candidates.append(
            {
                "season": query.start_season,
                "player_name": str(row.get("name") or ""),
                "team": str(row.get("team") or ""),
                "metric_value": metric_value,
                "sample_size": safe_float(row.get("starts")) or 0.0,
                "group": group,
                "starts": safe_int(row.get("starts")) or 0,
            }
        )
    return candidates


def fetch_statcast_batter_rows(connection, query: SeasonMetricQuery) -> list[dict[str, Any]]:
    rows = fetch_statcast_batter_summary_rows(connection, query)
    if not rows:
        rows = fetch_statcast_batter_event_rows(connection, query)
    if not rows:
        return []
    candidates: list[dict[str, Any]] = []
    for row in rows:
        metrics = statcast_batter_metric_values(row)
        if not passes_sample_threshold(query.metric, metrics):
            continue
        metric_value = metrics.get(query.metric.key)
        if metric_value is None:
            continue
        scope_start, scope_end, scope_text = row_scope(row)
        candidates.append(
            {
                "season": scope_end,
                "scope_label": scope_text,
                "scope_start_season": scope_start,
                "scope_end_season": scope_end,
                "player_name": str(row["player_name"] or ""),
                "team": str(row["team"] or ""),
                "metric_value": float(metric_value),
                "sample_size": float(metrics.get(query.metric.sample_basis or "plate_appearances") or 0),
                "games": safe_int(row["games"]) or 0,
                "plate_appearances": safe_int(row["plate_appearances"]) or 0,
                "at_bats": safe_int(row["at_bats"]) or 0,
                "hits": safe_int(row["hits"]) or 0,
                "singles": safe_int(row["singles"]) or 0,
                "doubles": safe_int(row["doubles"]) or 0,
                "triples": safe_int(row["triples"]) or 0,
                "home_runs": safe_int(row["home_runs"]) or 0,
                "walks": safe_int(row["walks"]) or 0,
                "strikeouts": safe_int(row["strikeouts"]) or 0,
                "runs_batted_in": safe_int(row["runs_batted_in"]) or 0,
                "avg": metrics.get("avg"),
                "obp": metrics.get("obp"),
                "slg": metrics.get("slg"),
                "ops": metrics.get("ops"),
                "xBA": metrics.get("xba"),
                "xwOBA": metrics.get("xwoba"),
                "xSLG": metrics.get("xslg"),
                "hard_hit_rate": metrics.get("hard_hit_rate"),
                "barrel_rate": metrics.get("barrel_rate"),
                "avg_exit_velocity": metrics.get("avg_exit_velocity"),
                "max_exit_velocity": metrics.get("max_exit_velocity"),
                "avg_bat_speed": metrics.get("avg_bat_speed"),
                "max_bat_speed": metrics.get("max_bat_speed"),
                "hits_per_game": metrics.get("hits_per_game"),
                "home_runs_per_game": metrics.get("home_runs_per_game"),
                "walks_per_game": metrics.get("walks_per_game"),
                "strikeouts_per_game": metrics.get("strikeouts_per_game"),
                "rbi_per_game": metrics.get("rbi_per_game"),
            }
        )
    return rank_rows(candidates, query)


def fetch_statcast_batter_summary_rows(connection, query: SeasonMetricQuery):
    if not table_exists(connection, "statcast_batter_games"):
        return []
    parameters: list[Any] = [query.start_season, query.end_season]
    team_filter_sql = ""
    if query.team_filter_code:
        team_filter_sql = "AND upper(team) = ?"
        parameters.append(query.team_filter_code.upper())
    season_select = (
        "MIN(season) AS start_season, MAX(season) AS end_season,"
        if query.aggregate_range
        else "season,"
    )
    season_group = "batter_id" if query.aggregate_range else "season, batter_id"
    rows = connection.execute(
        f"""
        SELECT
            {season_select}
            batter_id,
            MIN(batter_name) AS player_name,
            CASE WHEN COUNT(DISTINCT upper(team)) = 1 THEN MIN(upper(team)) ELSE 'MULTI' END AS team,
            COUNT(DISTINCT game_pk) AS games,
            SUM(plate_appearances) AS plate_appearances,
            SUM(at_bats) AS at_bats,
            SUM(hits) AS hits,
            SUM(singles) AS singles,
            SUM(doubles) AS doubles,
            SUM(triples) AS triples,
            SUM(home_runs) AS home_runs,
            SUM(walks) AS walks,
            SUM(strikeouts) AS strikeouts,
            SUM(runs_batted_in) AS runs_batted_in,
            SUM(batted_ball_events) AS batted_ball_events,
            SUM(xba_numerator) AS xba_numerator,
            SUM(xwoba_numerator) AS xwoba_numerator,
            SUM(xwoba_denom) AS xwoba_denom,
            SUM(xslg_numerator) AS xslg_numerator,
            SUM(hard_hit_bbe) AS hard_hit_bbe,
            SUM(barrel_bbe) AS barrel_bbe,
            SUM(launch_speed_sum) AS launch_speed_sum,
            SUM(launch_speed_count) AS launch_speed_count,
            MAX(max_launch_speed) AS max_launch_speed,
            AVG(avg_bat_speed) AS avg_bat_speed,
            MAX(max_bat_speed) AS max_bat_speed
        FROM statcast_batter_games
        WHERE season BETWEEN ? AND ?
          {team_filter_sql}
        GROUP BY {season_group}
        """,
        tuple(parameters),
    ).fetchall()
    return rows


def fetch_statcast_batter_event_rows(connection, query: SeasonMetricQuery):
    if not table_exists(connection, "statcast_events"):
        return []
    parameters: list[Any] = [query.start_season, query.end_season]
    team_filter_sql = ""
    if query.team_filter_code:
        team_filter_sql = "AND upper(batting_team) = ?"
        parameters.append(query.team_filter_code.upper())
    season_select = (
        "MIN(season) AS start_season, MAX(season) AS end_season,"
        if query.aggregate_range
        else "season,"
    )
    season_group = "batter_id" if query.aggregate_range else "season, batter_id"
    return connection.execute(
        f"""
        SELECT
            {season_select}
            batter_id,
            MIN(batter_name) AS player_name,
            CASE WHEN COUNT(DISTINCT upper(batting_team)) = 1 THEN MIN(upper(batting_team)) ELSE 'MULTI' END AS team,
            COUNT(DISTINCT game_pk) AS games,
            COUNT(*) AS plate_appearances,
            SUM(is_ab) AS at_bats,
            SUM(is_hit) AS hits,
            SUM(CASE WHEN event = 'single' THEN 1 ELSE 0 END) AS singles,
            SUM(CASE WHEN event = 'double' THEN 1 ELSE 0 END) AS doubles,
            SUM(CASE WHEN event = 'triple' THEN 1 ELSE 0 END) AS triples,
            SUM(CASE WHEN event = 'home_run' THEN 1 ELSE 0 END) AS home_runs,
            SUM(CASE WHEN event IN ('walk', 'intent_walk') THEN 1 ELSE 0 END) AS walks,
            SUM(is_strikeout) AS strikeouts,
            SUM(runs_batted_in) AS runs_batted_in,
            SUM(CASE WHEN launch_speed IS NOT NULL THEN 1 ELSE 0 END) AS batted_ball_events,
            SUM(COALESCE(estimated_ba, 0.0)) AS xba_numerator,
            SUM(COALESCE(estimated_woba, 0.0)) AS xwoba_numerator,
            SUM(CASE WHEN estimated_woba IS NOT NULL THEN 1 ELSE 0 END) AS xwoba_denom,
            SUM(COALESCE(estimated_slg, 0.0)) AS xslg_numerator,
            SUM(CASE WHEN launch_speed >= 95 THEN 1 ELSE 0 END) AS hard_hit_bbe,
            SUM(CASE WHEN launch_speed IS NOT NULL AND launch_angle IS NOT NULL AND launch_speed >= 98 AND launch_angle BETWEEN 26 AND 30 THEN 1 ELSE 0 END) AS barrel_bbe,
            SUM(COALESCE(launch_speed, 0.0)) AS launch_speed_sum,
            SUM(CASE WHEN launch_speed IS NOT NULL THEN 1 ELSE 0 END) AS launch_speed_count,
            MAX(launch_speed) AS max_launch_speed,
            AVG(bat_speed) AS avg_bat_speed,
            MAX(bat_speed) AS max_bat_speed
        FROM statcast_events
        WHERE season BETWEEN ? AND ?
          AND event <> ''
          {team_filter_sql}
        GROUP BY {season_group}
        """,
        tuple(parameters),
    ).fetchall()


def fetch_statcast_team_rows(connection, query: SeasonMetricQuery) -> list[dict[str, Any]]:
    if not table_exists(connection, "statcast_team_games"):
        return []
    season_select = (
        "MIN(season) AS start_season, MAX(season) AS end_season,"
        if query.aggregate_range
        else "season,"
    )
    season_group = "team" if query.aggregate_range else "season, team"
    rows = connection.execute(
        f"""
        SELECT
            {season_select}
            team,
            MIN(team_name) AS team_name,
            COUNT(*) AS games,
            SUM(plate_appearances) AS plate_appearances,
            SUM(at_bats) AS at_bats,
            SUM(hits) AS hits,
            SUM(strikeouts) AS strikeouts,
            SUM(batted_ball_events) AS batted_ball_events,
            SUM(xba_numerator) AS xba_numerator,
            SUM(xwoba_numerator) AS xwoba_numerator,
            SUM(xwoba_denom) AS xwoba_denom,
            SUM(xslg_numerator) AS xslg_numerator,
            SUM(hard_hit_bbe) AS hard_hit_bbe,
            SUM(barrel_bbe) AS barrel_bbe,
            SUM(launch_speed_sum) AS launch_speed_sum,
            SUM(launch_speed_count) AS launch_speed_count
        FROM statcast_team_games
        WHERE season BETWEEN ? AND ?
        GROUP BY {season_group}
        """,
        (query.start_season, query.end_season),
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        metrics = statcast_team_metric_values(row)
        if not passes_sample_threshold(query.metric, metrics):
            continue
        metric_value = metrics.get(query.metric.key)
        if metric_value is None:
            continue
        scope_start, scope_end, scope_text = row_scope(row)
        candidates.append(
            {
                "season": scope_end,
                "scope_label": scope_text,
                "scope_start_season": scope_start,
                "scope_end_season": scope_end,
                "team_name": str(row["team_name"] or row["team"] or ""),
                "team": str(row["team"] or ""),
                "metric_value": float(metric_value),
                "sample_size": float(metrics.get(query.metric.sample_basis or "plate_appearances") or 0),
                "games": safe_int(row["games"]) or 0,
                "plate_appearances": safe_int(row["plate_appearances"]) or 0,
                "at_bats": safe_int(row["at_bats"]) or 0,
                "hits": safe_int(row["hits"]) or 0,
                "strikeouts": safe_int(row["strikeouts"]) or 0,
                "xBA": metrics.get("xba"),
                "xwOBA": metrics.get("xwoba"),
                "xSLG": metrics.get("xslg"),
                "hard_hit_rate": metrics.get("hard_hit_rate"),
                "barrel_rate": metrics.get("barrel_rate"),
                "avg_exit_velocity": metrics.get("avg_exit_velocity"),
            }
        )
    return rank_rows(candidates, query)


def statcast_batter_metric_values(row) -> dict[str, float | None]:
    at_bats = safe_int(row["at_bats"]) or 0
    launch_speed_count = safe_int(row["launch_speed_count"]) or 0
    plate_appearances = safe_int(row["plate_appearances"]) or 0
    games = safe_int(row["games"]) or 0
    batted_ball_events = safe_int(row["batted_ball_events"]) or 0
    xwoba_denom = safe_float(row["xwoba_denom"]) or 0.0
    launch_speed_sum = safe_float(row["launch_speed_sum"]) or 0.0
    hits = safe_int(row["hits"]) or 0
    walks = safe_int(row["walks"]) or 0
    singles = safe_int(row["singles"]) or 0
    doubles = safe_int(row["doubles"]) or 0
    triples = safe_int(row["triples"]) or 0
    home_runs = safe_int(row["home_runs"]) or 0
    avg = (hits / at_bats) if at_bats else None
    obp = ((hits + walks) / plate_appearances) if plate_appearances else None
    slg = ((singles + (2 * doubles) + (3 * triples) + (4 * home_runs)) / at_bats) if at_bats else None
    return {
        "plate_appearances": float(plate_appearances),
        "at_bats": float(at_bats),
        "games": float(games),
        "batted_ball_events": float(batted_ball_events),
        "xwoba_denom": float(xwoba_denom),
        "launch_speed_count": float(launch_speed_count),
        "avg": avg,
        "obp": obp,
        "slg": slg,
        "ops": (obp + slg) if obp is not None and slg is not None else None,
        "xba": ((safe_float(row["xba_numerator"]) or 0.0) / at_bats) if at_bats else None,
        "xwoba": ((safe_float(row["xwoba_numerator"]) or 0.0) / xwoba_denom) if xwoba_denom else None,
        "xslg": ((safe_float(row["xslg_numerator"]) or 0.0) / at_bats) if at_bats else None,
        "hard_hit_rate": ((safe_int(row["hard_hit_bbe"]) or 0) / batted_ball_events) if batted_ball_events else None,
        "barrel_rate": ((safe_int(row["barrel_bbe"]) or 0) / batted_ball_events) if batted_ball_events else None,
        "avg_exit_velocity": (launch_speed_sum / launch_speed_count) if launch_speed_count else None,
        "max_exit_velocity": safe_float(row["max_launch_speed"]),
        "avg_bat_speed": safe_float(row["avg_bat_speed"]),
        "max_bat_speed": safe_float(row["max_bat_speed"]),
        "singles": float(singles),
        "doubles": float(doubles),
        "triples": float(triples),
        "home_runs": safe_float(row["home_runs"]),
        "home_runs_per_game": (home_runs / games) if games else None,
        "hits": safe_float(row["hits"]),
        "hits_per_game": (hits / games) if games else None,
        "walks": safe_float(row["walks"]),
        "walks_per_game": (walks / games) if games else None,
        "strikeouts": safe_float(row["strikeouts"]),
        "strikeouts_per_game": ((safe_int(row["strikeouts"]) or 0) / games) if games else None,
        "rbi": safe_float(row["runs_batted_in"]),
        "rbi_per_game": ((safe_int(row["runs_batted_in"]) or 0) / games) if games else None,
    }


def statcast_team_metric_values(row) -> dict[str, float | None]:
    at_bats = safe_int(row["at_bats"]) or 0
    launch_speed_count = safe_int(row["launch_speed_count"]) or 0
    plate_appearances = safe_int(row["plate_appearances"]) or 0
    batted_ball_events = safe_int(row["batted_ball_events"]) or 0
    xwoba_denom = safe_float(row["xwoba_denom"]) or 0.0
    launch_speed_sum = safe_float(row["launch_speed_sum"]) or 0.0
    return {
        "games": float(safe_int(row["games"]) or 0),
        "plate_appearances": float(plate_appearances),
        "at_bats": float(at_bats),
        "batted_ball_events": float(batted_ball_events),
        "xwoba_denom": float(xwoba_denom),
        "launch_speed_count": float(launch_speed_count),
        "xba": ((safe_float(row["xba_numerator"]) or 0.0) / at_bats) if at_bats else None,
        "xwoba": ((safe_float(row["xwoba_numerator"]) or 0.0) / xwoba_denom) if xwoba_denom else None,
        "xslg": ((safe_float(row["xslg_numerator"]) or 0.0) / at_bats) if at_bats else None,
        "hard_hit_rate": ((safe_int(row["hard_hit_bbe"]) or 0) / batted_ball_events) if batted_ball_events else None,
        "barrel_rate": ((safe_int(row["barrel_bbe"]) or 0) / batted_ball_events) if batted_ball_events else None,
        "avg_exit_velocity": (launch_speed_sum / launch_speed_count) if launch_speed_count else None,
        "hits": safe_float(row["hits"]),
        "hits_per_game": ((safe_int(row["hits"]) or 0) / (safe_int(row["games"]) or 0)) if (safe_int(row["games"]) or 0) else None,
        "strikeouts": safe_float(row["strikeouts"]),
        "strikeouts_per_game": ((safe_int(row["strikeouts"]) or 0) / (safe_int(row["games"]) or 0)) if (safe_int(row["games"]) or 0) else None,
    }


def row_scope(row) -> tuple[int, int, str]:
    keys = set(row.keys()) if hasattr(row, "keys") else set()
    start_season = safe_int(row["start_season"]) if "start_season" in keys else None
    end_season = safe_int(row["end_season"]) if "end_season" in keys else None
    if start_season is not None and end_season is not None:
        if start_season == end_season:
            return start_season, end_season, str(end_season)
        return start_season, end_season, f"{start_season}-{end_season}"
    season = safe_int(row["season"]) or 0
    return season, season, str(season)


def compute_historical_fip_constant(connection, start_season: int, end_season: int) -> float | None:
    if not table_exists(connection, "lahman_pitching"):
        return None
    pitching_columns = {column.lower() for column in list_table_columns(connection, "lahman_pitching")}
    hbp_select = (
        "SUM(CAST(COALESCE(hbp, '0') AS INTEGER)) AS hit_by_pitch"
        if "hbp" in pitching_columns
        else "0 AS hit_by_pitch"
    )
    row = connection.execute(
        f"""
        SELECT
            SUM(CAST(COALESCE(ipouts, '0') AS INTEGER)) AS ipouts,
            SUM(CAST(COALESCE(er, '0') AS INTEGER)) AS earned_runs,
            SUM(CAST(COALESCE(hr, '0') AS INTEGER)) AS home_runs_allowed,
            SUM(CAST(COALESCE(bb, '0') AS INTEGER)) AS walks,
            {hbp_select},
            SUM(CAST(COALESCE(so, '0') AS INTEGER)) AS strikeouts
        FROM lahman_pitching
        WHERE CAST(yearid AS INTEGER) BETWEEN ? AND ?
        """,
        (start_season, end_season),
    ).fetchone()
    if row is None:
        return None
    ipouts = safe_int(row["ipouts"]) or 0
    if ipouts <= 0:
        return None
    earned_runs = safe_int(row["earned_runs"]) or 0
    home_runs_allowed = safe_int(row["home_runs_allowed"]) or 0
    walks = safe_int(row["walks"]) or 0
    hit_by_pitch = safe_int(row["hit_by_pitch"]) or 0
    strikeouts = safe_int(row["strikeouts"]) or 0
    innings_pitched = ipouts / 3.0
    league_era = (earned_runs * 9.0 / innings_pitched) if innings_pitched else None
    if league_era is None:
        return None
    component = ((13.0 * home_runs_allowed) + (3.0 * (walks + hit_by_pitch)) - (2.0 * strikeouts)) / innings_pitched
    return league_era - component


def passes_sample_threshold(metric: SeasonMetricSpec, values: dict[str, float | int | None]) -> bool:
    if metric.sample_basis is None or metric.min_sample_size <= 0:
        return True
    sample_value = values.get(metric.sample_basis)
    try:
        return float(sample_value or 0) >= float(metric.min_sample_size)
    except (TypeError, ValueError):
        return False


def rank_rows(rows: list[dict[str, Any]], query: SeasonMetricQuery) -> list[dict[str, Any]]:
    rows = [row for row in rows if row.get("metric_value") is not None]
    rows.sort(
        key=lambda row: (
            -float(row["metric_value"]) if query.sort_desc else float(row["metric_value"]),
            -(row.get("sample_size") or 0.0),
            int(row.get("scope_end_season") or row.get("season") or 0),
            str(row.get("player_name") or row.get("team_name") or ""),
        )
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def build_season_metric_summary(query: SeasonMetricQuery, rows: list[dict[str, Any]]) -> str:
    leader = rows[0]
    subject_label = leader.get("player_name") or leader.get("team_name") or "Unknown"
    value_text = f"{float(leader['metric_value']):{query.metric.formatter}}"
    filter_text = f" for {query.team_filter_name}" if query.team_filter_name else ""
    subject_phrase = "team" if query.entity_scope == "team" else query.role
    leader_scope = str(leader.get("scope_label") or leader.get("season") or "")
    scope_parenthetical = f" ({leader_scope})" if leader_scope and not query.aggregate_range else ""
    summary = (
        f"For {query.scope_label}{filter_text}, the {query.descriptor} {subject_phrase} by {query.metric.label} "
        f"is {subject_label}{scope_parenthetical} at {value_text}."
    )
    trailing = rows[1:4]
    if trailing:
        parts: list[str] = []
        for row in trailing:
            row_label = row.get("player_name") or row.get("team_name") or "Unknown"
            row_scope = row.get("scope_label") or row.get("season")
            row_scope_text = f" ({row_scope})" if row_scope and not query.aggregate_range else ""
            parts.append(
                f"{row_label}{row_scope_text} {float(row['metric_value']):{query.metric.formatter}}"
            )
        summary = f"{summary} Next on the board: " + "; ".join(parts) + "."
    return summary


def build_citation(query: SeasonMetricQuery) -> str:
    if query.metric.source_family == "provider":
        return "FanGraphs season leaderboards via pybaseball"
    if query.metric.source_family == "statcast":
        return "Local Statcast season summaries aggregated from synced public Statcast data"
    if query.entity_scope == "team":
        return "Lahman Teams table"
    if query.role == "pitcher":
        return "Lahman Pitching and People tables"
    if query.role == "fielder":
        return "Lahman Fielding and People tables"
    return "Lahman Batting and People tables"
