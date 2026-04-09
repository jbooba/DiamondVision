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
from .storage import (
    STATCAST_HISTORY_BATTER_TABLE,
    STATCAST_HISTORY_PITCHER_TABLE,
    list_table_columns,
    table_exists,
)
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
TEAM_HISTORY_AGGREGATE_HINTS = (
    "franchise",
    "franchises",
    "all-time franchise",
    "team history",
    "franchise history",
    "combined all-time",
    "all-time combined",
    "combined career",
    "career combined",
    "cumulative",
    "cumulative team",
    "cumulative franchise",
    "across all seasons",
    "across every season",
    "all seasons combined",
    "combined batting average",
    "combined ops",
    "combined obp",
    "combined slg",
)
WINNING_RECORD_HINTS = (
    "with a winning record",
    "with winning record",
    "with a winning percentage above .500",
    "above .500",
    "over .500",
)
LOSING_RECORD_HINTS = (
    "with a losing record",
    "with losing record",
    "with a winning percentage below .500",
    "below .500",
    "under .500",
)
QUALIFIER_CLAUSE_PATTERN = re.compile(
    r"\b(?:with|and)?\s*(?:a\s+)?(?:minimum|min|at\s+least)\s+(?:of\s+)?[a-z0-9-]+(?:\s+[a-z0-9-]+){0,3}\s+"
    r"(?:starts?|gs|games?|plate\s+appearances|pa|at\s+bats|ab|innings|ip|home\s+runs?|hr|hits?|walks?|strikeouts?|outs?)\b",
    re.IGNORECASE,
)
METRIC_NORMALIZATION_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(
            r"\bstr(?:u|o)ck?\s+out(?:\s+the\s+(?:most|least|fewest|highest|lowest))?\s+times?\b",
            re.IGNORECASE,
        ),
        "strikeouts",
    ),
    (
        re.compile(
            r"\b(?:most|least|fewest|highest|lowest)\s+times?\s+str(?:u|o)ck?\s+out\b",
            re.IGNORECASE,
        ),
        "strikeouts",
    ),
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
EXPLICIT_HITTER_ENTITY_HINTS = (
    " hitter ",
    " hitters ",
    " batter ",
    " batters ",
    " position player ",
    " position players ",
    " non-pitcher ",
    " non pitchers ",
    " non-pitchers ",
)
STATCAST_HISTORY_NON_METRIC_COLUMNS = {
    "last_name_first_name",
    "player_id",
    "year",
    "pitch_hand",
    "p_formatted_ip",
}
STATCAST_HISTORY_TABLE_CONFIG = (
    {
        "table_name": STATCAST_HISTORY_BATTER_TABLE,
        "role": "hitter",
        "name_column": "last_name_first_name",
        "player_id_column": "player_id",
        "season_column": "year",
    },
    {
        "table_name": STATCAST_HISTORY_PITCHER_TABLE,
        "role": "pitcher",
        "name_column": "last_name_first_name",
        "player_id_column": "player_id",
        "season_column": "year",
    },
)
STATCAST_HISTORY_RATE_ALIASES: dict[str, tuple[str, ...]] = {
    "batting_avg": ("avg", "ba", "batting average"),
    "slg_percent": ("slg", "slugging percentage", "slugging"),
    "on_base_percent": ("obp", "on base percentage", "on-base percentage"),
    "on_base_plus_slg": ("ops", "on base plus slugging", "on-base plus slugging"),
    "isolated_power": ("iso", "isolated power"),
    "k_percent": ("k percent", "k%", "strikeout rate", "strikeout percentage"),
    "bb_percent": ("bb percent", "bb%", "walk rate", "walk percentage"),
    "xba": ("xba", "expected batting average"),
    "xslg": ("xslg", "expected slugging"),
    "woba": ("woba",),
    "xwoba": ("xwoba", "expected woba"),
    "xobp": ("xobp", "expected obp"),
    "xiso": ("xiso", "expected iso"),
    "wobacon": ("wobacon", "woba on contact"),
    "xwobacon": ("xwobacon", "expected woba on contact"),
    "bacon": ("bacon", "batting average on contact"),
    "xbacon": ("xbacon", "expected batting average on contact"),
    "exit_velocity_avg": ("average exit velocity", "avg exit velocity", "avg ev", "exit velocity", "ev"),
    "launch_angle_avg": ("average launch angle", "avg launch angle", "launch angle"),
    "hard_hit_percent": ("hard-hit rate", "hard hit rate", "hard-hit percentage"),
    "barrel_batted_rate": ("barrel rate", "barrel percentage"),
    "sweet_spot_percent": ("sweet spot rate", "sweet spot percentage"),
    "avg_swing_speed": ("average swing speed", "avg swing speed", "swing speed"),
    "avg_swing_length": ("average swing length", "avg swing length", "swing length"),
    "fast_swing_rate": ("fast swing rate",),
    "swing_percent": ("swing rate", "swing percentage", "swing%"),
    "oz_swing_percent": (
        "chase percent",
        "chase rate",
        "chase percentage",
        "chase%",
        "out-of-zone swing rate",
        "out of zone swing rate",
        "out-of-zone swing percentage",
        "out of zone swing percentage",
        "o-swing%",
        "o swing%",
    ),
    "z_swing_percent": (
        "zone swing rate",
        "zone swing percentage",
        "zone swing%",
        "z-swing%",
        "z swing%",
    ),
    "oz_contact_percent": (
        "chase contact rate",
        "chase contact percentage",
        "chase contact%",
        "out-of-zone contact rate",
        "out of zone contact rate",
        "o-contact%",
        "o contact%",
    ),
    "iz_contact_percent": (
        "zone contact rate",
        "zone contact percentage",
        "zone contact%",
        "z-contact%",
        "z contact%",
    ),
    "swords": ("swords",),
    "attack_angle": ("attack angle",),
    "attack_direction": ("attack direction",),
    "ideal_angle_rate": ("ideal angle rate",),
    "vertical_swing_path": ("vertical swing path",),
    "p_era": ("era", "earned run average"),
    "p_opp_batting_avg": ("opponent batting average", "opp batting average"),
    "p_opp_on_base_avg": ("opponent on base average", "opp obp"),
}
STATCAST_HISTORY_FALLBACK_COLUMNS: dict[tuple[str, str], str] = {
    ("hitter", "xba"): "xba",
    ("hitter", "xwoba"): "xwoba",
    ("hitter", "xslg"): "xslg",
    ("hitter", "avg_exit_velocity"): "exit_velocity_avg",
    ("hitter", "hard_hit_rate"): "hard_hit_percent",
    ("hitter", "barrel_rate"): "barrel_batted_rate",
}
STATCAST_HISTORY_PITCH_LABELS = {
    "ff": "four-seam fastball",
    "sl": "slider",
    "ch": "changeup",
    "cu": "curveball",
    "si": "sinker",
    "fc": "cutter",
    "fs": "splitter",
    "kn": "knuckleball",
    "st": "sweeper",
    "sv": "slurve",
    "fo": "forkball",
    "sc": "screwball",
    "fastball": "fastball",
    "breaking": "breaking ball",
    "offspeed": "offspeed pitch",
}


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
    dynamic_table_name: str | None = None
    dynamic_value_column: str | None = None
    dynamic_aggregate_mode: str | None = None


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
    team_record_filter: str | None = None
    exclude_pitcher_only_hitters: bool = False


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
    SeasonMetricSpec("singles", "Singles", ("singles",), "historical", "hitter", "player", True, ".0f", "plate_appearances", 5),
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
    SeasonMetricSpec("run_differential", "Run Differential", ("run differential", "run diff", "run difference", "rd"), "historical", "team", "team", True, ".0f", "games", 10),
    SeasonMetricSpec("run_differential_per_game", "Run Differential/G", ("run differential per game", "run diff per game", "rd/g"), "historical", "team", "team", True, ".2f", "games", 10),
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
    SeasonMetricSpec("singles", "Singles", ("singles",), "statcast", "hitter", "player", True, ".0f", "plate_appearances", 5),
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
        elif query.metric.source_family == "statcast_history":
            rows = fetch_statcast_history_rows(connection, query)
        elif query.metric.source_family == "statcast":
            rows: list[dict[str, Any]] = []
            history_fallback = None
            if query.entity_scope == "player":
                history_fallback = build_statcast_history_fallback_query(query)
            if query.aggregate_range and history_fallback is not None:
                fallback_rows = fetch_statcast_history_rows(connection, history_fallback)
                if fallback_rows:
                    query = history_fallback
                    rows = fallback_rows
            if not rows:
                rows = fetch_statcast_season_rows(connection, query)
            if not rows and history_fallback is not None:
                fallback_rows = fetch_statcast_history_rows(connection, history_fallback)
                if fallback_rows:
                    query = history_fallback
                    rows = fallback_rows
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
        metric = find_statcast_history_metric(connection, metric_search_text)
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
    explicit_span = extract_season_span(question, current_season) is not None
    explicit_season = extract_referenced_season(question, current_season) is not None
    source_scope_family = "statcast" if metric.source_family == "statcast_history" else metric.source_family
    start_season, end_season, scope_label, aggregate_range = resolve_season_scope(question, current_season, source_scope_family)
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
    if (
        entity_scope == "team"
        and metric.source_family == "historical"
        and aggregate_range
        and not explicit_span
        and not explicit_season
        and not any(token in lowered for token in TEAM_HISTORY_AGGREGATE_HINTS)
    ):
        aggregate_range = False
    team_filter_code = None
    team_filter_name = None
    if entity_scope == "player":
        team_filter_code, team_filter_name = resolve_question_team_filter(connection, settings, question, start_season, end_season)
    team_record_filter = parse_team_record_filter(lowered) if entity_scope == "team" else None
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
        team_record_filter=team_record_filter,
        exclude_pitcher_only_hitters=(
            entity_scope == "player"
            and role == "hitter"
            and any(token in lowered for token in EXPLICIT_HITTER_ENTITY_HINTS)
        ),
    )


def find_season_metric(lowered_question: str) -> SeasonMetricSpec | None:
    best_match: tuple[int, SeasonMetricSpec] | None = None
    for metric in SEASON_METRICS:
        for alias in metric.aliases:
            alias_lower = alias.lower().strip()
            if not alias_lower:
                continue
            pattern = rf"(?<![a-z0-9]){re.escape(alias_lower)}(?![a-z0-9])"
            score = metric_alias_match_score(alias_lower, pattern, lowered_question)
            if score is None:
                continue
            score += metric_match_bonus(metric, lowered_question)
            if best_match is None or score > best_match[0]:
                best_match = (score, metric)
    return best_match[1] if best_match else None


def metric_alias_match_score(alias_lower: str, pattern: str, lowered_question: str) -> int | None:
    if re.search(pattern, lowered_question) is None:
        return None
    if alias_lower == "era":
        scrubbed = re.sub(r"\b(?:in\s+the\s+)?statcast era\b", " ", lowered_question)
        if re.search(pattern, scrubbed) is None:
            return None
    return len(alias_lower)


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
        team_record_filter=query.team_record_filter,
        exclude_pitcher_only_hitters=query.exclude_pitcher_only_hitters,
    )


def build_statcast_history_fallback_metric(metric: SeasonMetricSpec, *, role: str) -> SeasonMetricSpec | None:
    column = STATCAST_HISTORY_FALLBACK_COLUMNS.get((role, metric.key))
    if not column:
        return None
    table_name = STATCAST_HISTORY_BATTER_TABLE if role == "hitter" else STATCAST_HISTORY_PITCHER_TABLE
    spec = build_statcast_history_metric_spec(
        column=column,
        table_name=table_name,
        role=role,
    )
    return SeasonMetricSpec(
        key=spec.key,
        label=metric.label,
        aliases=metric.aliases,
        source_family=spec.source_family,
        role=spec.role,
        entity_scope=spec.entity_scope,
        higher_is_better=metric.higher_is_better,
        formatter=metric.formatter,
        sample_basis=spec.sample_basis,
        min_sample_size=metric.min_sample_size,
        provider_metric_name=spec.provider_metric_name,
        provider_batting_column=spec.provider_batting_column,
        provider_pitching_column=spec.provider_pitching_column,
        provider_qualified_only=spec.provider_qualified_only,
        dynamic_table_name=spec.dynamic_table_name,
        dynamic_value_column=spec.dynamic_value_column,
        dynamic_aggregate_mode=spec.dynamic_aggregate_mode,
    )


def build_statcast_history_fallback_query(query: SeasonMetricQuery) -> SeasonMetricQuery | None:
    fallback_metric = build_statcast_history_fallback_metric(query.metric, role=query.role)
    if fallback_metric is None:
        return None
    return SeasonMetricQuery(
        metric=fallback_metric,
        descriptor=query.descriptor,
        sort_desc=query.sort_desc,
        entity_scope=query.entity_scope,
        role=query.role,
        start_season=query.start_season,
        end_season=query.end_season,
        scope_label=query.scope_label,
        team_filter_code=query.team_filter_code,
        team_filter_name=query.team_filter_name,
        provider_group_preference=query.provider_group_preference,
        minimum_starts=query.minimum_starts,
        aggregate_range=query.aggregate_range,
        team_record_filter=query.team_record_filter,
        exclude_pitcher_only_hitters=query.exclude_pitcher_only_hitters,
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
        team_record_filter=query.team_record_filter,
        exclude_pitcher_only_hitters=query.exclude_pitcher_only_hitters,
    )


def find_statcast_history_metric(connection, lowered_question: str) -> SeasonMetricSpec | None:
    best_match: tuple[int, SeasonMetricSpec] | None = None
    for config in STATCAST_HISTORY_TABLE_CONFIG:
        table_name = str(config["table_name"])
        if not table_exists(connection, table_name):
            continue
        role = str(config["role"])
        for column in list_table_columns(connection, table_name):
            spec = build_statcast_history_metric_spec(column=column, table_name=table_name, role=role)
            if spec is None:
                continue
            for alias in spec.aliases:
                alias_lower = alias.lower().strip()
                if not alias_lower:
                    continue
                pattern = rf"(?<![a-z0-9]){re.escape(alias_lower)}(?![a-z0-9])"
                score = metric_alias_match_score(alias_lower, pattern, lowered_question)
                if score is None:
                    continue
                score += metric_match_bonus(spec, lowered_question)
                if best_match is None or score > best_match[0]:
                    best_match = (score, spec)
    return best_match[1] if best_match else None


def build_statcast_history_metric_spec(*, column: str, table_name: str, role: str) -> SeasonMetricSpec | None:
    normalized = column.lower().strip()
    if normalized in STATCAST_HISTORY_NON_METRIC_COLUMNS:
        return None
    if normalized == "n":
        return None
    aliases = tuple(sorted(build_statcast_history_aliases(normalized, role)))
    if not aliases:
        return None
    aggregate_mode = infer_statcast_history_aggregate_mode(normalized, role)
    sample_basis = infer_statcast_history_sample_basis(normalized, role)
    min_sample_size = infer_statcast_history_min_sample_size(sample_basis, aggregate_mode)
    return SeasonMetricSpec(
        key=normalize_metric_key(normalized),
        label=format_statcast_history_label(normalized),
        aliases=aliases,
        source_family="statcast_history",
        role=role,
        entity_scope="player",
        higher_is_better=infer_statcast_history_higher_is_better(normalized, role),
        formatter=infer_statcast_history_formatter(normalized, aggregate_mode),
        sample_basis=sample_basis,
        min_sample_size=min_sample_size,
        dynamic_table_name=table_name,
        dynamic_value_column=normalized,
        dynamic_aggregate_mode=aggregate_mode,
    )


def build_statcast_history_aliases(column: str, role: str) -> set[str]:
    aliases = {
        column,
        column.replace("_", " "),
    }
    stripped = column
    for prefix in ("b_", "p_", "r_"):
        if stripped.startswith(prefix):
            stripped = stripped[len(prefix) :]
            aliases.add(stripped)
            aliases.add(stripped.replace("_", " "))
            break
    if stripped in STATCAST_HISTORY_RATE_ALIASES:
        aliases.update(STATCAST_HISTORY_RATE_ALIASES[stripped])
    if column in STATCAST_HISTORY_RATE_ALIASES:
        aliases.update(STATCAST_HISTORY_RATE_ALIASES[column])
    pitch_metric = parse_statcast_history_pitch_metric(stripped)
    if pitch_metric is not None:
        aliases.update(pitch_metric)
    if stripped.endswith("_percent"):
        base = stripped[: -len("_percent")].replace("_", " ")
        aliases.update({f"{base} percent", f"{base} percentage", f"{base} rate", f"{base}%"})
        if base == "whiff":
            aliases.update({"whiff", "whiffs"})
    if stripped.endswith("_avg_speed"):
        base = stripped[: -len("_avg_speed")].replace("_", " ")
        aliases.update({f"{base} average speed", f"{base} avg speed", f"{base} velocity", f"{base} average velocity"})
    if stripped.endswith("_avg_spin"):
        base = stripped[: -len("_avg_spin")].replace("_", " ")
        aliases.update({f"{base} average spin", f"{base} avg spin", f"{base} spin", f"{base} spin rate"})
    if stripped.endswith("_range_speed"):
        base = stripped[: -len("_range_speed")].replace("_", " ")
        aliases.update({f"{base} speed range", f"{base} velocity range"})
    if role == "pitcher" and "walk" in stripped and "intent" not in stripped:
        aliases.add(stripped.replace("walk", "walks allowed").replace("_", " "))
    return {alias.strip().lower() for alias in aliases if alias.strip()}


def parse_statcast_history_pitch_metric(column: str) -> set[str] | None:
    for prefix, label in STATCAST_HISTORY_PITCH_LABELS.items():
        if column == f"n_{prefix}_formatted":
            return {f"{label} count", f"{label} pitches", f"number of {label}s"}
        if not column.startswith(f"{prefix}_"):
            continue
        suffix = column[len(prefix) + 1 :]
        if suffix == "avg_speed":
            return {f"{label} velocity", f"{label} avg velocity", f"{label} average speed"}
        if suffix == "avg_spin":
            return {f"{label} spin", f"{label} spin rate", f"{label} avg spin"}
        if suffix == "avg_break_x":
            return {f"{label} horizontal break", f"{label} avg break x"}
        if suffix == "avg_break_z":
            return {f"{label} vertical break", f"{label} avg break z"}
        if suffix == "avg_break_z_induced":
            return {f"{label} induced vertical break", f"{label} ivb"}
        if suffix == "avg_break":
            return {f"{label} break", f"{label} average break"}
        if suffix == "range_speed":
            return {f"{label} velocity range", f"{label} speed range"}
    return None


def format_statcast_history_label(column: str) -> str:
    pitch_metric = parse_statcast_history_pitch_metric(column)
    if pitch_metric:
        alias = sorted(pitch_metric, key=len)[0]
        return alias.replace("avg", "Avg").replace("ivb", "IVB").title()
    if column in STATCAST_HISTORY_RATE_ALIASES:
        return STATCAST_HISTORY_RATE_ALIASES[column][0].replace("avg", "Avg").replace("xba", "xBA").replace("xslg", "xSLG").replace("xwoba", "xwOBA").replace("xobp", "xOBP").replace("xiso", "xISO").replace("woba", "wOBA")
    base = column
    for prefix in ("b_", "p_", "r_"):
        if base.startswith(prefix):
            base = base[len(prefix) :]
            break
    return base.replace("_", " ").title()


def infer_statcast_history_aggregate_mode(column: str, role: str) -> str:
    normalized = column.lower()
    if normalized == "player_age":
        return "max"
    if normalized.startswith("n_") and normalized.endswith("_formatted"):
        return "sum"
    if normalized.endswith("_avg"):
        return "weighted_avg"
    if any(
        token in normalized
        for token in (
            "_percent",
            "_avg_",
            "avg_",
            "attack_",
            "woba",
            "xba",
            "xslg",
            "xobp",
            "xiso",
            "bacon",
            "xbacon",
            "wobacon",
            "xwobacon",
            "batting_avg",
            "slg_percent",
            "on_base_percent",
            "on_base_plus_slg",
            "isolated_power",
            "babip",
            "arm_angle",
            "p_era",
            "p_opp_",
        )
    ):
        return "weighted_avg"
    return "sum"


def infer_statcast_history_sample_basis(column: str, role: str) -> str | None:
    normalized = column.lower()
    if normalized == "player_age":
        return None
    if normalized.startswith("n_") and normalized.endswith("_formatted"):
        return normalized
    for prefix in tuple(STATCAST_HISTORY_PITCH_LABELS):
        if normalized.startswith(f"{prefix}_"):
            return f"n_{prefix}_formatted"
    if any(token in normalized for token in ("pitch_count", "swing", "contact_percent", "whiff", "in_zone", "out_zone", "edge", "meatball", "f_strike")):
        return "pitch_count"
    if any(token in normalized for token in ("barrel", "hard_hit", "sweet_spot", "solidcontact", "flareburner", "poorly", "groundballs", "flyballs", "linedrives", "popups", "pull_percent", "straightaway_percent", "opposite_percent", "exit_velocity_avg", "launch_angle_avg", "bacon", "xbacon", "wobacon", "xwobacon")):
        return "batted_ball"
    if any(token in normalized for token in ("batting_avg", "slg_percent", "isolated_power", "xba", "xslg", "xiso")):
        return "ab"
    if any(token in normalized for token in ("on_base_percent", "on_base_plus_slg", "woba", "xwoba", "xobp", "k_percent", "bb_percent", "avg_swing_speed", "avg_swing_length", "fast_swing_rate", "blasts_", "squared_up_", "swords", "attack_", "ideal_angle_rate", "vertical_swing_path")):
        return "pa"
    if role == "pitcher" and any(token in normalized for token in ("p_era", "p_opp_", "walk", "strikeout", "hit", "home_run", "xba", "xslg", "woba", "xwoba", "avg_swing_speed")):
        return "pa"
    return None


def infer_statcast_history_min_sample_size(sample_basis: str | None, aggregate_mode: str) -> int:
    if aggregate_mode == "sum" or sample_basis is None:
        return 1
    if sample_basis in {"pa", "ab"}:
        return 20
    if sample_basis == "pitch_count":
        return 50
    if sample_basis == "batted_ball":
        return 10
    if sample_basis.startswith("n_"):
        return 10
    return 1


def infer_statcast_history_higher_is_better(column: str, role: str) -> bool:
    normalized = column.lower()
    negative_tokens = {"loss", "blown_save", "caught_stealing", "gnd_into_dp", "gnd_into_tp", "missed_bunt"}
    if any(token in normalized for token in negative_tokens):
        return False
    if role == "hitter":
        return not any(token in normalized for token in ("strikeout", "k_percent", "swing_miss", "out_"))
    return not any(
        token in normalized
        for token in (
            "p_era",
            "opp_",
            "earned_run",
            "unearned_run",
            "walk",
            "bb_percent",
            "batting_avg",
            "on_base_",
            "slg_percent",
            "isolated_power",
            "babip",
            "home_run",
            "hit",
            "woba",
            "xba",
            "xslg",
            "xobp",
            "xiso",
            "bacon",
            "xwoba",
            "exit_velocity",
            "launch_angle",
            "barrel",
            "hard_hit",
            "sweet_spot",
        )
    )


def infer_statcast_history_formatter(column: str, aggregate_mode: str) -> str:
    normalized = column.lower()
    if aggregate_mode == "sum":
        return ".0f"
    if "spin" in normalized:
        return ".0f"
    if "percent" in normalized or normalized.endswith("_rate"):
        return ".1f"
    if any(token in normalized for token in ("speed", "angle", "break", "era")):
        return ".1f" if normalized != "p_era" else ".2f"
    if any(token in normalized for token in ("avg", "woba", "xba", "xslg", "xobp", "xiso", "babip", "on_base", "slg", "ops", "iso")):
        return ".3f"
    return ".2f"


def parse_team_record_filter(lowered_question: str) -> str | None:
    if any(hint in lowered_question for hint in WINNING_RECORD_HINTS):
        return "winning"
    if any(hint in lowered_question for hint in LOSING_RECORD_HINTS):
        return "losing"
    return None


def team_record_filter_matches(filter_key: str | None, wins: int, losses: int) -> bool:
    if filter_key == "winning":
        return wins > losses
    if filter_key == "losing":
        return losses > wins
    return True


def describe_team_record_filter(filter_key: str | None) -> str:
    if filter_key == "winning":
        return " with a winning record"
    if filter_key == "losing":
        return " with a losing record"
    return ""


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
    role_join_sql = ""
    role_select_sql = "0 AS pitching_ipouts, 0 AS pitching_games_started, 0 AS has_non_pitch_fielding,"
    role_parameters: list[Any] = []
    if query.exclude_pitcher_only_hitters:
        role_select_sql = (
            "COALESCE(pt.pitching_ipouts, 0) AS pitching_ipouts, "
            "COALESCE(pt.pitching_games_started, 0) AS pitching_games_started, "
            "CASE WHEN npf.playerid IS NULL THEN 0 ELSE 1 END AS has_non_pitch_fielding,"
        )
        role_join_sql = """
        LEFT JOIN (
            SELECT
                playerid,
                SUM(CAST(COALESCE(ipouts, '0') AS INTEGER)) AS pitching_ipouts,
                SUM(CAST(COALESCE(gs, '0') AS INTEGER)) AS pitching_games_started
            FROM lahman_pitching
            WHERE CAST(yearid AS INTEGER) BETWEEN ? AND ?
            GROUP BY playerid
        ) AS pt
          ON pt.playerid = b.playerid
        LEFT JOIN (
            SELECT DISTINCT playerid
            FROM lahman_fielding
            WHERE CAST(yearid AS INTEGER) BETWEEN ? AND ?
              AND upper(COALESCE(pos, '')) <> 'P'
        ) AS npf
          ON npf.playerid = b.playerid
        """
        role_parameters.extend([query.start_season, query.end_season, query.start_season, query.end_season])
    rows = connection.execute(
        f"""
        SELECT
            {season_select}
            b.playerid,
            p.namefirst,
            p.namelast,
            CASE WHEN COUNT(DISTINCT upper(b.teamid)) = 1 THEN MIN(upper(b.teamid)) ELSE 'MULTI' END AS team,
            {role_select_sql}
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
        {role_join_sql}
        WHERE CAST(b.yearid AS INTEGER) BETWEEN ? AND ?
          {team_filter_sql}
        GROUP BY {season_group} b.playerid, p.namefirst, p.namelast, pitching_ipouts, pitching_games_started, has_non_pitch_fielding
        """,
        tuple(role_parameters + parameters),
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
        if query.exclude_pitcher_only_hitters and should_exclude_pitcher_only_batter(
            plate_appearances=plate_appearances,
            pitching_ipouts=safe_int(row["pitching_ipouts"]) or 0,
            pitching_games_started=safe_int(row["pitching_games_started"]) or 0,
            has_non_pitch_fielding=bool(safe_int(row["has_non_pitch_fielding"]) or 0),
        ):
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


def should_exclude_pitcher_only_batter(
    *,
    plate_appearances: int,
    pitching_ipouts: int,
    pitching_games_started: int,
    has_non_pitch_fielding: bool,
) -> bool:
    if has_non_pitch_fielding:
        return False
    if plate_appearances >= 200:
        return False
    if pitching_games_started >= 3:
        return True
    if pitching_ipouts >= 27:
        return True
    return False


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
    team_columns = {column.lower() for column in list_table_columns(connection, "lahman_teams")}
    if query.aggregate_range and "franchid" in team_columns:
        rows = connection.execute(
            """
            WITH team_rows AS (
                SELECT
                    CAST(yearid AS INTEGER) AS yearid,
                    upper(COALESCE(NULLIF(franchid, ''), teamid)) AS franchise_id,
                    upper(teamid) AS teamid,
                    name,
                    CAST(COALESCE(g, '0') AS INTEGER) AS g,
                    CAST(COALESCE(w, '0') AS INTEGER) AS w,
                    CAST(COALESCE(l, '0') AS INTEGER) AS l,
                    CAST(COALESCE(r, '0') AS INTEGER) AS r,
                    CAST(COALESCE(ab, '0') AS INTEGER) AS ab,
                    CAST(COALESCE(h, '0') AS INTEGER) AS h,
                    CAST(COALESCE(c_2b, '0') AS INTEGER) AS c_2b,
                    CAST(COALESCE(c_3b, '0') AS INTEGER) AS c_3b,
                    CAST(COALESCE(hr, '0') AS INTEGER) AS hr,
                    CAST(COALESCE(bb, '0') AS INTEGER) AS bb,
                    CAST(COALESCE(hbp, '0') AS INTEGER) AS hbp,
                    CAST(COALESCE(sf, '0') AS INTEGER) AS sf,
                    CAST(COALESCE(ra, '0') AS INTEGER) AS ra,
                    CAST(NULLIF(era, '') AS REAL) AS era,
                    CAST(NULLIF(fp, '') AS REAL) AS fp
                FROM lahman_teams
                WHERE CAST(yearid AS INTEGER) BETWEEN ? AND ?
            ),
            latest_names AS (
                SELECT
                    tr.franchise_id,
                    MIN(tr.name) AS name
                FROM team_rows AS tr
                JOIN (
                    SELECT franchise_id, MAX(yearid) AS latest_year
                    FROM team_rows
                    GROUP BY franchise_id
                ) AS latest
                  ON latest.franchise_id = tr.franchise_id
                 AND latest.latest_year = tr.yearid
                GROUP BY tr.franchise_id
            )
            SELECT
                MIN(tr.yearid) AS start_season,
                MAX(tr.yearid) AS end_season,
                tr.franchise_id AS teamid,
                SUM(tr.g) AS g,
                SUM(tr.w) AS w,
                SUM(tr.l) AS l,
                SUM(tr.r) AS r,
                SUM(tr.ab) AS ab,
                SUM(tr.h) AS h,
                SUM(tr.c_2b) AS c_2b,
                SUM(tr.c_3b) AS c_3b,
                SUM(tr.hr) AS hr,
                SUM(tr.bb) AS bb,
                SUM(tr.hbp) AS hbp,
                SUM(tr.sf) AS sf,
                SUM(tr.ra) AS ra,
                AVG(tr.era) AS era,
                AVG(tr.fp) AS fp,
                COALESCE(ln.name, tr.franchise_id) AS name
            FROM team_rows AS tr
            LEFT JOIN latest_names AS ln
              ON ln.franchise_id = tr.franchise_id
            GROUP BY tr.franchise_id, ln.name
            """,
            (query.start_season, query.end_season),
        ).fetchall()
    else:
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
        if not team_record_filter_matches(query.team_record_filter, wins, losses):
            continue
        runs = safe_int(row["r"]) or 0
        runs_allowed = safe_int(row["ra"]) or 0
        run_differential = runs - runs_allowed
        avg = (hits / at_bats) if at_bats else None
        obp = ((hits + walks + hbp) / obp_denom) if obp_denom else None
        slg = ((singles + (2 * doubles) + (3 * triples) + (4 * home_runs)) / at_bats) if at_bats else None
        ops = (obp + slg) if obp is not None and slg is not None else None
        metric_value = {
            "wins": float(wins),
            "losses": float(losses),
            "win_pct": (wins / (wins + losses)) if (wins + losses) else None,
            "runs": float(runs),
            "runs_per_game": (runs / games) if games else None,
            "runs_allowed": float(runs_allowed),
            "runs_allowed_per_game": (runs_allowed / games) if games else None,
            "run_differential": float(run_differential),
            "run_differential_per_game": (run_differential / games) if games else None,
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
                "runs": runs,
                "runs_allowed": runs_allowed,
                "run_differential": run_differential,
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


def fetch_statcast_history_rows(connection, query: SeasonMetricQuery) -> list[dict[str, Any]]:
    table_name = query.metric.dynamic_table_name or ""
    value_column = query.metric.dynamic_value_column or ""
    if (
        not table_name
        or not value_column
        or not table_exists(connection, table_name)
        or query.entity_scope != "player"
        or query.team_filter_code
    ):
        return []
    rows = connection.execute(
        f"""
        SELECT *
        FROM "{table_name}"
        WHERE CAST(NULLIF(year, '') AS INTEGER) BETWEEN ? AND ?
        """,
        (query.start_season, query.end_season),
    ).fetchall()
    if not rows:
        return []
    if query.aggregate_range:
        return rank_rows(
            build_aggregated_statcast_history_rows(rows, query),
            query,
        )
    candidates: list[dict[str, Any]] = []
    for row in rows:
        metric_value = safe_float(row[value_column])
        season = safe_int(row["year"])
        if metric_value is None or season is None:
            continue
        values = statcast_history_sample_values(row)
        if not passes_statcast_history_qualifiers(values, query):
            continue
        if not passes_sample_threshold(query.metric, values):
            continue
        candidate = build_statcast_history_row_payload(row, query, metric_value=float(metric_value))
        candidates.append(candidate)
    return rank_rows(candidates, query)


def build_aggregated_statcast_history_rows(rows: list[Any], query: SeasonMetricQuery) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    value_column = query.metric.dynamic_value_column or ""
    aggregate_mode = query.metric.dynamic_aggregate_mode or "sum"
    weight_column = query.metric.sample_basis
    for row in rows:
        metric_value = safe_float(row[value_column])
        if metric_value is None:
            continue
        player_id = str(row["player_id"] or "").strip()
        if not player_id:
            continue
        name = format_statcast_history_player_name(row["last_name_first_name"])
        group = groups.setdefault(
            player_id,
            {
                "player_id": player_id,
                "player_name": name,
                "scope_start_season": query.start_season,
                "scope_end_season": query.end_season,
                "scope_label": query.scope_label,
                "metric_value_sum": 0.0,
                "metric_weight_sum": 0.0,
                "metric_observation_count": 0,
                "sample_values": {},
                "display_values": {},
            },
        )
        group["metric_observation_count"] += 1
        if aggregate_mode == "weighted_avg":
            weight = safe_float(row[weight_column]) if weight_column and weight_column in row.keys() else None
            if weight is None or weight <= 0:
                weight = 1.0
            group["metric_value_sum"] += float(metric_value) * float(weight)
            group["metric_weight_sum"] += float(weight)
        elif aggregate_mode == "max":
            if group["metric_observation_count"] == 1 or float(metric_value) > group["metric_value_sum"]:
                group["metric_value_sum"] = float(metric_value)
            group["metric_weight_sum"] = 1.0
        else:
            group["metric_value_sum"] += float(metric_value)
            group["metric_weight_sum"] += 1.0
        merge_statcast_history_samples(group["sample_values"], row)
        merge_statcast_history_display_values(group["display_values"], row, query.role)
    candidates: list[dict[str, Any]] = []
    for group in groups.values():
        values = dict(group["sample_values"])
        if not passes_statcast_history_qualifiers(values, query):
            continue
        if not passes_sample_threshold(query.metric, values):
            continue
        metric_weight_sum = float(group["metric_weight_sum"] or 0.0)
        metric_value = (
            (float(group["metric_value_sum"]) / metric_weight_sum)
            if aggregate_mode == "weighted_avg"
            else float(group["metric_value_sum"])
        )
        if aggregate_mode == "weighted_avg" and metric_weight_sum <= 0:
            continue
        row_payload = {
            "season": query.end_season,
            "scope_label": query.scope_label,
            "scope_start_season": query.start_season,
            "scope_end_season": query.end_season,
            "player_name": group["player_name"],
            "player_id": group["player_id"],
            "metric_value": metric_value,
            "sample_size": infer_statcast_history_sample_size(values, query.metric.sample_basis, metric_value),
            **group["display_values"],
        }
        if weight_column:
            row_payload[weight_column] = values.get(weight_column)
        candidates.append(row_payload)
    return candidates


def statcast_history_sample_values(row: Any) -> dict[str, float | int | None]:
    values: dict[str, float | int | None] = {}
    for key in (
        "pa",
        "ab",
        "batted_ball",
        "pitch_count",
        "p_game",
        "p_starting_p",
        "hit",
        "home_run",
        "walk",
        "strikeout",
        "player_age",
    ):
        if key in row.keys():
            values[key] = safe_float(row[key])
    for key in row.keys():
        key_text = str(key)
        if key_text.startswith("n_") and key_text.endswith("_formatted"):
            values[key_text] = safe_float(row[key_text])
    return values


def merge_statcast_history_samples(target: dict[str, float | int | None], row: Any) -> None:
    for key, value in statcast_history_sample_values(row).items():
        numeric = safe_float(value)
        if numeric is None:
            continue
        target[key] = float(target.get(key) or 0.0) + float(numeric)


def merge_statcast_history_display_values(target: dict[str, Any], row: Any, role: str) -> None:
    display_columns = (
        (
            "player_age",
            "pa",
            "ab",
            "hit",
            "home_run",
            "walk",
            "strikeout",
            "batting_avg",
            "on_base_plus_slg",
            "exit_velocity_avg",
            "barrel",
            "pitch_count",
            "batted_ball",
        )
        if role == "hitter"
        else (
            "player_age",
            "p_game",
            "p_starting_p",
            "p_win",
            "p_loss",
            "p_save",
            "p_era",
            "strikeout",
            "walk",
            "pitch_count",
            "home_run",
            "hit",
        )
    )
    for column in display_columns:
        if column not in row.keys():
            continue
        value = safe_float(row[column])
        if value is None:
            continue
        aggregate_mode = infer_statcast_history_aggregate_mode(column, role)
        if aggregate_mode == "sum":
            target[column] = float(target.get(column) or 0.0) + float(value)
        else:
            target[column] = float(value)


def build_statcast_history_row_payload(row: Any, query: SeasonMetricQuery, *, metric_value: float) -> dict[str, Any]:
    values = statcast_history_sample_values(row)
    payload: dict[str, Any] = {
        "season": safe_int(row["year"]) or query.end_season,
        "scope_label": str(safe_int(row["year"]) or query.end_season),
        "scope_start_season": safe_int(row["year"]) or query.end_season,
        "scope_end_season": safe_int(row["year"]) or query.end_season,
        "player_name": format_statcast_history_player_name(row["last_name_first_name"]),
        "player_id": str(row["player_id"] or ""),
        "metric_value": metric_value,
        "sample_size": infer_statcast_history_sample_size(values, query.metric.sample_basis, metric_value),
    }
    merge_statcast_history_display_values(payload, row, query.role)
    return payload


def passes_statcast_history_qualifiers(values: dict[str, float | int | None], query: SeasonMetricQuery) -> bool:
    if query.role == "pitcher" and query.minimum_starts is not None:
        starts = values.get("p_starting_p")
        try:
            if float(starts or 0.0) < float(query.minimum_starts):
                return False
        except (TypeError, ValueError):
            return False
    return True


def infer_statcast_history_sample_size(
    values: dict[str, float | int | None],
    sample_basis: str | None,
    metric_value: float,
) -> float:
    if sample_basis and values.get(sample_basis) is not None:
        return float(values[sample_basis] or 0.0)
    return float(metric_value)


def format_statcast_history_player_name(value: Any) -> str:
    text = str(value or "").strip()
    if "," not in text:
        return text
    last_name, first_name = [part.strip() for part in text.split(",", 1)]
    return f"{first_name} {last_name}".strip()


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
    record_filter_text = describe_team_record_filter(query.team_record_filter) if query.entity_scope == "team" else ""
    subject_phrase = "team" if query.entity_scope == "team" else query.role
    leader_scope = str(leader.get("scope_label") or leader.get("season") or "")
    scope_parenthetical = f" ({leader_scope})" if leader_scope and not query.aggregate_range else ""
    summary = (
        f"For {query.scope_label}{filter_text}{record_filter_text}, the {query.descriptor} {subject_phrase} by {query.metric.label} "
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
    if query.metric.source_family == "statcast_history":
        return "Imported Statcast custom leaderboard history CSV exports"
    if query.metric.source_family == "statcast":
        return "Local Statcast season summaries aggregated from synced public Statcast data"
    if query.entity_scope == "team":
        return "Lahman Teams table"
    if query.role == "pitcher":
        return "Lahman Pitching and People tables"
    if query.role == "fielder":
        return "Lahman Fielding and People tables"
    return "Lahman Batting and People tables"
