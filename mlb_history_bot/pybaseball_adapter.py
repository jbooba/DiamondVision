from __future__ import annotations

import contextlib
import io
import warnings
from dataclasses import dataclass
from functools import lru_cache
from typing import Any


@dataclass(frozen=True, slots=True)
class PybaseballCapability:
    name: str
    category: str
    description: str


PYBASEBALL_CAPABILITIES: tuple[PybaseballCapability, ...] = (
    PybaseballCapability("playerid_lookup", "ids", "Resolve player names to Baseball Reference, FanGraphs, and MLBAM ids."),
    PybaseballCapability("playerid_reverse_lookup", "ids", "Resolve ids back to player records."),
    PybaseballCapability("player_search_list", "ids", "Look up the cached pybaseball player register search list."),
    PybaseballCapability("team_ids", "ids", "Look up team ids and abbreviations by season."),
    PybaseballCapability("fangraphs_teams", "ids", "Look up FanGraphs team metadata and abbreviations."),
    PybaseballCapability("batting_stats", "season", "FanGraphs batting leaderboards for full seasons."),
    PybaseballCapability("pitching_stats", "season", "FanGraphs pitching leaderboards for full seasons."),
    PybaseballCapability("fielding_stats", "season", "FanGraphs fielding leaderboards for full seasons."),
    PybaseballCapability("batting_stats_range", "range", "FanGraphs batting leaderboards over arbitrary date ranges."),
    PybaseballCapability("pitching_stats_range", "range", "FanGraphs pitching leaderboards over arbitrary date ranges."),
    PybaseballCapability("batting_stats_bref", "season", "Baseball Reference season batting table."),
    PybaseballCapability("pitching_stats_bref", "season", "Baseball Reference season pitching table."),
    PybaseballCapability("bwar_bat", "season", "Baseball Reference batting WAR table."),
    PybaseballCapability("bwar_pitch", "season", "Baseball Reference pitching WAR table."),
    PybaseballCapability("team_batting", "team", "FanGraphs team batting leaderboards."),
    PybaseballCapability("team_pitching", "team", "FanGraphs team pitching leaderboards."),
    PybaseballCapability("team_fielding", "team", "FanGraphs team fielding leaderboards."),
    PybaseballCapability("team_batting_bref", "team", "Baseball Reference team batting table."),
    PybaseballCapability("team_pitching_bref", "team", "Baseball Reference team pitching table."),
    PybaseballCapability("team_fielding_bref", "team", "Baseball Reference team fielding table."),
    PybaseballCapability("team_game_logs", "team", "Per-team batting or pitching game logs."),
    PybaseballCapability("schedule_and_record", "team", "Team schedule and results by season."),
    PybaseballCapability("standings", "team", "League standings by season."),
    PybaseballCapability("get_splits", "splits", "Player batting or pitching splits."),
    PybaseballCapability("statcast", "statcast", "Raw Statcast event feed for date ranges."),
    PybaseballCapability("statcast_single_game", "statcast", "Raw Statcast event feed for a single game."),
    PybaseballCapability("statcast_batter", "statcast", "Statcast events for a batter over a date range."),
    PybaseballCapability("statcast_pitcher", "statcast", "Statcast events for a pitcher over a date range."),
    PybaseballCapability("statcast_batter_expected_stats", "statcast", "Season leaderboard of batter xBA, xSLG, and xwOBA."),
    PybaseballCapability("statcast_batter_percentile_ranks", "statcast", "Season leaderboard of batter percentile ranks for key Statcast traits."),
    PybaseballCapability("statcast_batter_exitvelo_barrels", "statcast", "Season leaderboard of batter EV, barrel, and hard-hit outputs."),
    PybaseballCapability("statcast_batter_pitch_arsenal", "statcast", "Batter results against each pitch type."),
    PybaseballCapability("statcast_pitcher_expected_stats", "statcast", "Season leaderboard of pitcher xBA, xSLG, xwOBA, and xERA."),
    PybaseballCapability("statcast_pitcher_percentile_ranks", "statcast", "Season leaderboard of pitcher percentile ranks for key Statcast traits."),
    PybaseballCapability("statcast_pitcher_exitvelo_barrels", "statcast", "Season leaderboard of contact quality allowed by pitchers."),
    PybaseballCapability("statcast_pitcher_pitch_arsenal", "statcast", "Pitcher average velocity by pitch type."),
    PybaseballCapability("statcast_pitcher_arsenal_stats", "statcast", "Pitch-level run value and contact outcomes allowed by pitchers."),
    PybaseballCapability("statcast_pitcher_spin_dir_comp", "statcast", "Pitch movement/spin-direction comparison between pitch types."),
    PybaseballCapability("statcast_outs_above_average", "fielding", "Statcast OAA leaderboard by season and position."),
    PybaseballCapability("statcast_outfield_catch_prob", "fielding", "Outfield catch probability leaderboard."),
    PybaseballCapability("statcast_outfield_directional_oaa", "fielding", "Outfield directional OAA leaderboard."),
    PybaseballCapability("statcast_outfielder_jump", "fielding", "Outfielder jump leaderboard."),
    PybaseballCapability("statcast_running_splits", "running", "Statcast baserunning splits."),
    PybaseballCapability("statcast_sprint_speed", "running", "Sprint speed leaderboard."),
    PybaseballCapability("statcast_catcher_poptime", "catching", "Catcher pop time leaderboard."),
    PybaseballCapability("statcast_catcher_framing", "catching", "Catcher framing leaderboard."),
    PybaseballCapability("top_prospects", "prospects", "MLB top prospects leaderboard."),
    PybaseballCapability("amateur_draft", "prospects", "Amateur draft results by year and round."),
    PybaseballCapability("amateur_draft_by_team", "prospects", "Amateur draft results by team and year."),
    PybaseballCapability("season_game_logs", "historical", "Retrosheet regular-season game logs."),
    PybaseballCapability("world_series_logs", "historical", "Retrosheet World Series logs."),
    PybaseballCapability("all_star_game_logs", "historical", "Retrosheet All-Star Game logs."),
    PybaseballCapability("wild_card_logs", "historical", "Retrosheet Wild Card logs."),
    PybaseballCapability("division_series_logs", "historical", "Retrosheet Division Series logs."),
    PybaseballCapability("lcs_logs", "historical", "Retrosheet League Championship Series logs."),
    PybaseballCapability("schedules", "historical", "Retrosheet historical schedules."),
    PybaseballCapability("park_codes", "historical", "Retrosheet park-code lookup."),
    PybaseballCapability("rosters", "historical", "Retrosheet roster snapshots."),
    PybaseballCapability("lahman_managers", "historical", "Lahman manager table via pybaseball."),
    PybaseballCapability("lahman_managers_half", "historical", "Lahman partial-season manager table via pybaseball."),
    PybaseballCapability("lahman_people", "historical", "Lahman people table via pybaseball."),
    PybaseballCapability("lahman_teams_core", "historical", "Lahman core team-season table via pybaseball."),
    PybaseballCapability("lahman_teams_franchises", "historical", "Lahman franchises table via pybaseball."),
    PybaseballCapability("lahman_teams_half", "historical", "Lahman partial-season team standings via pybaseball."),
    PybaseballCapability("lahman", "historical", "Lahman database tables via pybaseball download helpers."),
    PybaseballCapability("retrosheet", "historical", "Retrosheet event and season-table access helpers."),
)


def list_pybaseball_capabilities() -> list[dict[str, str]]:
    return [
        {"name": capability.name, "category": capability.category, "description": capability.description}
        for capability in PYBASEBALL_CAPABILITIES
    ]


def _frame_to_records(frame: Any) -> list[dict[str, Any]]:
    return frame.to_dict(orient="records") if hasattr(frame, "empty") and not frame.empty else []


@lru_cache(maxsize=1)
def _enable_pybaseball_cache() -> bool:
    try:
        from pybaseball import cache

        cache.enable()
        return True
    except Exception:
        return False


def _run_quiet_pybaseball(callable_obj, /, *args, **kwargs):
    _enable_pybaseball_cache()
    sink = io.StringIO()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            return callable_obj(*args, **kwargs)


@lru_cache(maxsize=8)
def load_player_search_list() -> list[dict[str, Any]]:
    try:
        from pybaseball import player_search_list

        frame = player_search_list()
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=8)
def load_fangraphs_teams() -> list[dict[str, Any]]:
    try:
        from pybaseball import fangraphs_teams

        frame = fangraphs_teams()
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_batting_stats(start_season: int, end_season: int) -> list[dict[str, Any]]:
    try:
        from pybaseball import batting_stats

        frame = batting_stats(start_season, end_season, qual=0)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_pitching_stats(start_season: int, end_season: int) -> list[dict[str, Any]]:
    try:
        from pybaseball import pitching_stats

        frame = pitching_stats(start_season, end_season, qual=0)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_fielding_stats(start_season: int, end_season: int) -> list[dict[str, Any]]:
    try:
        from pybaseball import fielding_stats

        frame = fielding_stats(start_season, end_season, qual=0)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=64)
def load_batting_stats_range(start_date: str, end_date: str) -> list[dict[str, Any]]:
    try:
        from pybaseball import batting_stats_range

        frame = batting_stats_range(start_dt=start_date, end_dt=end_date)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=64)
def load_pitching_stats_range(start_date: str, end_date: str) -> list[dict[str, Any]]:
    try:
        from pybaseball import pitching_stats_range

        frame = pitching_stats_range(start_dt=start_date, end_dt=end_date)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=16)
def load_batting_stats_bref(season: int | None = None) -> list[dict[str, Any]]:
    try:
        from pybaseball import batting_stats_bref

        frame = batting_stats_bref(season=season)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=16)
def load_pitching_stats_bref(season: int | None = None) -> list[dict[str, Any]]:
    try:
        from pybaseball import pitching_stats_bref

        frame = pitching_stats_bref(season=season)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=4)
def load_bwar_bat(return_all: bool = False) -> list[dict[str, Any]]:
    try:
        from pybaseball import bwar_bat

        frame = bwar_bat(return_all=return_all)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=4)
def load_bwar_pitch(return_all: bool = False) -> list[dict[str, Any]]:
    try:
        from pybaseball import bwar_pitch

        frame = bwar_pitch(return_all=return_all)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_team_ids(season: int | None = None, league: str = "ALL") -> list[dict[str, Any]]:
    try:
        from pybaseball import team_ids

        frame = team_ids(season=season, league=league)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_standings(season: int | None = None) -> list[dict[str, Any]]:
    try:
        from pybaseball import standings

        frame = standings(season=season)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=64)
def load_team_game_logs(season: int, team: str, log_type: str = "batting") -> list[dict[str, Any]]:
    try:
        from pybaseball import team_game_logs

        frame = team_game_logs(season=season, team=team, log_type=log_type)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=64)
def load_schedule_and_record(season: int, team: str) -> list[dict[str, Any]]:
    try:
        from pybaseball import schedule_and_record

        frame = schedule_and_record(season=season, team=team)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_team_batting(start_season: int, end_season: int | None = None, team: str = "") -> list[dict[str, Any]]:
    try:
        from pybaseball import team_batting

        frame = team_batting(start_season, end_season=end_season, qual=0, team=team)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_team_pitching(start_season: int, end_season: int | None = None, team: str = "") -> list[dict[str, Any]]:
    try:
        from pybaseball import team_pitching

        frame = team_pitching(start_season, end_season=end_season, qual=0, team=team)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_team_fielding(start_season: int, end_season: int | None = None, team: str = "") -> list[dict[str, Any]]:
    try:
        from pybaseball import team_fielding

        frame = team_fielding(start_season, end_season=end_season, qual=0, team=team)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_team_batting_bref(team: str, start_season: int, end_season: int | None = None) -> list[dict[str, Any]]:
    try:
        from pybaseball import team_batting_bref

        frame = team_batting_bref(team, start_season, end_season)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_team_pitching_bref(team: str, start_season: int, end_season: int | None = None) -> list[dict[str, Any]]:
    try:
        from pybaseball import team_pitching_bref

        frame = team_pitching_bref(team, start_season, end_season)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_team_fielding_bref(team: str, start_season: int, end_season: int | None = None) -> list[dict[str, Any]]:
    try:
        from pybaseball import team_fielding_bref

        frame = team_fielding_bref(team, start_season, end_season)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=128)
def load_player_splits(playerid: str, year: int | None = None, pitching_splits: bool = False) -> list[dict[str, Any]]:
    try:
        from pybaseball import get_splits

        result = get_splits(playerid=playerid, year=year, pitching_splits=pitching_splits)
    except Exception:
        return []
    frame = result[0] if isinstance(result, tuple) else result
    return _frame_to_records(frame)


@lru_cache(maxsize=64)
def load_statcast_single_game(game_pk: int | str) -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_single_game

        frame = _run_quiet_pybaseball(statcast_single_game, game_pk=game_pk)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=128)
def load_statcast_range(start_date: str, end_date: str) -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast

        frame = _run_quiet_pybaseball(
            statcast,
            start_dt=start_date,
            end_dt=end_date,
            verbose=False,
            parallel=False,
        )
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=128)
def load_statcast_batter(start_date: str, end_date: str, mlbam_id: int) -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_batter

        frame = _run_quiet_pybaseball(statcast_batter, start_date, end_date, mlbam_id)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=128)
def load_statcast_pitcher(start_date: str, end_date: str, mlbam_id: int) -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_pitcher

        frame = _run_quiet_pybaseball(statcast_pitcher, start_date, end_date, mlbam_id)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_statcast_batter_expected_stats(season: int, min_pa: int | str = "q") -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_batter_expected_stats

        frame = _run_quiet_pybaseball(statcast_batter_expected_stats, season, minPA=min_pa)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_statcast_batter_percentile_ranks(season: int) -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_batter_percentile_ranks

        frame = _run_quiet_pybaseball(statcast_batter_percentile_ranks, season)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_statcast_batter_exitvelo_barrels(season: int, min_bbe: int | str = "q") -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_batter_exitvelo_barrels

        frame = _run_quiet_pybaseball(statcast_batter_exitvelo_barrels, season, minBBE=min_bbe)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_statcast_batter_pitch_arsenal(season: int, min_pa: int = 25) -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_batter_pitch_arsenal

        frame = _run_quiet_pybaseball(statcast_batter_pitch_arsenal, season, minPA=min_pa)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_statcast_pitcher_expected_stats(season: int, min_pa: int | str = "q") -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_pitcher_expected_stats

        frame = _run_quiet_pybaseball(statcast_pitcher_expected_stats, season, minPA=min_pa)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_statcast_pitcher_percentile_ranks(season: int) -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_pitcher_percentile_ranks

        frame = _run_quiet_pybaseball(statcast_pitcher_percentile_ranks, season)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_statcast_pitcher_exitvelo_barrels(season: int, min_bbe: int | str = "q") -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_pitcher_exitvelo_barrels

        frame = _run_quiet_pybaseball(statcast_pitcher_exitvelo_barrels, season, minBBE=min_bbe)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_statcast_pitcher_pitch_arsenal(
    season: int,
    min_p: int = 250,
    arsenal_type: str = "avg_speed",
) -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_pitcher_pitch_arsenal

        frame = _run_quiet_pybaseball(statcast_pitcher_pitch_arsenal, season, minP=min_p, arsenal_type=arsenal_type)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_statcast_pitcher_arsenal_stats(season: int, min_pa: int = 25) -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_pitcher_arsenal_stats

        frame = _run_quiet_pybaseball(statcast_pitcher_arsenal_stats, season, minPA=min_pa)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_statcast_pitcher_spin_dir_comp(
    season: int,
    pitch_a: str = "FF",
    pitch_b: str = "CH",
    min_p: int = 100,
    pitcher_pov: bool = True,
) -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_pitcher_spin_dir_comp

        frame = _run_quiet_pybaseball(
            statcast_pitcher_spin_dir_comp,
            season,
            pitch_a=pitch_a,
            pitch_b=pitch_b,
            minP=min_p,
            pitcher_pov=pitcher_pov,
        )
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=128)
def load_statcast_outs_above_average(season: int, position_code: int, min_att: int | str = 0):
    try:
        from pybaseball.statcast_fielding import statcast_outs_above_average

        return statcast_outs_above_average(season, position_code, min_att=min_att)
    except Exception:
        return None


@lru_cache(maxsize=64)
def load_outfield_catch_probability(season: int, min_opp: int | str = "q") -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_outfield_catch_prob

        frame = statcast_outfield_catch_prob(season, min_opp=min_opp)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=64)
def load_outfield_directional_oaa(season: int, min_opp: int | str = "q") -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_outfield_directional_oaa

        frame = statcast_outfield_directional_oaa(season, min_opp=min_opp)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=64)
def load_outfielder_jump(season: int, min_att: int | str = "q") -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_outfielder_jump

        frame = statcast_outfielder_jump(season, min_att=min_att)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=64)
def load_statcast_running_splits(season: int, min_opp: int = 5, raw_splits: bool = True) -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_running_splits

        frame = _run_quiet_pybaseball(statcast_running_splits, season, min_opp=min_opp, raw_splits=raw_splits)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=64)
def load_sprint_speed(season: int) -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_sprint_speed

        frame = statcast_sprint_speed(season)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=64)
def load_catcher_poptime(season: int, min_2b_att: int | str = "q") -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_catcher_poptime

        frame = statcast_catcher_poptime(season, min_2b_att=min_2b_att)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=64)
def load_catcher_framing(season: int, min_pitches: int | str = "q") -> list[dict[str, Any]]:
    try:
        from pybaseball import statcast_catcher_framing

        frame = statcast_catcher_framing(season, min_pitches=min_pitches)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=16)
def load_top_prospects(team_name: str | None = None, player_type: str | None = None) -> list[dict[str, Any]]:
    try:
        from pybaseball import top_prospects

        frame = top_prospects(teamName=team_name, playerType=player_type)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_amateur_draft(year: int, draft_round: int, keep_stats: bool = True) -> list[dict[str, Any]]:
    try:
        from pybaseball import amateur_draft

        frame = amateur_draft(year, draft_round, keep_stats=keep_stats)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=32)
def load_amateur_draft_by_team(team: str, year: int, keep_stats: bool = True) -> list[dict[str, Any]]:
    try:
        from pybaseball import amateur_draft_by_team

        frame = amateur_draft_by_team(team, year, keep_stats=keep_stats)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=8)
def load_season_game_logs(season: int) -> list[dict[str, Any]]:
    try:
        from pybaseball import season_game_logs

        frame = season_game_logs(season)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=4)
def load_world_series_logs() -> list[dict[str, Any]]:
    try:
        from pybaseball import world_series_logs

        frame = world_series_logs()
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=4)
def load_all_star_game_logs() -> list[dict[str, Any]]:
    try:
        from pybaseball import all_star_game_logs

        frame = all_star_game_logs()
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=4)
def load_wild_card_logs() -> list[dict[str, Any]]:
    try:
        from pybaseball import wild_card_logs

        frame = wild_card_logs()
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=4)
def load_division_series_logs() -> list[dict[str, Any]]:
    try:
        from pybaseball import division_series_logs

        frame = division_series_logs()
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=4)
def load_lcs_logs() -> list[dict[str, Any]]:
    try:
        from pybaseball import lcs_logs

        frame = lcs_logs()
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=8)
def load_schedules(season: int) -> list[dict[str, Any]]:
    try:
        from pybaseball import schedules

        frame = schedules(season)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=4)
def load_park_codes() -> list[dict[str, Any]]:
    try:
        from pybaseball import park_codes

        frame = park_codes()
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=8)
def load_rosters(season: int) -> list[dict[str, Any]]:
    try:
        from pybaseball import rosters

        frame = rosters(season)
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=4)
def load_lahman_managers() -> list[dict[str, Any]]:
    try:
        from pybaseball.lahman import managers

        frame = managers()
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=4)
def load_lahman_managers_half() -> list[dict[str, Any]]:
    try:
        from pybaseball.lahman import managers_half

        frame = managers_half()
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=4)
def load_lahman_people() -> list[dict[str, Any]]:
    try:
        from pybaseball.lahman import people

        frame = people()
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=4)
def load_lahman_teams_core() -> list[dict[str, Any]]:
    try:
        from pybaseball.lahman import teams_core

        frame = teams_core()
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=4)
def load_lahman_teams_franchises() -> list[dict[str, Any]]:
    try:
        from pybaseball.lahman import teams_franchises

        frame = teams_franchises()
    except Exception:
        return []
    return _frame_to_records(frame)


@lru_cache(maxsize=4)
def load_lahman_teams_half() -> list[dict[str, Any]]:
    try:
        from pybaseball.lahman import teams_half

        frame = teams_half()
    except Exception:
        return []
    return _frame_to_records(frame)
