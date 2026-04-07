from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from time import sleep
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .storage import (
    get_connection,
    get_metadata_value,
    replace_statcast_pitcher_games,
    replace_statcast_team_games,
    set_metadata_value,
    table_exists,
)


NO_AB_EVENTS = {
    "walk",
    "intent_walk",
    "hit_by_pitch",
    "sac_bunt",
    "sac_fly",
    "sac_fly_double_play",
    "truncated_pa",
}
STATCAST_METADATA_MAX_SYNCED_PREFIX = "statcast_max_synced_date_"
STATCAST_METADATA_LAST_RUN_KEY = "statcast_last_daily_sync_completed_at"
HIT_EVENTS = {"single", "double", "triple", "home_run"}
STRIKEOUT_EVENTS = {"strikeout", "strikeout_double_play"}
PITCH_FAMILY_BY_CODE = {
    "FA": "fastball",
    "FC": "fastball",
    "FF": "fastball",
    "FO": "changeup",
    "FS": "changeup",
    "FT": "fastball",
    "SI": "fastball",
    "SC": "changeup",
    "CH": "changeup",
    "CU": "curveball",
    "EP": "curveball",
    "KC": "curveball",
    "KN": "curveball",
    "CS": "curveball",
    "SL": "slider",
    "ST": "slider",
    "SV": "slider",
}

TEAM_NAMES = {
    "ARI": "Arizona Diamondbacks",
    "AZ": "Arizona Diamondbacks",
    "ATL": "Atlanta Braves",
    "BAL": "Baltimore Orioles",
    "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs",
    "CIN": "Cincinnati Reds",
    "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies",
    "CWS": "Chicago White Sox",
    "DET": "Detroit Tigers",
    "HOU": "Houston Astros",
    "KC": "Kansas City Royals",
    "KCR": "Kansas City Royals",
    "LAA": "Los Angeles Angels",
    "LAD": "Los Angeles Dodgers",
    "MIA": "Miami Marlins",
    "MIL": "Milwaukee Brewers",
    "MIN": "Minnesota Twins",
    "NYM": "New York Mets",
    "NYY": "New York Yankees",
    "OAK": "Oakland Athletics",
    "ATH": "Athletics",
    "PHI": "Philadelphia Phillies",
    "PIT": "Pittsburgh Pirates",
    "SD": "San Diego Padres",
    "SDP": "San Diego Padres",
    "SEA": "Seattle Mariners",
    "SF": "San Francisco Giants",
    "SFG": "San Francisco Giants",
    "STL": "St. Louis Cardinals",
    "TB": "Tampa Bay Rays",
    "TBR": "Tampa Bay Rays",
    "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays",
    "WSH": "Washington Nationals",
    "WSN": "Washington Nationals",
}


@dataclass(slots=True)
class StatcastSyncWindow:
    start_date: date
    end_date: date


def sync_statcast_data(
    settings: Settings,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    start_season: int | None = None,
    end_season: int | None = None,
    chunk_days: int = 7,
    daily: bool = False,
    backfill_days: int = 3,
) -> list[str]:
    try:
        from pybaseball import statcast
    except ImportError as exc:
        raise RuntimeError("pybaseball is required for Statcast sync") from exc

    connection = get_connection(settings.database_path)
    sync_windows = resolve_statcast_sync_windows(
        settings,
        connection=connection,
        start_date=start_date,
        end_date=end_date,
        start_season=start_season,
        end_season=end_season,
        daily=daily,
        backfill_days=backfill_days,
    )
    if not sync_windows:
        connection.close()
        return ["No Statcast date range resolved for sync."]
    messages: list[str] = []
    total_team_games = 0
    total_pitcher_games = 0
    total_windows = 0
    for window in sync_windows:
        window_rows = 0
        window_pitcher_rows = 0
        for chunk_start, chunk_end in iter_sync_chunks(window.start_date, window.end_date, chunk_days):
            frame = fetch_statcast_frame(statcast, chunk_start, chunk_end)
            rows = aggregate_statcast_team_games(frame)
            pitcher_rows = aggregate_statcast_pitcher_games(frame)
            replace_statcast_team_games(
                connection,
                start_date=chunk_start.isoformat(),
                end_date=chunk_end.isoformat(),
                rows=rows,
            )
            replace_statcast_pitcher_games(
                connection,
                start_date=chunk_start.isoformat(),
                end_date=chunk_end.isoformat(),
                rows=pitcher_rows,
            )
            total_team_games += len(rows)
            total_pitcher_games += len(pitcher_rows)
            window_rows += len(rows)
            window_pitcher_rows += len(pitcher_rows)
            total_windows += 1
        set_metadata_value(
            connection,
            f"{STATCAST_METADATA_MAX_SYNCED_PREFIX}{window.end_date.year}",
            window.end_date.isoformat(),
        )
        messages.append(
            f"Synced Statcast team-game aggregates for {window.start_date.isoformat()} through "
            f"{window.end_date.isoformat()} ({window_rows} team-game row(s), {window_pitcher_rows} pitcher-game row(s))."
        )
    if daily:
        set_metadata_value(connection, STATCAST_METADATA_LAST_RUN_KEY, date.today().isoformat())
    connection.close()
    messages.append(
        f"Statcast sync complete across {total_windows} chunk(s) with {total_team_games} team-game row(s) "
        f"and {total_pitcher_games} pitcher-game row(s) stored."
    )
    return messages


def fetch_statcast_frame(statcast_callable, chunk_start: date, chunk_end: date, *, attempts: int = 3):
    try:
        return statcast_with_retry(statcast_callable, chunk_start, chunk_end, attempts=attempts)
    except Exception:
        if chunk_start >= chunk_end:
            raise
        midpoint = chunk_start + timedelta(days=(chunk_end - chunk_start).days // 2)
        left = fetch_statcast_frame(statcast_callable, chunk_start, midpoint, attempts=attempts)
        right = fetch_statcast_frame(statcast_callable, midpoint + timedelta(days=1), chunk_end, attempts=attempts)
        try:
            import pandas as pd
        except ImportError as exc:
            raise RuntimeError("pandas is required for Statcast sync") from exc
        return pd.concat([left, right], ignore_index=True)


def statcast_with_retry(statcast_callable, chunk_start: date, chunk_end: date, *, attempts: int = 3):
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            return statcast_callable(
                start_dt=chunk_start.isoformat(),
                end_dt=chunk_end.isoformat(),
                verbose=False,
                parallel=False,
            )
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                sleep(1.0 + attempt)
    if last_error is not None:
        raise last_error
    raise RuntimeError(
        f"Statcast request failed for {chunk_start.isoformat()} through {chunk_end.isoformat()} with no exception details."
    )


def resolve_statcast_sync_windows(
    settings: Settings,
    *,
    connection=None,
    start_date: str | None = None,
    end_date: str | None = None,
    start_season: int | None = None,
    end_season: int | None = None,
    daily: bool = False,
    backfill_days: int = 3,
) -> list[StatcastSyncWindow]:
    if start_date and end_date:
        return [StatcastSyncWindow(date.fromisoformat(start_date), date.fromisoformat(end_date))]
    client = LiveStatsClient(settings)
    today = date.today()
    if daily:
        daily_window = resolve_daily_statcast_window(
            connection,
            client,
            today=today,
            season=today.year,
            backfill_days=backfill_days,
        )
        return [daily_window] if daily_window else []
    first_season = start_season or settings.live_season or today.year
    last_season = end_season or first_season
    windows: list[StatcastSyncWindow] = []
    for season in range(first_season, last_season + 1):
        bounds = resolve_statcast_season_bounds(client, season, today)
        if bounds:
            windows.append(bounds)
    return windows


def resolve_daily_statcast_window(
    connection,
    client: LiveStatsClient,
    *,
    today: date,
    season: int,
    backfill_days: int,
) -> StatcastSyncWindow | None:
    bounds = resolve_statcast_season_bounds(client, season, today)
    if bounds is None:
        return None
    latest_known = latest_synced_statcast_date(connection, season)
    if latest_known is None:
        return bounds
    start_value = max(bounds.start_date, latest_known - timedelta(days=max(0, backfill_days)))
    if start_value > bounds.end_date:
        return None
    return StatcastSyncWindow(start_value, bounds.end_date)


def latest_synced_statcast_date(connection, season: int) -> date | None:
    if connection is None:
        return None
    metadata_value = get_metadata_value(connection, f"{STATCAST_METADATA_MAX_SYNCED_PREFIX}{season}")
    if metadata_value:
        return date.fromisoformat(metadata_value)
    if not table_exists(connection, "statcast_team_games"):
        return None
    row = connection.execute(
        "SELECT MAX(game_date) AS max_date FROM statcast_team_games WHERE season = ?",
        (season,),
    ).fetchone()
    max_date = row["max_date"] if row is not None else None
    return date.fromisoformat(str(max_date)) if max_date else None


def resolve_statcast_season_bounds(
    client: LiveStatsClient,
    season: int,
    today: date,
) -> StatcastSyncWindow | None:
    payload = client.fetch_json(
        f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={season}&gameType=R"
    )
    dates = [date.fromisoformat(item["date"]) for item in payload.get("dates", []) if item.get("date")]
    if not dates:
        return None
    start_value = min(dates)
    end_value = min(max(dates), today) if season >= today.year else max(dates)
    if end_value < start_value:
        return None
    return StatcastSyncWindow(start_value, end_value)


def iter_sync_chunks(start_date: date, end_date: date, chunk_days: int) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    cursor = start_date
    delta = max(1, chunk_days)
    while cursor <= end_date:
        chunk_end = min(end_date, cursor + timedelta(days=delta - 1))
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def aggregate_statcast_team_games(frame) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required for Statcast aggregation") from exc

    if frame is None or frame.empty:
        return []
    final = frame[frame["events"].notna()].copy()
    if "game_type" in final.columns:
        final = final[final["game_type"].fillna("").eq("R")]
    final = final[final["events"] != "truncated_pa"]
    if final.empty:
        return []

    top_mask = final["inning_topbot"].fillna("").str.lower().eq("top")
    final["batting_team"] = final["away_team"].where(top_mask, final["home_team"])
    final["pitching_team"] = final["home_team"].where(top_mask, final["away_team"])
    final["is_home"] = (~top_mask).astype(int)

    event_series = final["events"].fillna("")
    launch_speed = pd.to_numeric(final["launch_speed"], errors="coerce")
    launch_angle = pd.to_numeric(final["launch_angle"], errors="coerce")
    estimated_ba = pd.to_numeric(final["estimated_ba_using_speedangle"], errors="coerce").fillna(0.0)
    estimated_woba = pd.to_numeric(final["estimated_woba_using_speedangle"], errors="coerce").fillna(0.0)
    estimated_slg = pd.to_numeric(final["estimated_slg_using_speedangle"], errors="coerce").fillna(0.0)
    woba_denom = pd.to_numeric(final["woba_denom"], errors="coerce").fillna(0.0)
    game_dates = pd.to_datetime(final["game_date"], errors="coerce")

    is_ab = ~event_series.isin(NO_AB_EVENTS)
    is_hit = event_series.isin(HIT_EVENTS)
    is_strikeout = event_series.str.contains("strikeout", case=False, na=False)
    tracked_bbe = launch_speed.notna()
    hard_hit = tracked_bbe & (launch_speed >= 95.0)
    barrel = tracked_bbe & is_barrel_series(launch_speed, launch_angle)

    final["season"] = game_dates.dt.year.astype(int)
    final["game_date_iso"] = game_dates.dt.strftime("%Y-%m-%d")
    final["plate_appearances"] = 1
    final["at_bats"] = is_ab.astype(int)
    final["hits"] = is_hit.astype(int)
    final["strikeouts"] = is_strikeout.astype(int)
    final["batted_ball_events"] = tracked_bbe.astype(int)
    final["xba_numerator"] = estimated_ba.where(is_ab, 0.0)
    final["xwoba_numerator"] = estimated_woba * woba_denom
    final["xwoba_denom"] = woba_denom
    final["xslg_numerator"] = estimated_slg.where(is_ab, 0.0)
    final["hard_hit_bbe"] = hard_hit.astype(int)
    final["barrel_bbe"] = barrel.astype(int)
    final["launch_speed_sum"] = launch_speed.fillna(0.0)
    final["launch_speed_count"] = tracked_bbe.astype(int)

    grouped = (
        final.groupby(
            ["season", "game_date_iso", "game_pk", "batting_team", "pitching_team", "is_home"],
            dropna=False,
        )[
            [
                "plate_appearances",
                "at_bats",
                "hits",
                "strikeouts",
                "batted_ball_events",
                "xba_numerator",
                "xwoba_numerator",
                "xwoba_denom",
                "xslg_numerator",
                "hard_hit_bbe",
                "barrel_bbe",
                "launch_speed_sum",
                "launch_speed_count",
            ]
        ]
        .sum()
        .reset_index()
    )
    grouped["team_name"] = grouped["batting_team"].map(TEAM_NAMES).fillna(grouped["batting_team"])
    grouped["opponent_name"] = grouped["pitching_team"].map(TEAM_NAMES).fillna(grouped["pitching_team"])
    grouped.rename(
        columns={
            "game_date_iso": "game_date",
            "batting_team": "team",
            "pitching_team": "opponent",
        },
        inplace=True,
    )
    return grouped.to_dict(orient="records")


def aggregate_statcast_pitcher_games(frame) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required for Statcast aggregation") from exc

    if frame is None or frame.empty:
        return []
    pitches = frame.copy()
    if "game_type" in pitches.columns:
        pitches = pitches[pitches["game_type"].fillna("").eq("R")]
    if pitches.empty:
        return []

    top_mask = pitches["inning_topbot"].fillna("").str.lower().eq("top")
    pitches["pitching_team"] = pitches["home_team"].where(top_mask, pitches["away_team"])
    pitches["batting_team"] = pitches["away_team"].where(top_mask, pitches["home_team"])
    pitches["season"] = pd.to_datetime(pitches["game_date"], errors="coerce").dt.year.astype("Int64")
    pitches["game_date_iso"] = pd.to_datetime(pitches["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    pitches["pitcher_id"] = pd.to_numeric(pitches["pitcher"], errors="coerce").astype("Int64")
    pitches["pitcher_name_fmt"] = pitches["player_name"].fillna("").map(format_statcast_pitcher_name)
    release_speed = pd.to_numeric(pitches["release_speed"], errors="coerce")
    pitch_family = pitches["pitch_type"].fillna("").map(PITCH_FAMILY_BY_CODE).fillna("")
    event_series = pitches["events"].fillna("").str.lower()
    is_strikeout = event_series.isin(STRIKEOUT_EVENTS)

    pitches["total_pitches"] = 1
    pitches["release_speed_num"] = release_speed
    pitches["pitches_95_plus"] = (release_speed > 95.0).fillna(False).astype(int)
    pitches["pitches_97_plus"] = (release_speed > 97.0).fillna(False).astype(int)
    pitches["pitches_98_plus"] = (release_speed > 98.0).fillna(False).astype(int)
    pitches["pitches_99_plus"] = (release_speed > 99.0).fillna(False).astype(int)
    pitches["pitches_100_plus"] = (release_speed > 100.0).fillna(False).astype(int)
    pitches["pitches_101_plus"] = (release_speed > 101.0).fillna(False).astype(int)
    pitches["pitches_102_plus"] = (release_speed > 102.0).fillna(False).astype(int)
    pitches["fastball_pitches"] = (pitch_family == "fastball").astype(int)
    pitches["fastball_strikeouts"] = ((pitch_family == "fastball") & is_strikeout).astype(int)
    pitches["changeup_pitches"] = (pitch_family == "changeup").astype(int)
    pitches["changeup_strikeouts"] = ((pitch_family == "changeup") & is_strikeout).astype(int)
    pitches["curveball_pitches"] = (pitch_family == "curveball").astype(int)
    pitches["curveball_strikeouts"] = ((pitch_family == "curveball") & is_strikeout).astype(int)
    pitches["slider_pitches"] = (pitch_family == "slider").astype(int)
    pitches["slider_strikeouts"] = ((pitch_family == "slider") & is_strikeout).astype(int)

    grouped = (
        pitches[
            pitches["game_date_iso"].notna()
            & pitches["pitcher_id"].notna()
            & pitches["pitcher_name_fmt"].ne("")
        ]
        .groupby(
            ["season", "game_date_iso", "game_pk", "pitcher_id", "pitcher_name_fmt", "pitching_team", "batting_team"],
            dropna=False,
        )
        .agg(
            total_pitches=("total_pitches", "sum"),
            max_release_speed=("release_speed_num", "max"),
            pitches_95_plus=("pitches_95_plus", "sum"),
            pitches_97_plus=("pitches_97_plus", "sum"),
            pitches_98_plus=("pitches_98_plus", "sum"),
            pitches_99_plus=("pitches_99_plus", "sum"),
            pitches_100_plus=("pitches_100_plus", "sum"),
            pitches_101_plus=("pitches_101_plus", "sum"),
            pitches_102_plus=("pitches_102_plus", "sum"),
            fastball_pitches=("fastball_pitches", "sum"),
            fastball_strikeouts=("fastball_strikeouts", "sum"),
            changeup_pitches=("changeup_pitches", "sum"),
            changeup_strikeouts=("changeup_strikeouts", "sum"),
            curveball_pitches=("curveball_pitches", "sum"),
            curveball_strikeouts=("curveball_strikeouts", "sum"),
            slider_pitches=("slider_pitches", "sum"),
            slider_strikeouts=("slider_strikeouts", "sum"),
        )
        .reset_index()
    )
    grouped["team_name"] = grouped["pitching_team"].map(TEAM_NAMES).fillna(grouped["pitching_team"])
    grouped["opponent_name"] = grouped["batting_team"].map(TEAM_NAMES).fillna(grouped["batting_team"])
    grouped.rename(
        columns={
            "game_date_iso": "game_date",
            "pitcher_name_fmt": "pitcher_name",
            "pitching_team": "team",
            "batting_team": "opponent",
        },
        inplace=True,
    )
    grouped["season"] = grouped["season"].fillna(0).astype(int)
    grouped["pitcher_id"] = grouped["pitcher_id"].fillna(0).astype(int)
    return grouped.to_dict(orient="records")


def is_barrel(exit_velocity: float | None, launch_angle: float | None) -> bool:
    if exit_velocity is None or launch_angle is None:
        return False
    if exit_velocity < 98:
        return False
    capped_velocity = min(exit_velocity, 116)
    angle_buffer = capped_velocity - 98
    lower_bound = max(8, 26 - angle_buffer)
    upper_bound = min(50, 30 + angle_buffer)
    return lower_bound <= launch_angle <= upper_bound


def is_barrel_series(exit_velocity, launch_angle):
    capped_velocity = exit_velocity.clip(lower=98, upper=116)
    angle_buffer = capped_velocity - 98
    lower_bound = 26 - angle_buffer
    upper_bound = 30 + angle_buffer
    lower_bound = lower_bound.clip(lower=8)
    upper_bound = upper_bound.clip(upper=50)
    return (exit_velocity >= 98) & launch_angle.notna() & (launch_angle >= lower_bound) & (launch_angle <= upper_bound)


def format_statcast_pitcher_name(value: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned or "," not in cleaned:
        return cleaned
    last_name, first_name = [part.strip() for part in cleaned.split(",", 1)]
    return f"{first_name} {last_name}".strip()
