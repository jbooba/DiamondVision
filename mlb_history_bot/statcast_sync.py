from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import re
from time import sleep
from typing import Any

from .config import Settings
from .live import LiveStatsClient
from .storage import (
    replace_statcast_batter_games,
    replace_statcast_batter_pitch_type_games,
    replace_statcast_events,
    get_connection,
    get_metadata_value,
    replace_statcast_pitch_type_games,
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
    total_batter_games = 0
    total_pitch_type_games = 0
    total_batter_pitch_type_games = 0
    total_events = 0
    total_windows = 0
    for window in sync_windows:
        window_rows = 0
        window_pitcher_rows = 0
        window_batter_rows = 0
        window_pitch_type_rows = 0
        window_batter_pitch_type_rows = 0
        window_event_rows = 0
        for chunk_start, chunk_end in iter_sync_chunks(window.start_date, window.end_date, chunk_days):
            frame = fetch_statcast_frame(statcast, chunk_start, chunk_end)
            rows = aggregate_statcast_team_games(frame)
            pitcher_rows = aggregate_statcast_pitcher_games(frame)
            batter_rows = aggregate_statcast_batter_games(frame)
            pitch_type_rows = aggregate_statcast_pitch_type_games(frame)
            batter_pitch_type_rows = aggregate_statcast_batter_pitch_type_games(frame)
            event_rows = aggregate_statcast_events(frame)
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
            replace_statcast_batter_games(
                connection,
                start_date=chunk_start.isoformat(),
                end_date=chunk_end.isoformat(),
                rows=batter_rows,
            )
            replace_statcast_pitch_type_games(
                connection,
                start_date=chunk_start.isoformat(),
                end_date=chunk_end.isoformat(),
                rows=pitch_type_rows,
            )
            replace_statcast_batter_pitch_type_games(
                connection,
                start_date=chunk_start.isoformat(),
                end_date=chunk_end.isoformat(),
                rows=batter_pitch_type_rows,
            )
            replace_statcast_events(
                connection,
                start_date=chunk_start.isoformat(),
                end_date=chunk_end.isoformat(),
                rows=event_rows,
            )
            total_team_games += len(rows)
            total_pitcher_games += len(pitcher_rows)
            total_batter_games += len(batter_rows)
            total_pitch_type_games += len(pitch_type_rows)
            total_batter_pitch_type_games += len(batter_pitch_type_rows)
            total_events += len(event_rows)
            window_rows += len(rows)
            window_pitcher_rows += len(pitcher_rows)
            window_batter_rows += len(batter_rows)
            window_pitch_type_rows += len(pitch_type_rows)
            window_batter_pitch_type_rows += len(batter_pitch_type_rows)
            window_event_rows += len(event_rows)
            total_windows += 1
        set_metadata_value(
            connection,
            f"{STATCAST_METADATA_MAX_SYNCED_PREFIX}{window.end_date.year}",
            window.end_date.isoformat(),
        )
        messages.append(
            f"Synced Statcast team-game aggregates for {window.start_date.isoformat()} through "
            f"{window.end_date.isoformat()} ({window_rows} team-game row(s), {window_pitcher_rows} pitcher-game row(s), "
            f"{window_batter_rows} batter-game row(s), {window_pitch_type_rows} pitcher pitch-type row(s), "
            f"{window_batter_pitch_type_rows} batter pitch-type row(s), {window_event_rows} compact event row(s))."
        )
    if daily:
        set_metadata_value(connection, STATCAST_METADATA_LAST_RUN_KEY, date.today().isoformat())
    connection.close()
    messages.append(
        f"Statcast sync complete across {total_windows} chunk(s) with {total_team_games} team-game row(s) "
        f"{total_pitcher_games} pitcher-game row(s), {total_batter_games} batter-game row(s), "
        f"{total_pitch_type_games} pitcher pitch-type row(s), {total_batter_pitch_type_games} batter pitch-type row(s), "
        f"and {total_events} compact event row(s) stored."
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


def prepare_statcast_pitch_rows(frame):
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required for Statcast aggregation") from exc

    if frame is None or frame.empty:
        return pd.DataFrame()

    pitches = frame.copy()
    if "game_type" in pitches.columns:
        pitches = pitches[pitches["game_type"].fillna("").eq("R")]
    if pitches.empty:
        return pitches

    top_mask = pitches["inning_topbot"].fillna("").str.lower().eq("top")
    pitches["batting_team"] = pitches["away_team"].where(top_mask, pitches["home_team"])
    pitches["pitching_team"] = pitches["home_team"].where(top_mask, pitches["away_team"])
    game_dates = pd.to_datetime(pitches["game_date"], errors="coerce")
    pitches["season"] = game_dates.dt.year.astype("Int64")
    pitches["game_date_iso"] = game_dates.dt.strftime("%Y-%m-%d")
    pitches["game_pk"] = pd.to_numeric(column_or_default(pitches, "game_pk"), errors="coerce").astype("Int64")
    pitches["at_bat_number"] = pd.to_numeric(column_or_default(pitches, "at_bat_number"), errors="coerce").astype("Int64")
    pitches["pitch_number"] = pd.to_numeric(column_or_default(pitches, "pitch_number"), errors="coerce").astype("Int64")
    pitches["pitcher_id"] = pd.to_numeric(column_or_default(pitches, "pitcher"), errors="coerce").astype("Int64")
    pitches["pitcher_name_fmt"] = column_or_default(pitches, "player_name").fillna("").map(format_statcast_pitcher_name)
    pitches["batter_id"] = pd.to_numeric(column_or_default(pitches, "batter"), errors="coerce").astype("Int64")
    pitches["pitch_type_code"] = column_or_default(pitches, "pitch_type").fillna("").astype(str).str.upper()
    pitches["pitch_family"] = pitches["pitch_type_code"].map(PITCH_FAMILY_BY_CODE).fillna("")
    pitches["pitch_name_fmt"] = column_or_default(pitches, "pitch_name").fillna("").astype(str).str.strip()
    pitches["release_speed_num"] = pd.to_numeric(column_or_default(pitches, "release_speed"), errors="coerce")
    pitches["release_spin_rate_num"] = pd.to_numeric(column_or_default(pitches, "release_spin_rate"), errors="coerce")
    pitches["launch_speed_num"] = pd.to_numeric(column_or_default(pitches, "launch_speed"), errors="coerce")
    pitches["launch_angle_num"] = pd.to_numeric(column_or_default(pitches, "launch_angle"), errors="coerce")
    pitches["bat_speed_num"] = pd.to_numeric(column_or_default(pitches, "bat_speed"), errors="coerce")
    pitches["estimated_ba_num"] = pd.to_numeric(column_or_default(pitches, "estimated_ba_using_speedangle"), errors="coerce").fillna(0.0)
    pitches["estimated_woba_num"] = pd.to_numeric(column_or_default(pitches, "estimated_woba_using_speedangle"), errors="coerce").fillna(0.0)
    pitches["estimated_slg_num"] = pd.to_numeric(column_or_default(pitches, "estimated_slg_using_speedangle"), errors="coerce").fillna(0.0)
    pitches["woba_denom_num"] = pd.to_numeric(column_or_default(pitches, "woba_denom"), errors="coerce").fillna(0.0)
    pitches["event_name"] = column_or_default(pitches, "events").fillna("").astype(str).str.lower()
    description_series = column_or_default(pitches, "des")
    if description_series.eq("").all():
        description_series = column_or_default(pitches, "description")
    pitches["description_text"] = description_series.fillna("").astype(str)
    pitches["batter_name_fmt"] = pitches["description_text"].map(extract_batter_name_from_description)
    pitches["tracked_bbe"] = pitches["launch_speed_num"].notna().astype(int)
    pitches["hard_hit_bbe"] = ((pitches["launch_speed_num"] >= 95.0) & pitches["launch_speed_num"].notna()).astype(int)
    pitches["barrel_bbe"] = is_barrel_series(pitches["launch_speed_num"], pitches["launch_angle_num"]).astype(int)
    pitches["is_ab"] = (~pitches["event_name"].isin(NO_AB_EVENTS)).astype(int)
    pitches["is_hit"] = pitches["event_name"].isin(HIT_EVENTS).astype(int)
    pitches["is_single"] = pitches["event_name"].eq("single").astype(int)
    pitches["is_double"] = pitches["event_name"].eq("double").astype(int)
    pitches["is_triple"] = pitches["event_name"].eq("triple").astype(int)
    pitches["is_home_run"] = pitches["event_name"].eq("home_run").astype(int)
    pitches["is_xbh"] = pitches["event_name"].isin({"double", "triple", "home_run"}).astype(int)
    pitches["is_walk"] = pitches["event_name"].isin({"walk", "intent_walk"}).astype(int)
    pitches["is_strikeout"] = pitches["event_name"].isin(STRIKEOUT_EVENTS).astype(int)
    pitches["rbi_num"] = pd.to_numeric(column_or_default(pitches, "rbi"), errors="coerce").fillna(0.0)
    description_column = column_or_default(pitches, "description").fillna("").astype(str).str.lower()
    pitches["called_strike"] = description_column.isin({"called_strike", "called strike"}).astype(int)
    description_lower = description_column.where(description_column.ne(""), pitches["description_text"].str.lower())
    pitches["swinging_strike"] = description_lower.isin({"swinging_strike", "swinging_strike_blocked", "swinging strike", "swinging strike blocked"}).astype(int)
    pitches["whiff"] = pitches["swinging_strike"]
    pitches["has_risp"] = (
        pd.to_numeric(column_or_default(pitches, "on_2b"), errors="coerce").fillna(0).ne(0)
        | pd.to_numeric(column_or_default(pitches, "on_3b"), errors="coerce").fillna(0).ne(0)
    ).astype(int)
    plate_x = pd.to_numeric(column_or_default(pitches, "plate_x"), errors="coerce")
    plate_z = pd.to_numeric(column_or_default(pitches, "plate_z"), errors="coerce")
    sz_top = pd.to_numeric(column_or_default(pitches, "sz_top"), errors="coerce")
    sz_bot = pd.to_numeric(column_or_default(pitches, "sz_bot"), errors="coerce")
    stand = column_or_default(pitches, "stand").fillna("").astype(str).str.upper()
    pitches["horizontal_location"] = classify_horizontal_location_series(plate_x, stand)
    pitches["vertical_location"] = classify_vertical_location_series(plate_z, sz_top, sz_bot)
    pitches["field_direction"] = pitches["description_text"].map(extract_field_direction_from_description)
    dedupe_subset = ["game_pk", "at_bat_number", "pitch_number"]
    identifiable = pitches[dedupe_subset].notna().all(axis=1)
    if identifiable.any():
        identified = pitches.loc[identifiable].drop_duplicates(subset=dedupe_subset, keep="last")
        unidentified = pitches.loc[~identifiable]
        pitches = pd.concat([identified, unidentified], ignore_index=True)
    return pitches


def prepare_statcast_final_events(frame):
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required for Statcast aggregation") from exc
    pitches = prepare_statcast_pitch_rows(frame)
    if pitches.empty:
        return pitches
    final = pitches[pitches["event_name"].ne("")].copy()
    final = final[final["event_name"] != "truncated_pa"]
    dedupe_subset = ["game_pk", "at_bat_number", "pitch_number"]
    identifiable = final[dedupe_subset].notna().all(axis=1)
    if identifiable.any():
        identified = final.loc[identifiable].drop_duplicates(subset=dedupe_subset, keep="last")
        unidentified = final.loc[~identifiable]
        final = pd.concat([identified, unidentified], ignore_index=True)
    return final


def aggregate_statcast_team_games(frame) -> list[dict[str, Any]]:
    final = prepare_statcast_final_events(frame)
    if final.empty:
        return []
    final = final[
        final["game_pk"].notna()
        & final["game_date_iso"].notna()
        & final["batting_team"].fillna("").ne("")
    ].copy()
    if final.empty:
        return []
    final["is_home"] = final["batting_team"].eq(final["home_team"]).astype(int)
    final["plate_appearances"] = 1
    final["at_bats"] = final["is_ab"]
    final["hits"] = final["is_hit"]
    final["strikeouts"] = final["is_strikeout"]
    final["batted_ball_events"] = final["tracked_bbe"]
    final["xba_numerator"] = final["estimated_ba_num"].where(final["is_ab"].astype(bool), 0.0)
    final["xwoba_numerator"] = final["estimated_woba_num"] * final["woba_denom_num"]
    final["xwoba_denom"] = final["woba_denom_num"]
    final["xslg_numerator"] = final["estimated_slg_num"].where(final["is_ab"].astype(bool), 0.0)
    final["launch_speed_sum"] = final["launch_speed_num"].fillna(0.0)
    final["launch_speed_count"] = final["tracked_bbe"]

    grouped = (
        final.groupby(
            ["season", "game_date_iso", "game_pk", "batting_team"],
            dropna=False,
        )
        .agg(
            opponent=("pitching_team", "first"),
            is_home=("is_home", "first"),
            plate_appearances=("plate_appearances", "sum"),
            at_bats=("at_bats", "sum"),
            hits=("hits", "sum"),
            strikeouts=("strikeouts", "sum"),
            batted_ball_events=("batted_ball_events", "sum"),
            xba_numerator=("xba_numerator", "sum"),
            xwoba_numerator=("xwoba_numerator", "sum"),
            xwoba_denom=("xwoba_denom", "sum"),
            xslg_numerator=("xslg_numerator", "sum"),
            hard_hit_bbe=("hard_hit_bbe", "sum"),
            barrel_bbe=("barrel_bbe", "sum"),
            launch_speed_sum=("launch_speed_sum", "sum"),
            launch_speed_count=("launch_speed_count", "sum"),
        )
        .reset_index()
    )
    grouped["team_name"] = grouped["batting_team"].map(TEAM_NAMES).fillna(grouped["batting_team"])
    grouped["opponent_name"] = grouped["opponent"].map(TEAM_NAMES).fillna(grouped["opponent"])
    grouped.rename(
        columns={
            "game_date_iso": "game_date",
            "batting_team": "team",
        },
        inplace=True,
    )
    grouped["season"] = grouped["season"].fillna(0).astype(int)
    grouped["game_pk"] = grouped["game_pk"].fillna(0).astype(int)
    return grouped.to_dict(orient="records")


def aggregate_statcast_pitcher_games(frame) -> list[dict[str, Any]]:
    pitches = prepare_statcast_pitch_rows(frame)
    if pitches.empty:
        return []
    pitches["total_pitches"] = 1
    release_speed = pitches["release_speed_num"]
    pitch_family = pitches["pitch_family"]
    pitches["pitches_95_plus"] = (release_speed > 95.0).fillna(False).astype(int)
    pitches["pitches_97_plus"] = (release_speed > 97.0).fillna(False).astype(int)
    pitches["pitches_98_plus"] = (release_speed > 98.0).fillna(False).astype(int)
    pitches["pitches_99_plus"] = (release_speed > 99.0).fillna(False).astype(int)
    pitches["pitches_100_plus"] = (release_speed > 100.0).fillna(False).astype(int)
    pitches["pitches_101_plus"] = (release_speed > 101.0).fillna(False).astype(int)
    pitches["pitches_102_plus"] = (release_speed > 102.0).fillna(False).astype(int)
    pitches["fastball_pitches"] = (pitch_family == "fastball").astype(int)
    pitches["fastball_strikeouts"] = ((pitch_family == "fastball") & pitches["is_strikeout"].astype(bool)).astype(int)
    pitches["changeup_pitches"] = (pitch_family == "changeup").astype(int)
    pitches["changeup_strikeouts"] = ((pitch_family == "changeup") & pitches["is_strikeout"].astype(bool)).astype(int)
    pitches["curveball_pitches"] = (pitch_family == "curveball").astype(int)
    pitches["curveball_strikeouts"] = ((pitch_family == "curveball") & pitches["is_strikeout"].astype(bool)).astype(int)
    pitches["slider_pitches"] = (pitch_family == "slider").astype(int)
    pitches["slider_strikeouts"] = ((pitch_family == "slider") & pitches["is_strikeout"].astype(bool)).astype(int)

    grouped = (
        pitches[
            pitches["game_date_iso"].notna()
            & pitches["game_pk"].notna()
            & pitches["pitcher_id"].notna()
            & pitches["pitcher_name_fmt"].ne("")
        ]
        .groupby(
            ["season", "game_date_iso", "game_pk", "pitcher_id"],
            dropna=False,
        )
        .agg(
            pitcher_name=("pitcher_name_fmt", "first"),
            team=("pitching_team", "first"),
            opponent=("batting_team", "first"),
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
    grouped["team_name"] = grouped["team"].map(TEAM_NAMES).fillna(grouped["team"])
    grouped["opponent_name"] = grouped["opponent"].map(TEAM_NAMES).fillna(grouped["opponent"])
    grouped.rename(
        columns={
            "game_date_iso": "game_date",
        },
        inplace=True,
    )
    grouped["season"] = grouped["season"].fillna(0).astype(int)
    grouped["game_pk"] = grouped["game_pk"].fillna(0).astype(int)
    grouped["pitcher_id"] = grouped["pitcher_id"].fillna(0).astype(int)
    return grouped.to_dict(orient="records")


def aggregate_statcast_batter_games(frame) -> list[dict[str, Any]]:
    final = prepare_statcast_final_events(frame)
    if final.empty:
        return []
    final = final[
        final["game_date_iso"].notna()
        & final["game_pk"].notna()
        & final["batter_id"].notna()
        & final["batter_name_fmt"].ne("")
    ].copy()
    if final.empty:
        return []
    final["plate_appearances"] = 1
    final["at_bats"] = final["is_ab"]
    final["hits"] = final["is_hit"]
    final["singles"] = final["is_single"]
    final["doubles"] = final["is_double"]
    final["triples"] = final["is_triple"]
    final["home_runs"] = final["is_home_run"]
    final["walks"] = final["is_walk"]
    final["strikeouts"] = final["is_strikeout"]
    final["runs_batted_in"] = final["rbi_num"].fillna(0).astype(int)
    final["batted_ball_events"] = final["tracked_bbe"]
    final["xba_numerator"] = final["estimated_ba_num"].where(final["is_ab"].astype(bool), 0.0)
    final["xwoba_numerator"] = final["estimated_woba_num"] * final["woba_denom_num"]
    final["xwoba_denom"] = final["woba_denom_num"]
    final["xslg_numerator"] = final["estimated_slg_num"].where(final["is_ab"].astype(bool), 0.0)
    final["launch_speed_sum"] = final["launch_speed_num"].fillna(0.0)
    final["launch_speed_count"] = final["tracked_bbe"]
    grouped = (
        final.groupby(
            ["season", "game_date_iso", "game_pk", "batter_id"],
            dropna=False,
        )
        .agg(
            batter_name=("batter_name_fmt", "first"),
            team=("batting_team", "first"),
            opponent=("pitching_team", "first"),
            plate_appearances=("plate_appearances", "sum"),
            at_bats=("at_bats", "sum"),
            hits=("hits", "sum"),
            singles=("singles", "sum"),
            doubles=("doubles", "sum"),
            triples=("triples", "sum"),
            home_runs=("home_runs", "sum"),
            walks=("walks", "sum"),
            strikeouts=("strikeouts", "sum"),
            runs_batted_in=("runs_batted_in", "sum"),
            batted_ball_events=("batted_ball_events", "sum"),
            xba_numerator=("xba_numerator", "sum"),
            xwoba_numerator=("xwoba_numerator", "sum"),
            xwoba_denom=("xwoba_denom", "sum"),
            xslg_numerator=("xslg_numerator", "sum"),
            hard_hit_bbe=("hard_hit_bbe", "sum"),
            barrel_bbe=("barrel_bbe", "sum"),
            launch_speed_sum=("launch_speed_sum", "sum"),
            launch_speed_count=("launch_speed_count", "sum"),
            max_launch_speed=("launch_speed_num", "max"),
            avg_bat_speed=("bat_speed_num", "mean"),
            max_bat_speed=("bat_speed_num", "max"),
        )
        .reset_index()
    )
    grouped["team_name"] = grouped["team"].map(TEAM_NAMES).fillna(grouped["team"])
    grouped["opponent_name"] = grouped["opponent"].map(TEAM_NAMES).fillna(grouped["opponent"])
    grouped.rename(columns={"game_date_iso": "game_date"}, inplace=True)
    grouped["season"] = grouped["season"].fillna(0).astype(int)
    grouped["game_pk"] = grouped["game_pk"].fillna(0).astype(int)
    grouped["batter_id"] = grouped["batter_id"].fillna(0).astype(int)
    return grouped.to_dict(orient="records")


def aggregate_statcast_pitch_type_games(frame) -> list[dict[str, Any]]:
    pitches = prepare_statcast_pitch_rows(frame)
    if pitches.empty:
        return []
    pitches = pitches[
        pitches["game_date_iso"].notna()
        & pitches["game_pk"].notna()
        & pitches["pitcher_id"].notna()
        & pitches["pitcher_name_fmt"].ne("")
        & pitches["pitch_type_code"].ne("")
    ].copy()
    if pitches.empty:
        return []
    pitches["pitches"] = 1
    grouped = (
        pitches.groupby(
            [
                "season",
                "game_date_iso",
                "game_pk",
                "pitcher_id",
                "pitch_type_code",
            ],
            dropna=False,
        )
        .agg(
            pitcher_name=("pitcher_name_fmt", "first"),
            team=("pitching_team", "first"),
            opponent=("batting_team", "first"),
            pitch_name=("pitch_name_fmt", "first"),
            pitch_family=("pitch_family", "first"),
            pitches=("pitches", "sum"),
            avg_release_speed=("release_speed_num", "mean"),
            max_release_speed=("release_speed_num", "max"),
            avg_release_spin_rate=("release_spin_rate_num", "mean"),
            max_release_spin_rate=("release_spin_rate_num", "max"),
            called_strikes=("called_strike", "sum"),
            swinging_strikes=("swinging_strike", "sum"),
            whiffs=("whiff", "sum"),
            strikeouts=("is_strikeout", "sum"),
            walks=("is_walk", "sum"),
            hits_allowed=("is_hit", "sum"),
            extra_base_hits_allowed=("is_xbh", "sum"),
            home_runs_allowed=("is_home_run", "sum"),
            batted_ball_events=("tracked_bbe", "sum"),
            xba_numerator=("estimated_ba_num", "sum"),
            xwoba_numerator=("estimated_woba_num", "sum"),
            xwoba_denom=("woba_denom_num", "sum"),
            xslg_numerator=("estimated_slg_num", "sum"),
            launch_speed_sum=("launch_speed_num", "sum"),
            launch_speed_count=("tracked_bbe", "sum"),
        )
        .reset_index()
    )
    grouped["team_name"] = grouped["team"].map(TEAM_NAMES).fillna(grouped["team"])
    grouped["opponent_name"] = grouped["opponent"].map(TEAM_NAMES).fillna(grouped["opponent"])
    grouped.rename(columns={"game_date_iso": "game_date", "pitch_type_code": "pitch_type"}, inplace=True)
    grouped["season"] = grouped["season"].fillna(0).astype(int)
    grouped["game_pk"] = grouped["game_pk"].fillna(0).astype(int)
    grouped["pitcher_id"] = grouped["pitcher_id"].fillna(0).astype(int)
    return grouped.to_dict(orient="records")


def aggregate_statcast_batter_pitch_type_games(frame) -> list[dict[str, Any]]:
    final = prepare_statcast_final_events(frame)
    if final.empty:
        return []
    final = final[
        final["game_date_iso"].notna()
        & final["game_pk"].notna()
        & final["batter_id"].notna()
        & final["batter_name_fmt"].ne("")
        & final["pitch_type_code"].ne("")
    ].copy()
    if final.empty:
        return []
    final["plate_appearances"] = 1
    final["at_bats"] = final["is_ab"]
    final["hits"] = final["is_hit"]
    final["singles"] = final["is_single"]
    final["doubles"] = final["is_double"]
    final["triples"] = final["is_triple"]
    final["home_runs"] = final["is_home_run"]
    final["walks"] = final["is_walk"]
    final["strikeouts"] = final["is_strikeout"]
    final["runs_batted_in"] = final["rbi_num"].fillna(0).astype(int)
    final["batted_ball_events"] = final["tracked_bbe"]
    final["xba_numerator"] = final["estimated_ba_num"].where(final["is_ab"].astype(bool), 0.0)
    final["xwoba_numerator"] = final["estimated_woba_num"] * final["woba_denom_num"]
    final["xwoba_denom"] = final["woba_denom_num"]
    final["xslg_numerator"] = final["estimated_slg_num"].where(final["is_ab"].astype(bool), 0.0)
    final["launch_speed_sum"] = final["launch_speed_num"].fillna(0.0)
    final["launch_speed_count"] = final["tracked_bbe"]
    grouped = (
        final.groupby(
            [
                "season",
                "game_date_iso",
                "game_pk",
                "batter_id",
                "pitch_type_code",
            ],
            dropna=False,
        )
        .agg(
            batter_name=("batter_name_fmt", "first"),
            team=("batting_team", "first"),
            opponent=("pitching_team", "first"),
            pitch_name=("pitch_name_fmt", "first"),
            pitch_family=("pitch_family", "first"),
            plate_appearances=("plate_appearances", "sum"),
            at_bats=("at_bats", "sum"),
            hits=("hits", "sum"),
            singles=("singles", "sum"),
            doubles=("doubles", "sum"),
            triples=("triples", "sum"),
            home_runs=("home_runs", "sum"),
            walks=("walks", "sum"),
            strikeouts=("strikeouts", "sum"),
            runs_batted_in=("runs_batted_in", "sum"),
            batted_ball_events=("batted_ball_events", "sum"),
            xba_numerator=("xba_numerator", "sum"),
            xwoba_numerator=("xwoba_numerator", "sum"),
            xwoba_denom=("xwoba_denom", "sum"),
            xslg_numerator=("xslg_numerator", "sum"),
            hard_hit_bbe=("hard_hit_bbe", "sum"),
            barrel_bbe=("barrel_bbe", "sum"),
            launch_speed_sum=("launch_speed_sum", "sum"),
            launch_speed_count=("launch_speed_count", "sum"),
            avg_bat_speed=("bat_speed_num", "mean"),
            max_bat_speed=("bat_speed_num", "max"),
        )
        .reset_index()
    )
    grouped["team_name"] = grouped["team"].map(TEAM_NAMES).fillna(grouped["team"])
    grouped["opponent_name"] = grouped["opponent"].map(TEAM_NAMES).fillna(grouped["opponent"])
    grouped.rename(columns={"game_date_iso": "game_date", "pitch_type_code": "pitch_type"}, inplace=True)
    grouped["season"] = grouped["season"].fillna(0).astype(int)
    grouped["game_pk"] = grouped["game_pk"].fillna(0).astype(int)
    grouped["batter_id"] = grouped["batter_id"].fillna(0).astype(int)
    return grouped.to_dict(orient="records")


def aggregate_statcast_events(frame) -> list[dict[str, Any]]:
    final = prepare_statcast_final_events(frame)
    if final.empty:
        return []
    final = final[
        final["game_date_iso"].notna()
        & final["game_pk"].notna()
        & final["at_bat_number"].notna()
        & final["pitch_number"].notna()
        & final["batter_id"].notna()
        & final["pitcher_id"].notna()
    ].copy()
    if final.empty:
        return []
    rows = []
    for row in final.to_dict(orient="records"):
        rows.append(
            {
                "season": int(row.get("season") or 0),
                "game_date": str(row.get("game_date_iso") or ""),
                "game_pk": row.get("game_pk"),
                "at_bat_number": row.get("at_bat_number"),
                "pitch_number": row.get("pitch_number"),
                "batter_id": row.get("batter_id"),
                "batter_name": row.get("batter_name_fmt") or "",
                "pitcher_id": row.get("pitcher_id"),
                "pitcher_name": row.get("pitcher_name_fmt") or "",
                "batting_team": row.get("batting_team") or "",
                "pitching_team": row.get("pitching_team") or "",
                "home_team": row.get("home_team") or "",
                "away_team": row.get("away_team") or "",
                "stand": row.get("stand") or "",
                "p_throws": row.get("p_throws") or "",
                "pitch_type": row.get("pitch_type_code") or "",
                "pitch_name": row.get("pitch_name_fmt") or "",
                "pitch_family": row.get("pitch_family") or "",
                "event": row.get("event_name") or "",
                "is_ab": row.get("is_ab") or 0,
                "is_hit": row.get("is_hit") or 0,
                "is_xbh": row.get("is_xbh") or 0,
                "is_home_run": row.get("is_home_run") or 0,
                "is_strikeout": row.get("is_strikeout") or 0,
                "has_risp": row.get("has_risp") or 0,
                "horizontal_location": row.get("horizontal_location") or "",
                "vertical_location": row.get("vertical_location") or "",
                "field_direction": row.get("field_direction") or "",
                "release_speed": row.get("release_speed_num"),
                "release_spin_rate": row.get("release_spin_rate_num"),
                "launch_speed": row.get("launch_speed_num"),
                "launch_angle": row.get("launch_angle_num"),
                "hit_distance": safe_float_or_none(row.get("hit_distance_sc")),
                "bat_speed": row.get("bat_speed_num"),
                "estimated_ba": row.get("estimated_ba_num"),
                "estimated_woba": row.get("estimated_woba_num"),
                "estimated_slg": row.get("estimated_slg_num"),
            }
        )
    return rows


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
    result = (
        exit_velocity.notna()
        & launch_angle.notna()
        & (exit_velocity >= 98)
        & (launch_angle >= lower_bound)
        & (launch_angle <= upper_bound)
    )
    return result.fillna(False)


def format_statcast_pitcher_name(value: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned or "," not in cleaned:
        return cleaned
    last_name, first_name = [part.strip() for part in cleaned.split(",", 1)]
    return f"{first_name} {last_name}".strip()


def extract_batter_name_from_description(value: str) -> str:
    description = str(value or "").strip()
    match = re.match(
        r"(.+?)\s+(?:singles|doubles|triples|homers|hits a home run|hits a grand slam|walks|grounds out|flies out|lines out|pops out|strikes out|reaches)\b",
        description,
        re.IGNORECASE,
    )
    if match is None:
        return ""
    return " ".join(part.capitalize() for part in match.group(1).split())


def extract_field_direction_from_description(value: str) -> str:
    lowered = str(value or "").casefold()
    if "right-center" in lowered or "right center" in lowered:
        return "right center"
    if "left-center" in lowered or "left center" in lowered:
        return "left center"
    if "to right field" in lowered or "right field" in lowered:
        return "right field"
    if "to left field" in lowered or "left field" in lowered:
        return "left field"
    if "to center field" in lowered or "center field" in lowered:
        return "center field"
    return ""


def classify_horizontal_location_series(plate_x, stand):
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required for Statcast aggregation") from exc
    inside = ((stand == "R") & (plate_x <= -0.35)) | ((stand == "L") & (plate_x >= 0.35))
    outside = ((stand == "R") & (plate_x >= 0.35)) | ((stand == "L") & (plate_x <= -0.35))
    result = pd.Series(["middle"] * len(plate_x), index=plate_x.index, dtype="object")
    result[inside.fillna(False)] = "inside"
    result[outside.fillna(False)] = "outside"
    result[plate_x.isna()] = ""
    return result


def classify_vertical_location_series(plate_z, sz_top, sz_bot):
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required for Statcast aggregation") from exc
    zone_height = sz_top - sz_bot
    upper_cut = sz_bot + zone_height * 0.67
    lower_cut = sz_bot + zone_height * 0.33
    result = pd.Series(["middle"] * len(plate_z), index=plate_z.index, dtype="object")
    low_mask = plate_z.notna() & lower_cut.notna() & (plate_z <= lower_cut)
    high_mask = plate_z.notna() & upper_cut.notna() & (plate_z >= upper_cut)
    fallback_high = plate_z.notna() & upper_cut.isna() & (plate_z >= 3.0)
    fallback_low = plate_z.notna() & lower_cut.isna() & (plate_z <= 2.0)
    result[low_mask | fallback_low] = "low"
    result[high_mask | fallback_high] = "high"
    result[plate_z.isna()] = ""
    return result


def safe_float_or_none(value):
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def column_or_default(frame, name: str):
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required for Statcast aggregation") from exc
    series = frame.get(name)
    if series is not None:
        return series
    return pd.Series([""] * len(frame), index=frame.index)
