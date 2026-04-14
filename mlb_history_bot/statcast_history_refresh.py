from __future__ import annotations

import csv
import json
import re
from collections.abc import Iterable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


SAVANT_CUSTOM_HISTORY_BASE_URL = "https://baseballsavant.mlb.com/leaderboard/custom"
SAVANT_CUSTOM_HISTORY_START_YEAR = 1950

# Pulled from the user's expansive custom leaderboard and reused for both hitter/pitcher
# season-history snapshots so the bundled CSVs stay broad and consistent.
DEFAULT_SAVANT_HISTORY_SELECTIONS: tuple[str, ...] = (
    "player_age",
    "p_formatted_ip",
    "pa",
    "ab",
    "hit",
    "single",
    "double",
    "triple",
    "home_run",
    "strikeout",
    "walk",
    "k_percent",
    "bb_percent",
    "batting_avg",
    "slg_percent",
    "on_base_percent",
    "on_base_plus_slg",
    "isolated_power",
    "babip",
    "xba",
    "xslg",
    "woba",
    "xwoba",
    "xobp",
    "xiso",
    "wobacon",
    "xwobacon",
    "bacon",
    "xbacon",
    "xbadiff",
    "xslgdiff",
    "wobadiff",
    "avg_swing_speed",
    "fast_swing_rate",
    "blasts_contact",
    "blasts_swing",
    "squared_up_contact",
    "squared_up_swing",
    "avg_swing_length",
    "swords",
    "attack_angle",
    "attack_direction",
    "ideal_angle_rate",
    "vertical_swing_path",
    "exit_velocity_avg",
    "launch_angle_avg",
    "sweet_spot_percent",
    "barrel",
    "barrel_batted_rate",
    "solidcontact_percent",
    "flareburner_percent",
    "poorlyunder_percent",
    "poorlytopped_percent",
    "poorlyweak_percent",
    "hard_hit_percent",
    "avg_best_speed",
    "avg_hyper_speed",
    "z_swing_percent",
    "z_swing_miss_percent",
    "oz_swing_percent",
    "oz_swing_miss_percent",
    "oz_contact_percent",
    "out_zone_swing_miss",
    "out_zone_swing",
    "out_zone_percent",
    "out_zone",
    "meatball_swing_percent",
    "meatball_percent",
    "pitch_count_offspeed",
    "pitch_count_fastball",
    "pitch_count_breaking",
    "pitch_count",
    "iz_contact_percent",
    "in_zone_swing_miss",
    "in_zone_swing",
    "in_zone_percent",
    "in_zone",
    "edge_percent",
    "edge",
    "whiff_percent",
    "swing_percent",
    "pull_percent",
    "straightaway_percent",
    "opposite_percent",
    "batted_ball",
    "f_strike_percent",
    "groundballs_percent",
    "groundballs",
    "flyballs_percent",
    "flyballs",
    "linedrives_percent",
    "linedrives",
    "popups_percent",
    "popups",
)

ROLE_TO_FILE_NAME = {
    "batter": "Batter_Stats_Statcast_History.csv",
    "pitcher": "Pitcher_Stats_Statcast_History.csv",
}

PLAYER_NAME_HEADER = "last_name, first_name"
_DATA_RE = re.compile(r"var\s+data\s*=\s*(\[[\s\S]*?\]);")


def build_savant_custom_history_url(
    *,
    role: str,
    years: Sequence[int],
    selections: Sequence[str] = DEFAULT_SAVANT_HISTORY_SELECTIONS,
    minimum: int = 1,
) -> str:
    normalized_role = role.strip().lower()
    if normalized_role not in ROLE_TO_FILE_NAME:
        raise ValueError(f"Unsupported Savant custom-history role: {role}")
    if not years:
        raise ValueError("At least one season is required")
    ordered_years = ",".join(str(year) for year in sorted({int(year) for year in years}, reverse=True))
    params = {
        "year": ordered_years,
        "type": normalized_role,
        "filter": "",
        "min": str(int(minimum)),
        "selections": ",".join(selections),
        "chart": "false",
        "x": "player_age",
        "y": "player_age",
        "r": "no",
        "chartType": "beeswarm",
        "sort": "player_name",
        "sortDir": "asc",
    }
    return f"{SAVANT_CUSTOM_HISTORY_BASE_URL}?{urlencode(params)}"


def extract_savant_custom_history_rows(html: str) -> list[dict[str, Any]]:
    match = _DATA_RE.search(html)
    if match is None:
        raise ValueError("Could not find embedded custom leaderboard data in the Savant page")
    payload = json.loads(match.group(1))
    if not isinstance(payload, list):
        raise ValueError("Embedded Savant custom leaderboard data was not a list")
    rows = [row for row in payload if isinstance(row, dict)]
    if not rows:
        raise ValueError("Embedded Savant custom leaderboard data contained no usable rows")
    return rows


def fetch_savant_custom_history_rows(
    *,
    role: str,
    years: Sequence[int],
    user_agent: str,
    selections: Sequence[str] = DEFAULT_SAVANT_HISTORY_SELECTIONS,
    minimum: int = 1,
) -> tuple[str, list[dict[str, Any]]]:
    url = build_savant_custom_history_url(
        role=role,
        years=years,
        selections=selections,
        minimum=minimum,
    )
    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    with urlopen(request, timeout=120) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        html = response.read().decode(charset, errors="replace")
    return url, extract_savant_custom_history_rows(html)


def _normalize_export_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in row.items():
        if key == "player_name":
            normalized[PLAYER_NAME_HEADER] = value
        else:
            normalized[key] = value
    return normalized


def _ordered_headers(rows: Iterable[dict[str, Any]]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            ordered.append(key)
    if PLAYER_NAME_HEADER in seen:
        ordered = [PLAYER_NAME_HEADER] + [key for key in ordered if key != PLAYER_NAME_HEADER]
    return ordered


def _safe_year(value: Any) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return -1


def _safe_name(value: Any) -> str:
    return str(value or "").strip().casefold()


def _safe_player_id(value: Any) -> str:
    return str(value or "").strip()


def sort_export_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            _safe_name(row.get(PLAYER_NAME_HEADER) or row.get("player_name")),
            _safe_year(row.get("year")),
            _safe_player_id(row.get("player_id")),
        ),
    )


def write_savant_custom_history_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    normalized_rows = [_normalize_export_row(row) for row in rows]
    headers = _ordered_headers(normalized_rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in sort_export_rows(normalized_rows):
            writer.writerow({header: row.get(header, "") for header in headers})


def load_statcast_history_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def merge_statcast_history_rows(
    existing_rows: Sequence[dict[str, Any]],
    replacement_rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, int, str], dict[str, Any]] = {}
    for row in existing_rows:
        key = (
            _safe_player_id(row.get("player_id")),
            _safe_year(row.get("year")),
            _safe_name(row.get(PLAYER_NAME_HEADER) or row.get("player_name")),
        )
        merged[key] = dict(row)
    for row in replacement_rows:
        normalized = _normalize_export_row(dict(row))
        key = (
            _safe_player_id(normalized.get("player_id")),
            _safe_year(normalized.get("year")),
            _safe_name(normalized.get(PLAYER_NAME_HEADER)),
        )
        merged[key] = normalized
    return sort_export_rows(list(merged.values()))


def resolve_history_years(
    *,
    full_history: bool,
    current_season: int,
    start_year: int = SAVANT_CUSTOM_HISTORY_START_YEAR,
) -> list[int]:
    if full_history:
        return list(range(start_year, current_season + 1))
    return [current_season]


def refresh_bundled_statcast_history(
    *,
    data_dir: Path,
    user_agent: str,
    current_season: int,
    full_history: bool = False,
    roles: Sequence[str] = ("batter", "pitcher"),
) -> list[str]:
    years = resolve_history_years(
        full_history=full_history,
        current_season=current_season,
    )
    messages: list[str] = []
    refreshed_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    for role in roles:
        normalized_role = role.strip().lower()
        if normalized_role not in ROLE_TO_FILE_NAME:
            raise ValueError(f"Unsupported role: {role}")
        url, rows = fetch_savant_custom_history_rows(
            role=normalized_role,
            years=years,
            user_agent=user_agent,
        )
        destination = data_dir / ROLE_TO_FILE_NAME[normalized_role]
        if full_history or not destination.exists():
            final_rows = merge_statcast_history_rows([], rows)
        else:
            existing_rows = load_statcast_history_csv(destination)
            final_rows = merge_statcast_history_rows(existing_rows, rows)
        write_savant_custom_history_csv(destination, final_rows)
        messages.append(
            f"Refreshed bundled {normalized_role} Statcast history from Savant "
            f"for {min(years)}-{max(years)} into {destination.name} "
            f"({len(final_rows)} total row(s); fetched {len(rows)} row(s) at {refreshed_at})."
        )
        messages.append(f"  Source: {url}")
    return messages
