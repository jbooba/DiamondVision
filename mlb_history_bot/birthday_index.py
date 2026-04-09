from __future__ import annotations

import json
import re
from datetime import date
from typing import Any

import requests


SAVANT_BIRTHDAY_INDEX_URL = "https://baseballsavant.mlb.com/birthday-index"
_BIRTHDAY_DATA_PATTERN = re.compile(r"const\s+birthdayData\s*=\s*(\[[\s\S]*?\]);")

_BATTER_METRIC_COLUMNS: dict[str, str] = {
    "avg": "birthday_BA",
    "ops": "birthday_OPS",
    "woba": "birthday_wOBA",
    "hits": "birthday_hits",
    "home_runs": "birthday_hit_hr",
    "walks": "birthday_walk",
    "strikeouts": "birthday_strikeout",
}


def is_supported_birthday_index_query(query, current_season: int) -> bool:
    if query.condition.key != "birthday":
        return False
    if query.metric.role not in {"hitter", "player"}:
        return False
    if query.condition_value or query.breakdown_all_values:
        return False
    if query.minimum_basis not in {None, "games"}:
        return False
    if query.start_season > 1969:
        return False
    if query.end_season < current_season - 1:
        return False
    return resolve_birthday_index_metric_column(query.metric.key, query.metric.label) is not None


def resolve_birthday_index_metric_column(metric_key: str, metric_label: str) -> str | None:
    lowered_key = (metric_key or "").strip().lower()
    if lowered_key in _BATTER_METRIC_COLUMNS:
        return _BATTER_METRIC_COLUMNS[lowered_key]
    lowered_label = (metric_label or "").strip().lower()
    label_hints = (
        ("ops", "birthday_OPS"),
        ("woba", "birthday_wOBA"),
        ("batting average", "birthday_BA"),
        ("avg", "birthday_BA"),
        ("home run", "birthday_hit_hr"),
        ("hits", "birthday_hits"),
        ("walk", "birthday_walk"),
        ("strikeout", "birthday_strikeout"),
    )
    for hint, column in label_hints:
        if hint in lowered_label:
            return column
    return None


def fetch_birthday_index_rows(query, *, current_season: int, timeout_seconds: int = 20) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not is_supported_birthday_index_query(query, current_season):
        return [], {"total_row_count": 0}

    minimum_games = max(int(query.minimum_value or 1), 1)
    response = requests.get(
        SAVANT_BIRTHDAY_INDEX_URL,
        params={
            "type": "batter",
            "showInactives": "true",
            "minGames": str(minimum_games),
            "sortColumn": "birthday_index",
            "sortDirection": "desc",
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    match = _BIRTHDAY_DATA_PATTERN.search(response.text)
    if not match:
        return [], {"total_row_count": 0}
    data = json.loads(match.group(1))
    metric_column = resolve_birthday_index_metric_column(query.metric.key, query.metric.label)
    if metric_column is None:
        return [], {"total_row_count": 0}

    candidates: list[dict[str, Any]] = []
    for item in data:
        metric_value = item.get(metric_column)
        if metric_value is None:
            continue
        birthday_games = safe_int(item.get("birthday_games")) or 0
        birthday_pa = safe_int(item.get("birthday_pa")) or 0
        hits = safe_int(item.get("birthday_hits")) or 0
        home_runs = safe_int(item.get("birthday_hit_hr")) or 0
        walks = safe_int(item.get("birthday_walk")) or 0
        strikeouts = safe_int(item.get("birthday_strikeout")) or 0
        at_bats = infer_birthday_at_bats(item, hits, walks, strikeouts)
        avg = safe_float(item.get("birthday_BA"))
        ops = safe_float(item.get("birthday_OPS"))
        woba = safe_float(item.get("birthday_wOBA"))
        obp, slg = infer_obp_slg(avg, ops)
        birthday_date = str(item.get("actual_birthday") or "").strip()
        first_season = int(birthday_date[:4]) if len(birthday_date) >= 4 and birthday_date[:4].isdigit() else 1969
        last_season = current_season
        candidates.append(
            {
                "player_name": str(item.get("player_name") or item.get("name") or "").strip(),
                "metric_value": float(metric_value),
                "sample_size": float(birthday_games),
                "games": birthday_games,
                "condition_games": birthday_games,
                "plate_appearances": birthday_pa,
                "at_bats": at_bats,
                "hits": hits,
                "home_runs": home_runs,
                "walks": walks,
                "strikeouts": strikeouts,
                "avg": avg,
                "obp": obp,
                "slg": slg,
                "ops": ops,
                "woba": woba,
                "runs_batted_in": None,
                "first_season": first_season,
                "last_season": last_season,
            }
        )

    total_row_count = len(candidates)
    max_condition_games = max((safe_int(row["condition_games"]) or 0) for row in candidates) if candidates else 0
    max_plate_appearances = max((safe_int(row["plate_appearances"]) or 0) for row in candidates) if candidates else 0
    max_at_bats = max((safe_int(row["at_bats"]) or 0) for row in candidates) if candidates else 0
    max_basis_value = float(max_condition_games) if query.minimum_basis == "games" else 0.0
    if query.minimum_value is not None and query.minimum_basis == "games":
        candidates = [
            row for row in candidates
            if float(row.get("games") or 0.0) >= float(query.minimum_value)
        ]
    candidates.sort(
        key=lambda row: (
            -float(row["metric_value"]) if query.sort_desc else float(row["metric_value"]),
            -(row.get("sample_size") or 0.0),
            int(row.get("last_season") or 0),
            str(row.get("player_name") or ""),
        )
    )
    for index, row in enumerate(candidates, start=1):
        row["rank"] = index
    metadata = {
        "total_row_count": total_row_count,
        "max_condition_games": max_condition_games,
        "max_plate_appearances": max_plate_appearances,
        "max_at_bats": max_at_bats,
        "max_basis_value": max_basis_value,
        "source_type": "birthday_index",
        "leaderboard_scope_note": (
            "This leaderboard was loaded from Baseball Savant's Sarah Langs Birthday Index and sorted locally. "
            "The returned rows cover qualified birthday-game hitter results dating back to 1969, limited to players "
            "whose birthdays fall during the baseball season."
        ),
    }
    return candidates, metadata


def infer_birthday_at_bats(item: dict[str, Any], hits: int, walks: int, strikeouts: int) -> int:
    at_bats = safe_int(item.get("birthday_ab"))
    if at_bats is not None and at_bats >= 0:
        return at_bats
    plate_appearances = safe_int(item.get("birthday_pa")) or 0
    if plate_appearances and hits is not None and walks is not None:
        guessed = plate_appearances - walks
        return max(guessed, hits, strikeouts)
    return max(hits, strikeouts)


def infer_obp_slg(avg: float | None, ops: float | None) -> tuple[float | None, float | None]:
    if avg is None and ops is None:
        return None, None
    if avg is not None and ops is not None:
        obp = avg
        slg = ops - obp
        if slg < 0:
            slg = None
        return obp, slg
    return None, None


def safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
