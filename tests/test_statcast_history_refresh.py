from __future__ import annotations

import csv
import json
from pathlib import Path

from mlb_history_bot.statcast_history_refresh import (
    PLAYER_NAME_HEADER,
    build_savant_custom_history_url,
    extract_savant_custom_history_rows,
    merge_statcast_history_rows,
    refresh_bundled_statcast_history,
    write_savant_custom_history_csv,
)


def test_build_savant_custom_history_url_uses_requested_role_and_years() -> None:
    url = build_savant_custom_history_url(role="pitcher", years=[2026, 2025, 2024])

    assert "type=pitcher" in url
    assert "year=2026%2C2025%2C2024" in url
    assert "meatball_percent" in url
    assert "linedrives" in url


def test_extract_savant_custom_history_rows_reads_embedded_json() -> None:
    payload = [{"player_name": "Bell, Josh", "year": 2021, "exit_velocity_avg": 92.5}]
    html = f"<html><script>var data = {json.dumps(payload)};</script></html>"

    rows = extract_savant_custom_history_rows(html)

    assert rows == payload


def test_merge_statcast_history_rows_replaces_matching_player_season() -> None:
    existing = [
        {PLAYER_NAME_HEADER: "Bell, Josh", "player_id": "605137", "year": "2025", "exit_velocity_avg": "90.0"},
        {PLAYER_NAME_HEADER: "Bell, Josh", "player_id": "605137", "year": "2024", "exit_velocity_avg": "91.0"},
    ]
    replacement = [
        {"player_name": "Bell, Josh", "player_id": "605137", "year": "2025", "exit_velocity_avg": "92.0"},
    ]

    merged = merge_statcast_history_rows(existing, replacement)

    assert len(merged) == 2
    assert any(
        row[PLAYER_NAME_HEADER] == "Bell, Josh"
        and str(row["year"]) == "2025"
        and str(row["exit_velocity_avg"]) == "92.0"
        for row in merged
    )


def test_write_savant_custom_history_csv_renames_player_name_header(tmp_path: Path) -> None:
    destination = tmp_path / "history.csv"
    rows = [{"player_name": "Skubal, Tarik", "player_id": "669373", "year": 2025, "whiff_percent": 32.5}]

    write_savant_custom_history_csv(destination, rows)

    with destination.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        values = next(reader)

    assert header[0] == PLAYER_NAME_HEADER
    assert values[0] == "Skubal, Tarik"


def test_refresh_bundled_statcast_history_merges_current_season(monkeypatch, tmp_path: Path) -> None:
    batter_path = tmp_path / "Batter_Stats_Statcast_History.csv"
    write_savant_custom_history_csv(
        batter_path,
        [
            {"player_name": "Bell, Josh", "player_id": "605137", "year": 2025, "exit_velocity_avg": 90.0},
            {"player_name": "Bell, Josh", "player_id": "605137", "year": 2024, "exit_velocity_avg": 91.0},
        ],
    )

    def fake_fetch(*, role: str, years: list[int], user_agent: str, selections=(), minimum: int = 1):
        assert role == "batter"
        assert years == [2026]
        return "https://example.test/batter", [
            {"player_name": "Bell, Josh", "player_id": "605137", "year": 2026, "exit_velocity_avg": 92.5},
        ]

    monkeypatch.setattr(
        "mlb_history_bot.statcast_history_refresh.fetch_savant_custom_history_rows",
        fake_fetch,
    )

    messages = refresh_bundled_statcast_history(
        data_dir=tmp_path,
        user_agent="test-agent",
        current_season=2026,
        full_history=False,
        roles=("batter",),
    )

    assert messages
    with batter_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    assert len(rows) == 3
    assert any(row[PLAYER_NAME_HEADER] == "Bell, Josh" and row["year"] == "2026" for row in rows)
