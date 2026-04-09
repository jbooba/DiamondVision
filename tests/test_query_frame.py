from __future__ import annotations

import sqlite3

from mlb_history_bot.metrics import MetricCatalog
from mlb_history_bot.query_frame import build_query_frame
from mlb_history_bot.storage import import_statcast_history_exports


def test_build_query_frame_for_direct_player_metric_uses_history_metric(tmp_path):
    database_path = tmp_path / "test.sqlite3"
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        pitcher_csv = tmp_path / "pitcher.csv"
        pitcher_csv.write_text(
            "last_name, first_name,player_id,year,whiff_percent,pitch_count,pa\n"
            "\"Skubal, Tarik\",669373,2025,32.5,2849,748\n",
            encoding="utf-8",
        )
        import_statcast_history_exports(connection, pitcher_csv=pitcher_csv)
        catalog = MetricCatalog(metrics=[])
        frame = build_query_frame(
            "what was Tarik Skubal's Whiff% last year?",
            current_season=2026,
            catalog=catalog,
            connection=connection,
        )
        assert frame.kind == "direct_player_metric"
        assert frame.metric_label == "Whiff Percent"
        assert frame.metric_source == "statcast_history"
        assert frame.season == 2025
    finally:
        connection.close()


def test_build_query_frame_for_birthday_leaderboard_marks_conditions_and_qualifiers(tmp_path):
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    try:
        catalog = MetricCatalog(metrics=[])
        frame = build_query_frame(
            "Which hitter has the highest OPS when playing on their birthday with a minimum of 20 PA?",
            current_season=2026,
            catalog=catalog,
            connection=connection,
        )
        assert frame.kind == "cohort_or_condition_leaderboard"
        assert frame.metric_label == "OPS"
        assert frame.ranking == "highest"
        assert "birthday" in frame.conditions
        assert frame.qualifiers.get("pa") == 20
        assert frame.layer_count >= 4
    finally:
        connection.close()
