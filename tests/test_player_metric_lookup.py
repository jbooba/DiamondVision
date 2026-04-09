from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.metrics import MetricCatalog
from mlb_history_bot.player_metric_lookup import (
    PlayerMetricLookupResearcher,
    parse_player_metric_query,
)


TEST_SETTINGS = Settings.from_env(Path(__file__).resolve().parents[1])


class FakeLiveClient:
    def search_people(self, query: str):
        if query.strip().casefold() != "tarik skubal":
            return []
        return [
            {
                "id": 669373,
                "fullName": "Tarik Skubal",
                "active": True,
                "isPlayer": True,
                "isVerified": True,
                "primaryPosition": {"abbreviation": "P", "code": "1"},
            }
        ]


def create_history_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE statcast_history_pitcher_seasons (
            last_name_first_name TEXT,
            player_id TEXT,
            year TEXT,
            pitch_hand TEXT,
            pa TEXT,
            p_era TEXT,
            pitch_count TEXT,
            whiff_percent TEXT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO statcast_history_pitcher_seasons(
            last_name_first_name, player_id, year, pitch_hand, pa, p_era, pitch_count, whiff_percent
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("Skubal, Tarik", "669373", "2025", "L", "748", "2.39", "2849", "32.5"),
    )
    connection.execute(
        """
        CREATE TABLE statcast_history_batter_seasons (
            last_name_first_name TEXT,
            player_id TEXT,
            year TEXT,
            pa TEXT,
            pitch_count TEXT,
            whiff_percent TEXT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO statcast_history_batter_seasons(
            last_name_first_name, player_id, year, pa, pitch_count, whiff_percent
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("Bell, Josh", "605137", "2021", "568", "2250", "24.1"),
    )
    connection.commit()


def test_parse_player_metric_query_uses_imported_history_for_whiff_percent() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    create_history_tables(connection)
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_player_metric_query(
        "what was Tarik Skubal's Whiff% last year?",
        FakeLiveClient(),
        catalog,
        2026,
        connection=connection,
    )
    assert query is not None
    assert query.season == 2025
    assert query.metric_name == "Whiff%"
    assert query.history_spec is not None
    assert query.history_spec.dynamic_value_column == "whiff_percent"
    assert query.preferred_role == "pitcher"
    connection.close()


def test_parse_player_metric_query_recognizes_bare_whiff_alias() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    create_history_tables(connection)
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_player_metric_query(
        "what was Tarik Skubal's Whiff last year?",
        FakeLiveClient(),
        catalog,
        2026,
        connection=connection,
    )
    assert query is not None
    assert query.history_spec is not None
    assert query.metric_name == "Whiff%"
    connection.close()


def test_player_metric_lookup_reads_imported_statcast_history_for_whiff_percent(tmp_path: Path) -> None:
    database_path = tmp_path / "mlb_history.sqlite3"
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    create_history_tables(connection)
    connection.close()

    settings = replace(TEST_SETTINGS, database_path=database_path, processed_data_dir=tmp_path)
    researcher = PlayerMetricLookupResearcher(settings)
    researcher.live_client = FakeLiveClient()

    snippet = researcher.build_snippet("what was Tarik Skubal's Whiff% last year?")

    assert snippet is not None
    assert snippet.source == "Statcast Custom History"
    assert "32.5 Whiff%" in snippet.summary
    assert "2025" in snippet.summary
    assert snippet.payload["rows"][0]["value"] == "32.5"
