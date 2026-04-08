from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.statcast_relationships import StatcastRelationshipResearcher
from mlb_history_bot.storage import initialize_database


def build_test_settings(tmp_path: Path) -> Settings:
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    raw_dir.mkdir(parents=True)
    processed_dir.mkdir(parents=True)
    return Settings(
        project_root=Path(__file__).resolve().parents[1],
        raw_data_dir=raw_dir,
        processed_data_dir=processed_dir,
        database_path=processed_dir / "mlb_history.sqlite3",
        sabr_docs_dir=raw_dir / "sabr",
        openai_model="gpt-5.4",
        openai_reasoning_effort="medium",
        live_season=2026,
        user_agent="test-agent",
        fielding_bible_api_base="https://example.com",
        fielding_bible_start_season=2003,
    )


def seed_statcast_events(database_path: Path) -> None:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    connection.execute(
        """
        INSERT INTO statcast_events (
            season, game_date, game_pk, at_bat_number, pitch_number,
            batter_id, batter_name, pitcher_id, pitcher_name,
            batting_team, pitching_team, home_team, away_team,
            pitch_type, pitch_name, pitch_family, event,
            is_ab, is_hit, is_home_run, is_strikeout, is_xbh,
            has_risp, count_key, runs_batted_in, horizontal_location, vertical_location, field_direction,
            release_speed, release_spin_rate, launch_speed, launch_angle, hit_distance,
            bat_speed, estimated_ba, estimated_woba, estimated_slg
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            2023,
            "2023-06-14",
            555,
            2,
            5,
            100,
            "Freddie Freeman",
            301,
            "Pitcher Curve",
            "LAN",
            "SF",
            "SF",
            "LAN",
            "CU",
            "Curveball",
            "curveball",
            "home_run",
            1,
            1,
            1,
            0,
            1,
            0,
            "2-1",
            2,
            "middle",
            "middle",
            "right field",
            80.4,
            2780.0,
            103.7,
            31.0,
            401.0,
            73.0,
            0.960,
            0.910,
            2.250,
        ),
    )
    connection.commit()
    connection.close()


class FakeLiveClient:
    def search_people(self, query: str) -> list[dict]:
        return [{"id": 100, "fullName": "Freddie Freeman", "active": True, "isPlayer": True}]


def test_statcast_relationship_uses_local_event_warehouse_for_player_pitch_query() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        settings = build_test_settings(Path(temp_dir))
        seed_statcast_events(settings.database_path)
        researcher = StatcastRelationshipResearcher(settings)
        researcher.live_client = FakeLiveClient()

        snippet = researcher.build_snippet("show me freddie freeman homeruns off curveballs in 2023")

        assert snippet is not None
        assert snippet.source == "Statcast Relationships"
        assert snippet.payload["analysis_type"] == "statcast_relationship_events"
        assert snippet.payload["rows"][0]["batter"] == "Freddie Freeman"
        assert snippet.payload["rows"][0]["pitch_name"] == "Curveball"


def test_statcast_relationship_returns_local_gap_snippet_when_parsed_query_has_no_rows() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        settings = build_test_settings(Path(temp_dir))
        connection = sqlite3.connect(settings.database_path)
        connection.row_factory = sqlite3.Row
        initialize_database(connection)
        connection.close()

        researcher = StatcastRelationshipResearcher(settings)
        researcher.live_client = FakeLiveClient()

        snippet = researcher.build_snippet("show me freddie freeman homeruns off curveballs in 2023")

        assert snippet is not None
        assert snippet.source == "Statcast Relationships"
        assert snippet.payload["analysis_type"] == "contextual_source_gap"
