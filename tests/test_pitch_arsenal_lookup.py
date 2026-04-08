from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.models import EvidenceSnippet
from mlb_history_bot.pitch_arsenal_leaderboards import PitchArsenalLeaderboardResearcher
from mlb_history_bot.search import BaseballResearchEngine
from mlb_history_bot.storage import initialize_database


TEST_SETTINGS = Settings.from_env(Path(__file__).resolve().parents[1])


class FakeReplayFinder:
    def build_snippets(self, question: str) -> list[EvidenceSnippet]:
        return []

    def build_recent_player_snippets(self, question: str) -> list[EvidenceSnippet]:
        return [
            EvidenceSnippet(
                source="Sporty Replay",
                title="recent Sandy Alcantara replay matches",
                citation="fake replay source",
                summary="Found recent Sandy Alcantara highlights.",
                payload={"clip_count": 1, "clips": [{"title": "Sandy Alcantara strikeout"}]},
            )
        ]


def build_test_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    connection.executemany(
        """
        INSERT INTO statcast_pitch_type_games (
            season, game_date, game_pk, pitcher_id, pitcher_name, team, team_name,
            opponent, opponent_name, pitch_type, pitch_name, pitch_family, pitches,
            avg_release_speed, max_release_speed, avg_release_spin_rate, max_release_spin_rate,
            called_strikes, swinging_strikes, whiffs, strikeouts, walks, hits_allowed,
            extra_base_hits_allowed, home_runs_allowed, batted_ball_events, xba_numerator,
            xwoba_numerator, xwoba_denom, xslg_numerator, launch_speed_sum, launch_speed_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                2025, "2025-07-01", 1, 592826, "Sandy Alcantara", "MIA", "Miami Marlins",
                "ATL", "Atlanta Braves", "FF", "4-Seam Fastball", "fastball", 600,
                97.4, 99.1, 2410.0, 2520.0, 100, 70, 65, 80, 20, 40,
                15, 8, 120, 0.0, 0.0, 0.0, 0.0, 11000.0, 120
            ),
            (
                2025, "2025-07-01", 1, 592826, "Sandy Alcantara", "MIA", "Miami Marlins",
                "ATL", "Atlanta Braves", "CH", "Changeup", "changeup", 220,
                90.1, 91.8, 1860.0, 1930.0, 40, 35, 32, 28, 10, 15,
                4, 1, 45, 0.0, 0.0, 0.0, 0.0, 3800.0, 45
            ),
            (
                2026, "2026-04-05", 2, 592826, "Sandy Alcantara", "MIA", "Miami Marlins",
                "NYM", "New York Mets", "FF", "4-Seam Fastball", "fastball", 310,
                97.9, 99.6, 2440.0, 2550.0, 55, 40, 37, 41, 8, 18,
                6, 2, 62, 0.0, 0.0, 0.0, 0.0, 5700.0, 62
            ),
            (
                2026, "2026-04-05", 2, 592826, "Sandy Alcantara", "MIA", "Miami Marlins",
                "NYM", "New York Mets", "SI", "Sinker", "fastball", 180,
                96.3, 98.1, 2290.0, 2390.0, 24, 18, 16, 14, 6, 10,
                3, 1, 38, 0.0, 0.0, 0.0, 0.0, 3400.0, 38
            ),
            (
                2026, "2026-04-05", 2, 592826, "Sandy Alcantara", "MIA", "Miami Marlins",
                "NYM", "New York Mets", "CH", "Changeup", "changeup", 140,
                90.8, 92.2, 1875.0, 1945.0, 22, 26, 24, 19, 5, 11,
                3, 1, 29, 0.0, 0.0, 0.0, 0.0, 2500.0, 29
            ),
            (
                2026, "2026-04-05", 2, 592826, "Sandy Alcantara", "MIA", "Miami Marlins",
                "NYM", "New York Mets", "SL", "Slider", "slider", 95,
                88.0, 89.7, 2510.0, 2620.0, 18, 20, 19, 16, 2, 8,
                2, 0, 20, 0.0, 0.0, 0.0, 0.0, 1700.0, 20
            ),
        ],
    )
    connection.commit()
    connection.close()


def test_pitch_arsenal_lookup_returns_latest_local_repertoire() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        database_path = tmp_path / "mlb_history.sqlite3"
        build_test_database(database_path)

        settings = Settings.from_env(Path(__file__).resolve().parents[1])
        settings.database_path = database_path
        settings.processed_data_dir = tmp_path

        researcher = PitchArsenalLeaderboardResearcher(settings)
        snippet = researcher.build_snippet("what pitches does Sandy Alcantara throw?")

        assert snippet is not None
        assert snippet.payload["analysis_type"] == "pitch_arsenal_lookup"
        assert snippet.payload["season"] == 2026
        assert snippet.payload["repertoire"][0]["pitch_label"] == "4-Seam Fastball"
        assert "Sandy Alcantara's tracked 2026 pitch mix" in snippet.summary
        assert "Slider" in snippet.summary


def test_compile_context_keeps_arsenal_answer_and_adds_recent_replay_supplement() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        database_path = tmp_path / "mlb_history.sqlite3"
        build_test_database(database_path)

        settings = Settings.from_env(Path(__file__).resolve().parents[1])
        settings.database_path = database_path
        settings.processed_data_dir = tmp_path

        engine = BaseballResearchEngine(settings)
        engine.sporty_replay_finder = FakeReplayFinder()

        context = engine.compile_context("what pitches does Sandy Alcantara throw?")

        assert context.historical_evidence
        assert context.historical_evidence[0].payload["analysis_type"] == "pitch_arsenal_lookup"
        assert context.replay_evidence
        assert context.replay_evidence[0].source == "Sporty Replay"
