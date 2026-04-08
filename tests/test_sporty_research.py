from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.models import EvidenceSnippet
from mlb_history_bot.search import BaseballResearchEngine
from mlb_history_bot.storage import initialize_database


TEST_SETTINGS = Settings.from_env(Path(__file__).resolve().parents[1])


class FakeReplayFinder:
    def build_snippets(self, question: str) -> list[EvidenceSnippet]:
        return [
            EvidenceSnippet(
                source="Sporty Replay",
                title="recent Freddie Freeman replay matches",
                citation="fake replay source",
                summary="Found a Freddie Freeman home run clip in a recent replay search window.",
                payload={"clip_count": 1, "clips": [{"title": "Freddie Freeman homers"}]},
            )
        ]


def build_test_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    connection.execute(
        """
        CREATE TABLE lahman_people (
            playerid TEXT PRIMARY KEY,
            namefirst TEXT,
            namelast TEXT,
            namegiven TEXT,
            birthyear TEXT,
            birthmonth TEXT,
            birthday TEXT,
            birthcity TEXT,
            birthstate TEXT,
            birthcountry TEXT,
            bats TEXT,
            throws TEXT,
            debut TEXT,
            finalgame TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE lahman_batting (
            playerid TEXT,
            yearid TEXT,
            teamid TEXT,
            g TEXT,
            h TEXT,
            hr TEXT,
            rbi TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE lahman_pitching (
            playerid TEXT,
            yearid TEXT,
            teamid TEXT,
            g TEXT,
            w TEXT,
            l TEXT,
            so TEXT,
            sv TEXT,
            ipouts TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE lahman_fielding (
            playerid TEXT,
            yearid TEXT,
            teamid TEXT,
            pos TEXT,
            g TEXT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO lahman_people (
            playerid, namefirst, namelast, namegiven, birthyear, birthmonth, birthday,
            birthcity, birthstate, birthcountry, bats, throws, debut, finalgame
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "freemfr01",
            "Freddie",
            "Freeman",
            "Frederick Charles Freeman",
            "1989",
            "9",
            "12",
            "Villa Park",
            "CA",
            "USA",
            "L",
            "R",
            "2010-09-01",
            "",
        ),
    )
    connection.execute(
        "INSERT INTO lahman_batting (playerid, yearid, teamid, g, h, hr, rbi) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("freemfr01", "2025", "LAN", "147", "175", "22", "89"),
    )
    connection.execute(
        "INSERT INTO lahman_fielding (playerid, yearid, teamid, pos, g) VALUES (?, ?, ?, ?, ?)",
        ("freemfr01", "2025", "LAN", "3", "142"),
    )
    connection.commit()
    connection.close()


def test_compile_context_prefers_replay_lane_for_undated_player_clip_prompt() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        database_path = tmp_path / "mlb_history.sqlite3"
        build_test_database(database_path)
        settings = Settings.from_env(Path(__file__).resolve().parents[1])
        settings.database_path = database_path
        settings.processed_data_dir = tmp_path

        engine = BaseballResearchEngine(settings)
        engine.sporty_replay_finder = FakeReplayFinder()
        context = engine.compile_context("show me a clip of a freddie freeman home run")

        assert context.replay_evidence
        assert context.replay_evidence[0].source == "Sporty Replay"
        assert not context.historical_evidence
