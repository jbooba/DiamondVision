from __future__ import annotations

import sqlite3
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.player_season_analysis import PlayerSeasonAnalysisResearcher


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


class FakeLiveClient:
    def search_people(self, query: str):
        if query.strip().casefold() == "joe sewell":
            return [{"fullName": "Joe Sewell", "active": False, "isPlayer": True, "isVerified": True}]
        return []

    def player_season_snapshot(self, player_name: str, season: int):
        raise AssertionError("historical player season query should use the local Lahman snapshot")


def build_connection() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        """
        CREATE TABLE lahman_people (
            playerid TEXT PRIMARY KEY,
            namefirst TEXT,
            namelast TEXT,
            namegiven TEXT,
            birthyear TEXT,
            debut TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE lahman_teams (
            yearid TEXT,
            teamid TEXT,
            name TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE lahman_batting (
            playerid TEXT,
            yearid TEXT,
            teamid TEXT,
            g TEXT,
            ab TEXT,
            r TEXT,
            h TEXT,
            c_2b TEXT,
            c_3b TEXT,
            hr TEXT,
            rbi TEXT,
            sb TEXT,
            cs TEXT,
            bb TEXT,
            so TEXT,
            hbp TEXT,
            sh TEXT,
            sf TEXT
        )
        """
    )
    con.executemany(
        "INSERT INTO lahman_people(playerid, namefirst, namelast, namegiven, birthyear, debut) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("seweljo01", "Joe", "Sewell", "Joseph Wheeler Sewell", "1898", "1920-04-14"),
        ],
    )
    con.executemany(
        "INSERT INTO lahman_teams(yearid, teamid, name) VALUES (?, ?, ?)",
        [
            ("1928", "NYA", "New York Yankees"),
            ("1929", "NYA", "New York Yankees"),
        ],
    )
    con.executemany(
        """
        INSERT INTO lahman_batting(
            playerid, yearid, teamid, g, ab, r, h, c_2b, c_3b, hr, rbi, sb, cs, bb, so, hbp, sh, sf
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("seweljo01", "1928", "NYA", "151", "553", "86", "170", "24", "8", "6", "63", "4", "0", "63", "9", "2", "33", "0"),
            ("seweljo01", "1929", "NYA", "152", "578", "90", "182", "38", "3", "7", "73", "7", "2", "48", "4", "5", "41", "0"),
        ],
    )
    con.commit()
    return con


def test_historical_player_season_analysis_returns_full_stat_line(tmp_path: Path) -> None:
    settings = build_test_settings(tmp_path)
    researcher = PlayerSeasonAnalysisResearcher(settings)
    researcher.live_client = FakeLiveClient()
    con = build_connection()
    try:
        snippet = researcher.build_snippet("what were Joe Sewell's batting stats for the 1929 season", con)
        assert snippet is not None
        assert snippet.payload["mode"] == "historical"
        assert snippet.payload["team"] == "New York Yankees"
        assert "AB 578" in snippet.summary
        assert "H 182" in snippet.summary
        assert "2B 38" in snippet.summary
        assert "3B 3" in snippet.summary
        assert "HBP 5" in snippet.summary
        row = snippet.payload["rows"][0]
        assert row["ab"] == 578
        assert row["hits"] == 182
        assert row["doubles"] == 38
        assert row["triples"] == 3
        assert row["hr"] == 7
        assert row["rbi"] == 73
        assert row["bb"] == 48
        assert row["so"] == 4
        assert row["hbp"] == 5
        assert row["sh"] == 41
        assert row["tb"] == 247
    finally:
        con.close()
