from __future__ import annotations

import sqlite3
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.player_span_metrics import PlayerSpanMetricResearcher
from mlb_history_bot.storage import initialize_database


TEST_SETTINGS = Settings.from_env(Path(__file__).resolve().parents[1])


class SchwarberLiveClient:
    def search_people(self, query: str):
        if query.strip().casefold() in {"schwarber", "kyle schwarber"}:
            return [
                {
                    "id": 656941,
                    "fullName": "Kyle Schwarber",
                    "active": True,
                    "isPlayer": True,
                    "isVerified": True,
                }
            ]
        return []


def build_connection() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    initialize_database(con)
    con.execute("CREATE TABLE lahman_people (playerid TEXT PRIMARY KEY, namefirst TEXT, namelast TEXT)")
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
    con.execute(
        "INSERT INTO lahman_people(playerid, namefirst, namelast) VALUES ('schwaky01', 'Kyle', 'Schwarber')"
    )
    con.executemany(
        """
        INSERT INTO lahman_batting(
            playerid, yearid, teamid, g, ab, r, h, c_2b, c_3b, hr, rbi, sb, cs, bb, so, hbp, sh, sf
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("schwaky01", "2017", "CHN", "129", "486", "75", "102", "20", "2", "30", "59", "4", "3", "50", "171", "3", "0", "3"),
            ("schwaky01", "2018", "CHN", "137", "510", "86", "122", "14", "1", "26", "61", "2", "1", "78", "140", "4", "0", "4"),
            ("schwaky01", "2019", "CHN", "155", "529", "94", "132", "38", "2", "38", "92", "2", "0", "86", "189", "3", "0", "5"),
        ],
    )
    con.commit()
    return con


def test_player_span_metric_home_runs_between_years_builds() -> None:
    con = build_connection()
    researcher = PlayerSpanMetricResearcher(TEST_SETTINGS)
    researcher.live_client = SchwarberLiveClient()
    snippet = researcher.build_snippet(con, "how many home runs did Schwarber hit between 2017 and 2019?")
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "player_span_metric"
    assert snippet.payload["rows"][0]["player_name"] == "Kyle Schwarber"
    assert snippet.payload["rows"][0]["home_runs"] == 94
    assert "Kyle Schwarber hit 94 home runs from 2017-2019." in snippet.summary
    con.close()


def test_player_span_metric_avg_between_years_builds() -> None:
    con = build_connection()
    researcher = PlayerSpanMetricResearcher(TEST_SETTINGS)
    researcher.live_client = SchwarberLiveClient()
    snippet = researcher.build_snippet(con, "what was Kyle Schwarber's batting average between 2017 and 2019?")
    assert snippet is not None
    assert snippet.payload["rows"][0]["hits"] == 356
    assert snippet.payload["rows"][0]["at_bats"] == 1525
    assert round(snippet.payload["rows"][0]["avg"], 3) == 0.233
    con.close()
