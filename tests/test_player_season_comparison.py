import sqlite3
from pathlib import Path

from mlb_history_bot.metrics import MetricCatalog
from mlb_history_bot.player_season_comparison import (
    build_historical_player_snapshot,
    parse_player_season_comparison_query,
)


class FakeLiveClient:
    def search_people(self, query: str):
        normalized = query.strip().casefold()
        if normalized == "aaron judge":
            return [
                {
                    "id": 592450,
                    "fullName": "Aaron Judge",
                    "active": True,
                    "isPlayer": True,
                    "isVerified": True,
                }
            ]
        if normalized == "pete alonso":
            return [
                {
                    "id": 624413,
                    "fullName": "Pete Alonso",
                    "active": True,
                    "isPlayer": True,
                    "isVerified": True,
                }
            ]
        if normalized == "cal raleigh":
            return [
                {
                    "id": 663728,
                    "fullName": "Cal Raleigh",
                    "active": True,
                    "isPlayer": True,
                    "isVerified": True,
                }
            ]
        return []


def test_parse_player_season_comparison_query() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE lahman_people (
            playerid TEXT,
            namefirst TEXT,
            namelast TEXT,
            namegiven TEXT,
            debut TEXT,
            finalgame TEXT
        )
        """
    )
    connection.execute(
        "INSERT INTO lahman_people (playerid, namefirst, namelast, namegiven, debut, finalgame) VALUES (?, ?, ?, ?, ?, ?)",
        ("alonspe01", "Pete", "Alonso", "Pete Alonso", "2019-03-28", ""),
    )
    connection.execute(
        "INSERT INTO lahman_people (playerid, namefirst, namelast, namegiven, debut, finalgame) VALUES (?, ?, ?, ?, ?, ?)",
        ("judgeaa01", "Aaron", "Judge", "Aaron Judge", "2016-08-13", ""),
    )
    connection.execute(
        "INSERT INTO lahman_people (playerid, namefirst, namelast, namegiven, debut, finalgame) VALUES (?, ?, ?, ?, ?, ?)",
        ("raleica01", "Cal", "Raleigh", "Cal Raleigh", "2021-07-11", ""),
    )
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_player_season_comparison_query(
        connection,
        "compare Pete Alonso 2022 to Cal Raleigh 2025",
        FakeLiveClient(),
        catalog,
        2026,
    )
    connection.close()
    assert query is not None
    assert query.left.player_name == "Pete Alonso"
    assert query.left.season == 2022
    assert query.right.player_name == "Cal Raleigh"
    assert query.right.season == 2025


def test_parse_player_season_comparison_query_strips_window_phrase_and_uses_pronoun_fallback() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE lahman_people (
            playerid TEXT,
            namefirst TEXT,
            namelast TEXT,
            namegiven TEXT,
            debut TEXT,
            finalgame TEXT
        )
        """
    )
    connection.execute(
        "INSERT INTO lahman_people (playerid, namefirst, namelast, namegiven, debut, finalgame) VALUES (?, ?, ?, ?, ?, ?)",
        ("judgeaa01", "Aaron", "Judge", "Aaron Judge", "2016-08-13", ""),
    )
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_player_season_comparison_query(
        connection,
        "compare Aaron Judge's first 10 games of 2026 to his first 10 games of 2025",
        FakeLiveClient(),
        catalog,
        2026,
    )
    connection.close()
    assert query is not None
    assert query.left.player_name == "Aaron Judge"
    assert query.left.player_query == "Aaron Judge"
    assert query.left.season == 2026
    assert query.right.player_name == "Aaron Judge"
    assert query.right.player_query == "Aaron Judge"
    assert query.right.season == 2025


def test_build_historical_player_snapshot() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE lahman_people (
            playerid TEXT,
            namefirst TEXT,
            namelast TEXT,
            namegiven TEXT,
            debut TEXT,
            finalgame TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE lahman_batting (
            playerid TEXT,
            yearid INTEGER,
            g TEXT,
            ab TEXT,
            h TEXT,
            c_2b TEXT,
            c_3b TEXT,
            hr TEXT,
            rbi TEXT,
            sb TEXT,
            bb TEXT,
            so TEXT,
            hbp TEXT,
            sh TEXT,
            sf TEXT,
            teamid TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE lahman_pitching (
            playerid TEXT,
            yearid INTEGER,
            g TEXT,
            w TEXT,
            l TEXT,
            sv TEXT,
            ipouts TEXT,
            h TEXT,
            er TEXT,
            hr TEXT,
            bb TEXT,
            so TEXT,
            teamid TEXT
        )
        """
    )
    connection.execute(
        "INSERT INTO lahman_people (playerid, namefirst, namelast, namegiven, debut, finalgame) VALUES (?, ?, ?, ?, ?, ?)",
        ("alonspe01", "Pete", "Alonso", "Pete Alonso", "2019-03-28", ""),
    )
    connection.execute(
        """
        INSERT INTO lahman_batting (
            playerid, yearid, g, ab, h, c_2b, c_3b, hr, rbi, sb, bb, so, hbp, sh, sf, teamid
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("alonspe01", 2022, "160", "597", "162", "27", "0", "40", "131", "5", "67", "128", "4", "0", "17", "NYN"),
    )
    reference = type(
        "Reference",
        (),
        {
            "player_name": "Pete Alonso",
            "season": 2022,
            "lahman_player_id": "alonspe01",
        },
    )()
    snapshot = build_historical_player_snapshot(connection, reference)
    connection.close()
    assert snapshot is not None
    assert snapshot.player_name == "Pete Alonso"
    assert snapshot.team == "NYN"
    assert snapshot.home_runs == 40
    assert snapshot.runs_batted_in == 131
    assert snapshot.ops is not None and snapshot.ops > 0.8
