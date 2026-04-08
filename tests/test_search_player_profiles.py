from __future__ import annotations

import sqlite3
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.search import BaseballResearchEngine
from mlb_history_bot.storage import initialize_database


TEST_SETTINGS = Settings.from_env(Path(__file__).resolve().parents[1])


class FakeLiveClient:
    def search_people(self, query: str):
        if query.strip().casefold() == "pete alonso":
            return [
                {
                    "id": 624413,
                    "fullName": "Pete Alonso",
                    "active": True,
                    "isPlayer": True,
                    "isVerified": True,
                }
            ]
        return []

    def person_details(self, player_id: int):
        if player_id != 624413:
            return None
        return {
            "id": 624413,
            "fullName": "Pete Alonso",
            "birthDate": "1994-12-07",
            "birthCity": "Tampa",
            "birthStateProvince": "FL",
            "birthCountry": "USA",
            "currentTeam": {"id": 110, "name": "Baltimore Orioles"},
            "primaryPosition": {"name": "First Base"},
            "batSide": {"code": "R"},
            "pitchHand": {"code": "R"},
            "active": True,
        }


class EmptyLiveClient:
    def search_people(self, query: str):
        return []

    def person_details(self, player_id: int):
        return None


def build_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
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
            "alonspe01",
            "Pete",
            "Alonso",
            "Peter Morgan Alonso",
            "1994",
            "12",
            "7",
            "Tampa",
            "FL",
            "USA",
            "R",
            "R",
            "2019-03-28",
            "",
        ),
    )
    connection.execute(
        "INSERT INTO lahman_batting (playerid, yearid, teamid, g, h, hr, rbi) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("alonspe01", "2025", "NYN", "162", "172", "34", "88"),
    )
    connection.execute(
        "INSERT INTO lahman_fielding (playerid, yearid, teamid, pos, g) VALUES (?, ?, ?, ?, ?)",
        ("alonspe01", "2025", "NYN", "3", "150"),
    )
    connection.commit()
    return connection


def test_player_summary_prefers_live_birth_date_and_current_team() -> None:
    engine = BaseballResearchEngine(TEST_SETTINGS)
    engine.live_client = FakeLiveClient()
    connection = build_connection()
    try:
        snippet = engine._player_summary(connection, "Pete Alonso")
    finally:
        connection.close()
    assert snippet is not None
    assert "Born on December 7, 1994 in Tampa, FL, USA." in snippet.summary
    assert "currently with Baltimore Orioles" in snippet.summary
    assert snippet.payload["birth_date"] == "1994-12-07"
    assert snippet.payload["current_team"] == "Baltimore Orioles"


def test_player_summary_uses_exact_lahman_birth_date_without_live_match() -> None:
    engine = BaseballResearchEngine(TEST_SETTINGS)
    engine.live_client = EmptyLiveClient()
    connection = build_connection()
    try:
        snippet = engine._player_summary(connection, "Pete Alonso")
    finally:
        connection.close()
    assert snippet is not None
    assert "Born on December 7, 1994 in Tampa, FL, USA." in snippet.summary
    assert "currently with Baltimore Orioles" not in snippet.summary
    assert snippet.payload["birth_date"] == "1994-12-07"
