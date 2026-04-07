import sqlite3

from mlb_history_bot.config import Settings
from mlb_history_bot.historical_team_facts import (
    HistoricalTeamFactsResearcher,
    extract_team_phrase_from_manager_question,
    parse_historical_manager_query,
)


def build_test_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE lahman_teams (
            yearid TEXT,
            teamid TEXT,
            teamidretro TEXT,
            franchid TEXT,
            name TEXT,
            w TEXT,
            l TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE lahman_people (
            playerid TEXT,
            namefirst TEXT,
            namelast TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE lahman_managers (
            playerid TEXT,
            yearid TEXT,
            teamid TEXT,
            inseason TEXT,
            g TEXT,
            w TEXT,
            l TEXT,
            rank TEXT,
            plyrmgr TEXT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO lahman_teams (yearid, teamid, teamidretro, franchid, name, w, l)
        VALUES ('2023', 'NYN', 'NYN', 'NYM', 'New York Mets', '75', '87')
        """
    )
    connection.execute(
        """
        INSERT INTO lahman_people (playerid, namefirst, namelast)
        VALUES ('showabu99', 'Buck', 'Showalter')
        """
    )
    connection.execute(
        """
        INSERT INTO lahman_managers (playerid, yearid, teamid, inseason, g, w, l, rank, plyrmgr)
        VALUES ('showabu99', '2023', 'NYN', '1', '162', '75', '87', '4', 'N')
        """
    )
    connection.commit()
    return connection


def test_parse_historical_manager_query() -> None:
    query = parse_historical_manager_query("who was the Mets manager in 2023?")
    assert query is not None
    assert query.season == 2023
    assert query.team_phrase == "Mets"


def test_extract_team_phrase_from_manager_question() -> None:
    assert extract_team_phrase_from_manager_question("Who managed the New York Mets in 2023?", 2023) == "New York Mets"


def test_build_manager_snippet_from_local_lahman_tables() -> None:
    settings = Settings.from_env()
    researcher = HistoricalTeamFactsResearcher(settings)
    connection = build_test_connection()
    try:
        snippet = researcher.build_snippet(connection, "who was the Mets manager in 2023?")
    finally:
        connection.close()
    assert snippet is not None
    assert "Buck Showalter" in snippet.summary
    assert snippet.payload["analysis_type"] == "historical_manager_lookup"
    assert snippet.payload["rows"][0]["manager"] == "Buck Showalter"
