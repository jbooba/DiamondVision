import sqlite3

from mlb_history_bot.contextual_performance import (
    ContextualPerformanceResearcher,
    aggregate_count_chunk,
    build_team_relationship_snippet,
    extract_reached_counts_from_sequence,
    parse_count_split_query,
    parse_team_relationship_query,
)
from mlb_history_bot.config import Settings
from mlb_history_bot.storage import (
    initialize_database,
    upsert_retrosheet_player_count_splits,
    upsert_retrosheet_player_reached_count_splits,
)


def test_aggregate_count_chunk() -> None:
    import pandas as pd

    frame = pd.DataFrame(
        [
            {
                "batter": "slug001",
                "balls": "3",
                "strikes": "0",
                "pa": "1",
                "ab": "1",
                "single": "1",
                "double": "0",
                "triple": "0",
                "hr": "0",
                "walk": "0",
                "iw": "0",
                "hbp": "0",
                "sf": "0",
                "k": "0",
                "rbi": "0",
                "date": "20240401",
                "gametype": "regular",
            },
            {
                "batter": "slug001",
                "balls": "3",
                "strikes": "0",
                "pa": "1",
                "ab": "1",
                "single": "0",
                "double": "0",
                "triple": "0",
                "hr": "1",
                "walk": "0",
                "iw": "0",
                "hbp": "0",
                "sf": "0",
                "k": "0",
                "rbi": "2",
                "date": "20250401",
                "gametype": "regular",
            },
        ]
    )
    totals: dict[tuple[str, str], dict[str, int | str]] = {}
    aggregate_count_chunk(frame, totals)
    row = totals[("slug001", "3-0")]
    assert row["hits"] == 2
    assert row["home_runs"] == 1
    assert row["first_season"] == 2024
    assert row["last_season"] == 2025


def test_count_split_snippet_from_synced_table() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    connection.execute(
        """
        CREATE TABLE lahman_people (
            retroid TEXT,
            namefirst TEXT,
            namelast TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE retrosheet_allplayers (
            id TEXT,
            first TEXT,
            last TEXT
        )
        """
    )
    connection.execute("INSERT INTO lahman_people (retroid, namefirst, namelast) VALUES ('slug001', 'Slugger', 'One')")
    upsert_retrosheet_player_count_splits(
        connection,
        [
            {
                "player_id": "slug001",
                "count_key": "3-0",
                "plate_appearances": 40,
                "at_bats": 30,
                "hits": 12,
                "doubles": 0,
                "triples": 0,
                "home_runs": 2,
                "walks": 10,
                "hit_by_pitch": 0,
                "sacrifice_flies": 0,
                "strikeouts": 0,
                "runs_batted_in": 5,
                "first_season": 2024,
                "last_season": 2025,
            }
        ],
    )
    researcher = ContextualPerformanceResearcher(Settings.from_env())
    snippet = researcher.build_snippet(connection, "who has the lowest batting average on 3-0 counts?")
    connection.close()
    assert snippet is not None
    assert snippet.payload["leaders"][0]["player_name"] == "Slugger One"


def test_reached_count_sequence_parser() -> None:
    assert extract_reached_counts_from_sequence("BBCFX") == {"1-0", "2-0", "2-1", "2-2"}
    assert extract_reached_counts_from_sequence("SSBX") == {"0-1", "0-2", "1-2"}
    assert extract_reached_counts_from_sequence("BBBB") == {"1-0", "2-0", "3-0"}


def test_reached_count_snippet_from_synced_table() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    connection.execute(
        """
        CREATE TABLE lahman_people (
            retroid TEXT,
            namefirst TEXT,
            namelast TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE retrosheet_allplayers (
            id TEXT,
            first TEXT,
            last TEXT
        )
        """
    )
    connection.execute("INSERT INTO lahman_people (retroid, namefirst, namelast) VALUES ('slug001', 'Slugger', 'One')")
    upsert_retrosheet_player_reached_count_splits(
        connection,
        [
            {
                "player_id": "slug001",
                "count_key": "0-2",
                "plate_appearances": 40,
                "at_bats": 30,
                "hits": 12,
                "doubles": 1,
                "triples": 0,
                "home_runs": 2,
                "walks": 8,
                "hit_by_pitch": 0,
                "sacrifice_flies": 0,
                "strikeouts": 10,
                "runs_batted_in": 7,
                "first_season": 2021,
                "last_season": 2025,
            }
        ],
    )
    researcher = ContextualPerformanceResearcher(Settings.from_env())
    snippet = researcher.build_snippet(connection, "who has the highest batting average after 0-2 counts?")
    connection.close()
    assert snippet is not None
    assert snippet.payload["relation"] == "after"
    assert snippet.payload["leaders"][0]["player_name"] == "Slugger One"


def test_parse_count_split_query_for_walks() -> None:
    query = parse_count_split_query("who has the most walks after 3-0 counts?")
    assert query is not None
    assert query.metric_key == "walks"
    assert query.relation == "after"


def test_parse_count_split_query_for_offensive_summary_defaults_to_ops() -> None:
    query = parse_count_split_query("who has performed best offensively after 0-2 counts?")
    assert query is not None
    assert query.metric_key == "ops"
    assert query.relation == "after"


def test_invalid_count_returns_validation_snippet() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    researcher = ContextualPerformanceResearcher(Settings.from_env())
    snippet = researcher.build_snippet(connection, "who has the lowest batting average on 0-3 counts?")
    connection.close()
    assert snippet is not None
    assert snippet.payload["analysis_type"] == "contextual_invalid_count"
    assert snippet.payload["count_key"] == "0-3"


def test_team_relationship_snippet() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
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
        CREATE TABLE lahman_teams (
            teamidretro TEXT,
            yearid TEXT,
            name TEXT
        )
        """
    )
    connection.execute("INSERT INTO lahman_people VALUES ('player1','Slugger','One')")
    connection.executemany(
        "INSERT INTO lahman_teams VALUES (?,?,?)",
        [
            ("NYN", "2025", "New York Mets"),
            ("ATL", "2025", "Atlanta Braves"),
        ],
    )
    connection.execute(
        """
        INSERT INTO retrosheet_player_opponent_contexts (
            player_id, opponent, context_key, plate_appearances, at_bats, hits,
            doubles, triples, home_runs, walks, intentional_walks, hit_by_pitch,
            sacrifice_flies, strikeouts, runs_batted_in, first_season, last_season
        ) VALUES (
            'player1', 'NYN', 'former_team', 4, 4, 2,
            1, 0, 1, 0, 0, 0,
            0, 0, 3, 2021, 2021
        )
        """
    )
    query = parse_team_relationship_query("Who has the most home runs against their former team?")
    snippet = build_team_relationship_snippet(connection, query)
    connection.close()
    assert snippet is not None
    assert snippet.payload["leaders"][0]["player_name"] == "Slugger One"


def test_team_relationship_query_for_batting_average() -> None:
    query = parse_team_relationship_query("Who has the highest batting average against their former team?")
    assert query is not None
    assert query.metric_key == "ba"
    assert query.sample_basis == "at_bats"


def test_team_relationship_query_for_rbi() -> None:
    query = parse_team_relationship_query("Who has the most RBI against their former team?")
    assert query is not None
    assert query.metric_key == "runs_batted_in"
    assert query.sort_desc is True


def test_team_relationship_query_supports_aggregate_scope() -> None:
    query = parse_team_relationship_query(
        "Who has the lowest batting average aggregated across all former teams?"
    )
    assert query is not None
    assert query.metric_key == "ba"
    assert query.aggregate_scope == "player"
