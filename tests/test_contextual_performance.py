import sqlite3

from mlb_history_bot.contextual_performance import (
    ContextualPerformanceResearcher,
    aggregate_count_chunk,
    aggregate_opponent_pitcher_chunk,
    build_birthday_matchup_snippet,
    build_team_relationship_snippet,
    extract_reached_counts_from_sequence,
    parse_birthday_matchup_query,
    parse_opponent_pitcher_cohort_query,
    parse_count_split_query,
    parse_team_relationship_query,
)
from mlb_history_bot.config import Settings
from mlb_history_bot.storage import (
    initialize_database,
    upsert_retrosheet_player_count_splits,
    upsert_retrosheet_player_opponent_pitchers,
    upsert_retrosheet_player_opponent_pitcher_cohorts,
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


def test_parse_opponent_pitcher_cohort_query_for_cy_young_winners() -> None:
    query = parse_opponent_pitcher_cohort_query("which hitter has the best OPS against Cy Young Award Winners?")
    assert query is not None
    assert query.cohort_kind == "award"
    assert query.cohort_value == "cy_young"
    assert query.metric_key == "ops"


def test_parse_opponent_pitcher_cohort_query_for_left_handed_pitchers() -> None:
    query = parse_opponent_pitcher_cohort_query("which hitter has the best OPS against left-handed pitchers?")
    assert query is not None
    assert query.cohort_kind == "throw_handedness"
    assert query.metric_key == "ops"
    assert query.cohort_filter is not None
    assert query.cohort_filter.kind == "throw_handedness"


def test_parse_opponent_pitcher_cohort_query_respects_explicit_pa_minimum() -> None:
    query = parse_opponent_pitcher_cohort_query(
        "Which hitter has the highest OPS against pitchers who have won the Cy Young Award with at least 100 PA?"
    )
    assert query is not None
    assert query.sample_basis == "plate_appearances"
    assert query.min_sample_size == 100


def test_opponent_pitcher_cohort_snippet() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    connection.execute(
        """
        CREATE TABLE lahman_people (
            playerid TEXT,
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
    connection.execute("INSERT INTO lahman_people VALUES ('player1','slug001','Slugger','One')")
    connection.execute("INSERT INTO retrosheet_allplayers VALUES ('slug001','Slugger','One')")
    upsert_retrosheet_player_opponent_pitcher_cohorts(
        connection,
        [
            {
                "player_id": "player1",
                "cohort_kind": "award",
                "cohort_value": "cy_young",
                "plate_appearances": 40,
                "at_bats": 30,
                "hits": 12,
                "doubles": 2,
                "triples": 0,
                "home_runs": 3,
                "walks": 9,
                "intentional_walks": 1,
                "hit_by_pitch": 0,
                "sacrifice_flies": 1,
                "strikeouts": 5,
                "runs_batted_in": 14,
                "pitchers_faced": 3,
                "first_season": 2021,
                "last_season": 2025,
            }
        ],
    )
    researcher = ContextualPerformanceResearcher(Settings.from_env())
    snippet = researcher.build_snippet(connection, "which hitter has the best OPS against Cy Young Award Winners?")
    connection.close()
    assert snippet is not None
    assert snippet.source == "Opponent Pitcher Cohorts"
    assert snippet.payload["leaders"][0]["player_name"] == "Slugger One"


def test_opponent_pitcher_cohort_snippet_uses_generic_pitcher_matchups() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    connection.execute(
        """
        CREATE TABLE lahman_people (
            playerid TEXT,
            retroid TEXT,
            namefirst TEXT,
            namelast TEXT,
            throws TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE lahman_pitching (
            playerid TEXT
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
    connection.executemany(
        "INSERT INTO lahman_people VALUES (?,?,?,?,?)",
        [
            ("slug001", "slug001", "Slugger", "One", None),
            ("pitchl1", "pitchl1", "Lefty", "Ace", "L"),
            ("pitchr1", "pitchr1", "Righty", "Ace", "R"),
        ],
    )
    connection.executemany(
        "INSERT INTO lahman_pitching VALUES (?)",
        [("pitchl1",), ("pitchr1",)],
    )
    connection.execute("INSERT INTO retrosheet_allplayers VALUES ('slug001','Slugger','One')")
    upsert_retrosheet_player_opponent_pitchers(
        connection,
        [
            {
                "player_id": "slug001",
                "pitcher_id": "pitchl1",
                "plate_appearances": 40,
                "at_bats": 30,
                "hits": 12,
                "doubles": 2,
                "triples": 0,
                "home_runs": 3,
                "walks": 8,
                "intentional_walks": 1,
                "hit_by_pitch": 0,
                "sacrifice_flies": 1,
                "strikeouts": 4,
                "runs_batted_in": 12,
                "first_season": 2021,
                "last_season": 2025,
            },
            {
                "player_id": "slug001",
                "pitcher_id": "pitchr1",
                "plate_appearances": 20,
                "at_bats": 18,
                "hits": 2,
                "doubles": 0,
                "triples": 0,
                "home_runs": 0,
                "walks": 1,
                "intentional_walks": 0,
                "hit_by_pitch": 0,
                "sacrifice_flies": 0,
                "strikeouts": 7,
                "runs_batted_in": 1,
                "first_season": 2021,
                "last_season": 2025,
            },
        ],
    )
    researcher = ContextualPerformanceResearcher(Settings.from_env())
    snippet = researcher.build_snippet(connection, "which hitter has the best OPS against left-handed pitchers?")
    connection.close()
    assert snippet is not None
    assert snippet.source == "Opponent Pitcher Cohorts"
    assert snippet.payload["leaders"][0]["player_name"] == "Slugger One"
    assert snippet.payload["leaders"][0]["pitchers_faced"] == 1


def test_parse_birthday_matchup_query_for_hitter_ops_against_birthday_pitchers() -> None:
    query = parse_birthday_matchup_query(
        "Which hitter has the highest OPS against pitchers on their birthday?"
    )
    assert query is not None
    assert query.subject_role == "hitter"
    assert query.birthday_side == "pitcher"
    assert query.metric_key == "ops"
    assert query.supported is True


def test_parse_birthday_matchup_query_routes_pitcher_era_to_game_lines() -> None:
    query = parse_birthday_matchup_query(
        "Which pitcher has the lowest ERA when facing hitters on their birthday?"
    )
    assert query is not None
    assert query.subject_role == "pitcher"
    assert query.birthday_side == "batter"
    assert query.metric_key == "era"
    assert query.supported is True
    assert query.metric_label == "ERA"
    assert query.source_mode == "pitching_game_lines"


def test_aggregate_opponent_pitcher_chunk_tracks_birthday_flags() -> None:
    import pandas as pd

    frame = pd.DataFrame(
        [
            {
                "batter": "hit001",
                "pitcher": "pit001",
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
                "rbi": "1",
                "date": "20240410",
                "gametype": "regular",
            },
            {
                "batter": "hit001",
                "pitcher": "pit001",
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
                "rbi": "1",
                "date": "20240411",
                "gametype": "regular",
            },
        ]
    )
    totals: dict[tuple[str, str], dict[str, int | str]] = {}
    aggregate_opponent_pitcher_chunk(
        frame,
        totals,
        {
            "hit001": "0410",
            "pit001": "0411",
        },
    )
    row = totals[("hit001", "pit001")]
    assert row["batter_birthday_plate_appearances"] == 1
    assert row["batter_birthday_hits"] == 1
    assert row["pitcher_birthday_plate_appearances"] == 1
    assert row["pitcher_birthday_home_runs"] == 1


def test_birthday_matchup_snippet_for_hitter_against_birthday_pitchers() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    connection.execute(
        """
        CREATE TABLE lahman_people (
            playerid TEXT,
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
    connection.executemany(
        "INSERT INTO lahman_people VALUES (?,?,?,?)",
        [
            ("hit001", "hit001", "Birthday", "Slugger"),
            ("pit001", "pit001", "Birthday", "Ace"),
        ],
    )
    connection.execute("INSERT INTO retrosheet_allplayers VALUES ('hit001','Birthday','Slugger')")
    upsert_retrosheet_player_opponent_pitchers(
        connection,
        [
            {
                "player_id": "hit001",
                "pitcher_id": "pit001",
                "plate_appearances": 30,
                "at_bats": 28,
                "hits": 6,
                "doubles": 1,
                "triples": 0,
                "home_runs": 2,
                "walks": 2,
                "intentional_walks": 0,
                "hit_by_pitch": 0,
                "sacrifice_flies": 0,
                "strikeouts": 4,
                "runs_batted_in": 7,
                "pitcher_birthday_plate_appearances": 30,
                "pitcher_birthday_at_bats": 28,
                "pitcher_birthday_hits": 6,
                "pitcher_birthday_doubles": 1,
                "pitcher_birthday_triples": 0,
                "pitcher_birthday_home_runs": 2,
                "pitcher_birthday_walks": 2,
                "pitcher_birthday_intentional_walks": 0,
                "pitcher_birthday_hit_by_pitch": 0,
                "pitcher_birthday_sacrifice_flies": 0,
                "pitcher_birthday_strikeouts": 4,
                "pitcher_birthday_runs_batted_in": 7,
                "first_season": 2021,
                "last_season": 2024,
            }
        ],
    )
    query = parse_birthday_matchup_query(
        "Which hitter has the highest OPS against pitchers on their birthday?"
    )
    snippet = build_birthday_matchup_snippet(connection, query)
    connection.close()
    assert snippet is not None
    assert snippet.source == "Birthday Matchups"
    assert snippet.payload["leaders"][0]["player_name"] == "Birthday Slugger"


def test_birthday_matchup_snippet_for_pitcher_era_against_birthday_hitters() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    initialize_database(connection)
    connection.execute(
        """
        CREATE TABLE lahman_people (
            playerid TEXT,
            retroid TEXT,
            namefirst TEXT,
            namelast TEXT,
            birthmonth TEXT,
            birthday TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE retrosheet_pitching (
            id TEXT,
            gid TEXT,
            opp TEXT,
            date TEXT,
            stattype TEXT,
            gametype TEXT,
            p_gs TEXT,
            p_ipouts TEXT,
            p_bfp TEXT,
            p_h TEXT,
            p_d TEXT,
            p_t TEXT,
            p_hr TEXT,
            p_r TEXT,
            p_er TEXT,
            p_w TEXT,
            p_iw TEXT,
            p_hbp TEXT,
            p_sh TEXT,
            p_sf TEXT,
            p_k TEXT,
            wp TEXT,
            lp TEXT,
            save TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE retrosheet_batting (
            id TEXT,
            gid TEXT,
            team TEXT,
            date TEXT,
            stattype TEXT,
            gametype TEXT
        )
        """
    )
    connection.executemany(
        "INSERT INTO lahman_people VALUES (?,?,?,?,?,?)",
        [
            ("ace001", "ace001", "Birthday", "Ace", "1", "1"),
            ("work001", "work001", "Worker", "Bee", "1", "1"),
            ("bat001", "bat001", "Birthday", "Batter", "4", "10"),
            ("bat002", "bat002", "Party", "Hitter", "4", "11"),
        ],
    )
    connection.executemany(
        "INSERT INTO retrosheet_pitching VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("ace001", "G1", "NYN", "20240410", "value", "regular", "1", "27", "30", "5", "1", "0", "1", "1", "1", "2", "0", "0", "0", "0", "8", "1", "0", "0"),
            ("work001", "G2", "ATL", "20240411", "value", "regular", "1", "24", "28", "8", "2", "0", "1", "4", "4", "3", "0", "1", "0", "1", "5", "0", "1", "0"),
        ],
    )
    connection.executemany(
        "INSERT INTO retrosheet_batting VALUES (?,?,?,?,?,?)",
        [
            ("bat001", "G1", "NYN", "20240410", "value", "regular"),
            ("bat002", "G2", "ATL", "20240411", "value", "regular"),
        ],
    )
    query = parse_birthday_matchup_query(
        "Which pitcher has the lowest ERA when facing hitters on their birthday?"
    )
    snippet = build_birthday_matchup_snippet(connection, query)
    connection.close()
    assert snippet is not None
    assert snippet.source == "Birthday Matchups"
    assert snippet.payload["source_mode"] == "pitching_game_lines"
    assert snippet.payload["leaders"][0]["player_name"] == "Birthday Ace"
    assert abs(snippet.payload["leaders"][0]["era"] - 1.0) < 1e-9
