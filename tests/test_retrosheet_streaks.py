from __future__ import annotations

import sqlite3
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.retrosheet_streaks import RetrosheetStreakResearcher, sync_retrosheet_player_streaks
from mlb_history_bot.storage import get_connection, initialize_database, table_exists


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
        live_season=None,
        user_agent="test-agent",
        fielding_bible_api_base="https://example.com",
        fielding_bible_start_season=2003,
    )


def seed_batting_and_people(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE lahman_people (
            playerid TEXT PRIMARY KEY,
            namefirst TEXT,
            namelast TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE retrosheet_batting (
            id TEXT,
            gid TEXT,
            date TEXT,
            gametype TEXT,
            b_pa TEXT,
            b_ab TEXT,
            b_r TEXT,
            b_h TEXT,
            b_d TEXT,
            b_t TEXT,
            b_hr TEXT,
            b_rbi TEXT,
            b_w TEXT,
            b_hbp TEXT,
            b_k TEXT,
            b_sb TEXT
        )
        """
    )
    connection.executemany(
        "INSERT INTO lahman_people(playerid, namefirst, namelast) VALUES (?, ?, ?)",
        [
            ("seweljo01", "Joe", "Sewell"),
            ("willite01", "Ted", "Williams"),
            ("dimagjo01", "Joe", "DiMaggio"),
            ("stairsm01", "Matt", "Stairs"),
            ("sluggjo01", "Slugger", "Jones"),
            ("pitchac01", "Pitcher", "Ace"),
        ],
    )
    connection.executemany(
        """
        INSERT INTO retrosheet_batting(id, gid, date, gametype, b_pa, b_ab, b_r, b_h, b_d, b_t, b_hr, b_rbi, b_w, b_hbp, b_k, b_sb)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("dimagjo01", "g1", "19410515", "regular", "4", "4", "1", "1", "0", "0", "0", "1", "0", "0", "0", "0"),
            ("dimagjo01", "g2", "19410516", "regular", "4", "4", "1", "1", "0", "0", "0", "1", "0", "0", "0", "0"),
            ("dimagjo01", "g3", "19410517", "regular", "4", "4", "1", "1", "0", "0", "0", "1", "0", "0", "1", "0"),
            ("stairsm01", "g4", "20010901", "regular", "4", "4", "0", "0", "0", "0", "1", "2", "1", "0", "1", "0"),
            ("stairsm01", "g5", "20010902", "regular", "4", "4", "0", "0", "1", "0", "1", "1", "1", "0", "0", "0"),
            ("stairsm01", "g6", "20010903", "regular", "4", "4", "0", "0", "0", "0", "0", "0", "1", "0", "0", "0"),
        ],
    )
    connection.commit()


def write_retrosheet_plays(retrosheet_dir: Path) -> None:
    retrosheet_dir.mkdir(parents=True, exist_ok=True)
    header = "gid,batter,pitcher,pa,ab,single,double,triple,k,walk,hr,date,gametype"
    rows = [
        "g1,seweljo01,pitchac01,1,1,1,0,0,0,0,0,19220601,regular",
        "g1,seweljo01,pitchac01,1,1,1,0,0,0,0,0,19220601,regular",
        "g1,seweljo01,pitchac01,1,1,1,0,0,0,0,0,19220601,regular",
        "g1,seweljo01,pitchac01,1,1,1,0,0,0,0,0,19220601,regular",
        "g2,seweljo01,pitchac01,1,1,0,0,0,1,0,0,19220602,regular",
        "g3,willite01,pitchac01,1,1,1,0,0,0,0,0,19410701,regular",
        "g3,willite01,pitchac01,1,1,1,0,0,0,0,0,19410701,regular",
        "g3,willite01,pitchac01,1,0,0,0,0,0,0,0,19410701,regular",
        "g4,willite01,pitchac01,1,1,0,0,0,1,0,0,19410702,regular",
        "g5,sluggjo01,pitchac01,1,1,0,0,0,0,0,1,20010501,regular",
        "g5,sluggjo01,pitchac01,1,1,0,0,0,0,0,1,20010501,regular",
        "g6,sluggjo01,pitchac01,1,1,0,0,0,0,0,1,20010502,regular",
        "g6,sluggjo01,pitchac01,1,1,0,0,0,0,0,1,20010502,regular",
        "g7,otherpl01,pitchac01,1,0,0,0,0,0,1,0,20010503,regular",
        "g7,otherpl02,pitchac01,1,0,0,0,0,0,1,0,20010503,regular",
        "g7,otherpl03,pitchac01,1,0,0,0,0,0,1,0,20010503,regular",
        "g7,otherpl04,pitchac01,1,1,0,0,0,1,0,0,20010503,regular",
    ]
    (retrosheet_dir / "plays.csv").write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")


def test_sync_retrosheet_streaks_builds_records_and_answers_queries(tmp_path: Path) -> None:
    settings = build_test_settings(tmp_path)
    connection = get_connection(settings.database_path)
    initialize_database(connection)
    seed_batting_and_people(connection)
    connection.close()
    write_retrosheet_plays(settings.raw_data_dir / "retrosheet")

    messages = sync_retrosheet_player_streaks(
        settings,
        retrosheet_dir=settings.raw_data_dir / "retrosheet",
        chunk_size=4,
    )
    assert any("streak warehouse" in message.lower() for message in messages)

    connection = get_connection(settings.database_path)
    try:
        assert table_exists(connection, "retrosheet_player_streak_records")
        researcher = RetrosheetStreakResearcher(settings)
        ab_snippet = researcher.build_snippet(connection, "what is the longest number of at bats without a strikeout")
        assert ab_snippet is not None
        assert ab_snippet.payload["analysis_type"] == "player_streak_leaderboard"
        assert ab_snippet.payload["rows"][0]["player_name"] == "Joe Sewell"
        assert ab_snippet.payload["rows"][0]["streak_length"] == 4

        spaced_phrase = researcher.build_snippet(connection, "what's the longest at bat streak without a strike out?")
        assert spaced_phrase is not None
        assert spaced_phrase.payload["analysis_type"] == "player_streak_leaderboard"
        assert spaced_phrase.payload["rows"][0]["player_name"] == "Joe Sewell"
        assert spaced_phrase.payload["rows"][0]["streak_length"] == 4

        walk_snippet = researcher.build_snippet(connection, "who has the longest walk streak")
        assert walk_snippet is not None
        assert walk_snippet.payload["rows"][0]["player_name"] == "Matt Stairs"
        assert walk_snippet.payload["rows"][0]["streak_length"] == 3

        hitless_snippet = researcher.build_snippet(connection, "who has the longest games without a hit")
        assert hitless_snippet is not None
        assert hitless_snippet.payload["rows"][0]["player_name"] == "Matt Stairs"
        assert hitless_snippet.payload["rows"][0]["streak_length"] == 3

        hit_snippet = researcher.build_snippet(connection, "who has the longest hit streak")
        assert hit_snippet is not None
        assert hit_snippet.payload["rows"][0]["player_name"] == "Joe DiMaggio"
        assert hit_snippet.payload["rows"][0]["streak_length"] == 3

        xbh_snippet = researcher.build_snippet(connection, "what is the longest streak of extra base hits in MLB history")
        assert xbh_snippet is not None
        assert xbh_snippet.payload["rows"][0]["player_name"] == "Matt Stairs"
        assert xbh_snippet.payload["rows"][0]["streak_length"] == 2

        hit_ab_snippet = researcher.build_snippet(connection, "what is the major league record for most consecutive at bats with a hit")
        assert hit_ab_snippet is not None
        assert hit_ab_snippet.payload["rows"][0]["player_name"] == "Joe Sewell"
        assert hit_ab_snippet.payload["rows"][0]["streak_length"] == 4

        hr_ab_snippet = researcher.build_snippet(connection, "has anyone ever hit a home run in four straight at bats?")
        assert hr_ab_snippet is not None
        assert hr_ab_snippet.payload["rows"][0]["player_name"] == "Slugger Jones"
        assert hr_ab_snippet.payload["rows"][0]["streak_length"] == 4

        hr_streak_snippet = researcher.build_snippet(connection, "most consecutive homeruns by one player")
        assert hr_streak_snippet is not None
        assert hr_streak_snippet.payload["rows"][0]["player_name"] == "Matt Stairs"
        assert hr_streak_snippet.payload["rows"][0]["streak_length"] == 2

        pitcher_walks_snippet = researcher.build_snippet(connection, "what is the most consecutive walks recorded by a pitcher in a single game")
        assert pitcher_walks_snippet is not None
        assert pitcher_walks_snippet.payload["rows"][0]["player_name"] == "Pitcher Ace"
        assert pitcher_walks_snippet.payload["rows"][0]["streak_length"] == 3
    finally:
        connection.close()
