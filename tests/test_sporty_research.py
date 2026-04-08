from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.models import EvidenceSnippet
from mlb_history_bot.search import BaseballResearchEngine
from mlb_history_bot.sporty_research import SportyReplayFinder
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

    def build_recent_player_snippets(self, question: str) -> list[EvidenceSnippet]:
        return self.build_snippets(question)


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
    connection.execute(
        """
        INSERT INTO statcast_events (
            season, game_date, game_pk, at_bat_number, pitch_number,
            batter_id, batter_name, pitcher_id, pitcher_name,
            batting_team, pitching_team, home_team, away_team,
            pitch_type, pitch_name, pitch_family, event,
            is_ab, is_hit, is_home_run, is_strikeout, is_xbh,
            has_risp, count_key, runs_batted_in, horizontal_location, vertical_location, field_direction,
            release_speed, release_spin_rate, launch_speed, launch_angle, hit_distance,
            bat_speed, estimated_ba, estimated_woba, estimated_slg
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            2026,
            "2026-04-07",
            42,
            3,
            4,
            100,
            "Freddie Freeman",
            200,
            "Pitcher Example",
            "LAN",
            "ARI",
            "LAN",
            "ARI",
            "FF",
            "4-Seam Fastball",
            "fastball",
            "home_run",
            1,
            1,
            1,
            0,
            1,
            0,
            "2-1",
            1,
            "middle",
            "middle",
            "right field",
            95.2,
            2310.0,
            105.8,
            27.0,
            403.0,
            74.0,
            0.910,
            0.880,
            2.100,
        ),
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
        assert context.replay_evidence[0].payload["clip_count"] == 1


class FakeReplayLiveClient:
    def person_details(self, player_id: int) -> dict:
        return {"currentTeam": {"id": 119}}

    def schedule_range(self, start_date: str, end_date: str, *, hydrate: str | None = None, team_id: int | None = None) -> dict:
        return {"dates": []}

    def game_feed(self, game_pk: int) -> dict:
        return {
            "liveData": {
                "plays": {
                    "allPlays": [
                        {
                            "about": {"inning": 6, "halfInning": "top", "atBatIndex": 2},
                            "atBatIndex": 2,
                            "result": {
                                "eventType": "home_run",
                                "description": "Freddie Freeman hits a home run to right field.",
                            },
                            "matchup": {
                                "batter": {"id": 100, "fullName": "Freddie Freeman"},
                                "pitcher": {"id": 200, "fullName": "Pitcher Example"},
                            },
                            "playEvents": [{"playId": "freddie-play"}],
                        }
                    ]
                }
            }
        }


class FakeReplayVideoClient:
    def fetch(self, play_id: str):
        return None


def test_replay_finder_falls_back_to_recent_statcast_homer_for_generic_clip_prompt() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        database_path = tmp_path / "mlb_history.sqlite3"
        build_test_database(database_path)
        settings = Settings.from_env(Path(__file__).resolve().parents[1])
        settings.database_path = database_path
        settings.processed_data_dir = tmp_path

        finder = SportyReplayFinder(settings)
        finder.live_client = FakeReplayLiveClient()
        finder.sporty_video_client = FakeReplayVideoClient()
        finder._resolve_players = lambda _queries: {100: "Freddie Freeman"}

        snippets = finder.build_snippets("show me a freddie freeman homerun")

        assert snippets
        payload = snippets[0].payload
        assert payload["clip_count"] == 1
        assert payload["clips"][0]["actor_name"] == "Freddie Freeman"
        assert payload["clips"][0]["savant_url"].endswith("playId=freddie-play")
