from __future__ import annotations

from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.live_game_research import LiveGameResearcher
from mlb_history_bot.sporty_video import SportyVideoPage


def build_test_settings() -> Settings:
    return Settings.from_env(Path(__file__).resolve().parents[1])


class FakeLiveClient:
    def teams(self, season: int | None = None) -> list[dict]:
        return [
            {
                "id": 109,
                "name": "Arizona Diamondbacks",
                "abbreviation": "ARI",
                "shortName": "Diamondbacks",
                "clubName": "Diamondbacks",
                "franchiseName": "Diamondbacks",
                "locationName": "Arizona",
                "league": {"name": "NL"},
                "division": {"name": "West"},
            },
            {
                "id": 121,
                "name": "New York Mets",
                "abbreviation": "NYM",
                "shortName": "Mets",
                "clubName": "Mets",
                "franchiseName": "Mets",
                "locationName": "New York",
                "league": {"name": "NL"},
                "division": {"name": "East"},
            },
        ]

    def schedule(self, target_date: str, *, hydrate: str | None = None) -> dict:
        return {
            "dates": [
                {
                    "date": target_date,
                    "games": [
                        {
                            "gamePk": 700,
                            "status": {"detailedState": "In Progress"},
                            "teams": {
                                "away": {"team": {"id": 109, "name": "Arizona Diamondbacks"}, "score": 5},
                                "home": {"team": {"id": 121, "name": "New York Mets"}, "score": 0},
                            },
                        }
                    ],
                }
            ]
        }

    def game_feed(self, game_pk: int) -> dict:
        return {
            "liveData": {
                "plays": {
                    "allPlays": [
                        {
                            "about": {"inning": 1, "halfInning": "top"},
                            "result": {
                                "eventType": "single",
                                "description": "Corbin Carroll singles on a line drive, scoring Ketel Marte.",
                            },
                            "matchup": {
                                "batter": {"fullName": "Corbin Carroll"},
                                "pitcher": {"fullName": "David Peterson"},
                            },
                            "runners": [{"details": {"isScoringEvent": True}}],
                            "playEvents": [{"playId": "score-play-1"}],
                        },
                        {
                            "about": {"inning": 3, "halfInning": "top"},
                            "result": {
                                "eventType": "home_run",
                                "description": "Eugenio Suarez hits a 3-run home run to left field.",
                            },
                            "matchup": {
                                "batter": {"fullName": "Eugenio Suarez"},
                                "pitcher": {"fullName": "David Peterson"},
                            },
                            "runners": [
                                {"details": {"isScoringEvent": True}},
                                {"details": {"isScoringEvent": True}},
                                {"details": {"isScoringEvent": True}},
                            ],
                            "playEvents": [{"playId": "score-play-2"}],
                        },
                    ]
                },
                "boxscore": {"teams": {"away": {"team": {"id": 109}, "players": {}}, "home": {"team": {"id": 121}, "players": {}}}},
            }
        }

    def search_people(self, query: str) -> list[dict]:
        if query.strip().casefold() == "aaron judge":
            return [{"id": 99, "fullName": "Aaron Judge", "active": True, "isPlayer": True, "isVerified": True}]
        return []

    def person_details(self, player_id: int) -> dict | None:
        if player_id == 99:
            return {"id": 99, "fullName": "Aaron Judge", "currentTeam": {"id": 147, "name": "New York Yankees"}}
        return None


class FakeSportyVideoClient:
    def fetch(self, play_id: str):
        if play_id == "score-play-1":
            return SportyVideoPage(
                play_id=play_id,
                title="Corbin Carroll RBI single",
                savant_url=f"https://baseballsavant.mlb.com/sporty-videos?playId={play_id}",
                mp4_url="https://example.com/score-play-1.mp4",
                batter="Corbin Carroll",
                pitcher="David Peterson",
                exit_velocity=101.2,
                launch_angle=12.0,
                hit_distance=None,
                hr_parks=None,
                matchup="ARI @ NYM",
                page_date="2026-04-08",
            )
        if play_id == "score-play-2":
            return SportyVideoPage(
                play_id=play_id,
                title="Eugenio Suarez belts a 3-run homer",
                savant_url=f"https://baseballsavant.mlb.com/sporty-videos?playId={play_id}",
                mp4_url="https://example.com/score-play-2.mp4",
                batter="Eugenio Suarez",
                pitcher="David Peterson",
                exit_velocity=106.4,
                launch_angle=28.0,
                hit_distance=414.0,
                hr_parks=28,
                matchup="ARI @ NYM",
                page_date="2026-04-08",
            )
        if play_id == "judge-play":
            return SportyVideoPage(
                play_id=play_id,
                title="Aaron Judge belts a home run",
                savant_url=f"https://baseballsavant.mlb.com/sporty-videos?playId={play_id}",
                mp4_url="https://example.com/judge-play.mp4",
                batter="Aaron Judge",
                pitcher="Pitcher Example",
                exit_velocity=111.7,
                launch_angle=29.0,
                hit_distance=431.0,
                hr_parks=30,
                matchup="NYY @ BOS",
                page_date="2026-04-08",
            )
        return None


def test_live_game_research_defaults_scoring_breakdown_queries_to_today() -> None:
    researcher = LiveGameResearcher(build_test_settings())
    researcher.live_client = FakeLiveClient()
    researcher.sporty_video_client = FakeSportyVideoClient()

    snippet = researcher.build_snippet("how did Arizona score their runs?")

    assert snippet is not None
    assert snippet.source == "Live Game Research"
    assert "Scoring plays for Diamondbacks" in snippet.summary
    assert "Loaded 2 scoring-play clip(s)" in snippet.summary
    assert "Corbin Carroll singles" in snippet.summary
    assert "Eugenio Suarez hits a 3-run home run" in snippet.summary
    assert len(snippet.payload["scoring_plays"]) == 2
    assert len(snippet.payload["clips"]) == 2
    assert snippet.payload["clips"][0]["mp4_url"] == "https://example.com/score-play-1.mp4"
    assert snippet.payload["clips"][1]["hr_parks"] == 28


class FakeHomeRunClipLiveClient:
    def teams(self, season: int | None = None) -> list[dict]:
        return [
            {"id": 147, "name": "New York Yankees", "abbreviation": "NYY", "shortName": "Yankees", "clubName": "Yankees", "franchiseName": "Yankees", "locationName": "New York"},
            {"id": 111, "name": "Boston Red Sox", "abbreviation": "BOS", "shortName": "Red Sox", "clubName": "Red Sox", "franchiseName": "Red Sox", "locationName": "Boston"},
        ]

    def schedule(self, target_date: str, *, hydrate: str | None = None) -> dict:
        return {
            "dates": [
                {
                    "date": target_date,
                    "games": [
                        {
                            "gamePk": 1001,
                            "status": {"detailedState": "Final"},
                            "teams": {
                                "away": {"team": {"id": 147, "name": "New York Yankees"}, "score": 4},
                                "home": {"team": {"id": 111, "name": "Boston Red Sox"}, "score": 3},
                            },
                        }
                    ],
                }
            ]
        }

    def game_feed(self, game_pk: int) -> dict:
        return {
            "liveData": {
                "plays": {
                    "allPlays": [
                        {
                            "about": {"inning": 1, "halfInning": "top", "atBatIndex": 0},
                            "result": {"eventType": "home_run", "description": "Aaron Judge homers to left field."},
                            "matchup": {
                                "batter": {"id": 99, "fullName": "Aaron Judge"},
                                "pitcher": {"id": 33, "fullName": "Pitcher Example"},
                            },
                            "playEvents": [{"playId": "judge-play"}],
                        }
                    ]
                },
                "boxscore": {"teams": {"away": {"team": {"id": 147}, "players": {}}, "home": {"team": {"id": 111}, "players": {}}}},
            }
        }


class FakePlayerPerformanceLiveClient:
    def teams(self, season: int | None = None) -> list[dict]:
        return [
            {"id": 147, "name": "New York Yankees", "abbreviation": "NYY", "shortName": "Yankees", "clubName": "Yankees", "franchiseName": "Yankees", "locationName": "New York"},
            {"id": 111, "name": "Boston Red Sox", "abbreviation": "BOS", "shortName": "Red Sox", "clubName": "Red Sox", "franchiseName": "Red Sox", "locationName": "Boston"},
        ]

    def schedule(self, target_date: str, *, hydrate: str | None = None) -> dict:
        return {
            "dates": [
                {
                    "date": target_date,
                    "games": [
                        {
                            "gamePk": 1001,
                            "status": {"detailedState": "Final"},
                            "teams": {
                                "away": {"team": {"id": 147, "name": "New York Yankees"}, "score": 4},
                                "home": {"team": {"id": 111, "name": "Boston Red Sox"}, "score": 3},
                            },
                        }
                    ],
                }
            ]
        }

    def game_feed(self, game_pk: int) -> dict:
        return {
            "liveData": {
                "plays": {
                    "allPlays": [
                        {
                            "about": {"inning": 1, "halfInning": "top", "atBatIndex": 0},
                            "result": {"eventType": "home_run", "description": "Aaron Judge homers to left field."},
                            "matchup": {
                                "batter": {"id": 99, "fullName": "Aaron Judge"},
                                "pitcher": {"id": 33, "fullName": "Pitcher Example"},
                            },
                            "runners": [{"details": {"isScoringEvent": True}}],
                            "playEvents": [{"playId": "judge-play"}],
                        }
                    ]
                },
                "boxscore": {
                    "teams": {
                        "away": {
                            "team": {"id": 147, "name": "New York Yankees"},
                            "players": {
                                "ID99": {
                                    "person": {"id": 99, "fullName": "Aaron Judge"},
                                    "position": {"abbreviation": "RF"},
                                    "stats": {
                                        "batting": {
                                            "atBats": 4,
                                            "hits": 2,
                                            "runs": 1,
                                            "homeRuns": 1,
                                            "rbi": 2,
                                            "baseOnBalls": 1,
                                            "strikeOuts": 1,
                                        },
                                        "pitching": {},
                                        "fielding": {"errors": 0},
                                    },
                                }
                            },
                        },
                        "home": {"team": {"id": 111, "name": "Boston Red Sox"}, "players": {}},
                    }
                },
            }
        }

    def search_people(self, query: str) -> list[dict]:
        if query.strip().casefold() == "aaron judge":
            return [{"id": 99, "fullName": "Aaron Judge", "active": True, "isPlayer": True, "isVerified": True}]
        return []

    def person_details(self, player_id: int) -> dict | None:
        if player_id == 99:
            return {"id": 99, "fullName": "Aaron Judge", "currentTeam": {"id": 147, "name": "New York Yankees"}}
        return None


class FakeScheduledPlayerPerformanceLiveClient(FakePlayerPerformanceLiveClient):
    def schedule(self, target_date: str, *, hydrate: str | None = None) -> dict:
        return {
            "dates": [
                {
                    "date": target_date,
                    "games": [
                        {
                            "gamePk": 1002,
                            "status": {"detailedState": "Scheduled"},
                            "teams": {
                                "away": {"team": {"id": 147, "name": "New York Yankees"}, "score": 0},
                                "home": {"team": {"id": 111, "name": "Boston Red Sox"}, "score": 0},
                            },
                        }
                    ],
                }
            ]
        }

    def game_feed(self, game_pk: int) -> dict:
        return {
            "liveData": {
                "plays": {"allPlays": []},
                "boxscore": {
                    "teams": {
                        "away": {"team": {"id": 147, "name": "New York Yankees"}, "players": {}},
                        "home": {"team": {"id": 111, "name": "Boston Red Sox"}, "players": {}},
                    }
                },
            }
        }


def test_live_game_research_builds_daily_home_run_clips_from_game_feeds() -> None:
    researcher = LiveGameResearcher(build_test_settings())
    researcher.live_client = FakeHomeRunClipLiveClient()
    researcher.sporty_video_client = FakeSportyVideoClient()

    snippet = researcher.build_snippet("show me clips of yesterdays homeruns")

    assert snippet is not None
    assert snippet.source == "Live Game Research"
    assert snippet.payload["analysis_type"] == "daily_home_run_clips"
    assert snippet.payload["clip_count"] == 1
    assert snippet.payload["clips"][0]["batter_name"] == "Aaron Judge"
    assert snippet.payload["clips"][0]["mp4_url"] == "https://example.com/judge-play.mp4"


def test_live_game_research_builds_player_day_performance_with_clips() -> None:
    researcher = LiveGameResearcher(build_test_settings())
    researcher.live_client = FakePlayerPerformanceLiveClient()
    researcher.sporty_video_client = FakeSportyVideoClient()

    snippet = researcher.build_snippet("how did Aaron Judge play yesterday?")

    assert snippet is not None
    assert snippet.source == "Live Game Research"
    assert snippet.payload["analysis_type"] == "player_day_performance"
    assert "2-for-4" in snippet.summary
    assert "1 HR" in snippet.summary
    assert snippet.payload["clip_count"] == 1
    assert snippet.payload["clips"][0]["batter_name"] == "Aaron Judge"


def test_live_game_research_reports_scheduled_game_for_today_player_query() -> None:
    researcher = LiveGameResearcher(build_test_settings())
    researcher.live_client = FakeScheduledPlayerPerformanceLiveClient()
    researcher.sporty_video_client = FakeSportyVideoClient()

    snippet = researcher.build_snippet("how did Aaron Judge play today?")

    assert snippet is not None
    assert snippet.payload["analysis_type"] == "player_day_performance"
    assert "has not played yet" in snippet.summary
    assert snippet.payload["clip_count"] == 0
