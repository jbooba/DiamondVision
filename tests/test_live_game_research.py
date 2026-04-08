from __future__ import annotations

from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.live_game_research import LiveGameResearcher


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


def test_live_game_research_defaults_scoring_breakdown_queries_to_today() -> None:
    researcher = LiveGameResearcher(build_test_settings())
    researcher.live_client = FakeLiveClient()

    snippet = researcher.build_snippet("how did Arizona score their runs?")

    assert snippet is not None
    assert snippet.source == "Live Game Research"
    assert "Scoring plays for Diamondbacks" in snippet.summary
    assert "Corbin Carroll singles" in snippet.summary
    assert "Eugenio Suarez hits a 3-run home run" in snippet.summary
    assert len(snippet.payload["scoring_plays"]) == 2
