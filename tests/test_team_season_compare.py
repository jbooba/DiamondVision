import sqlite3
from pathlib import Path

from mlb_history_bot.provider_metrics import parse_provider_metric_query
from mlb_history_bot.metrics import MetricCatalog
from mlb_history_bot.team_season_compare import parse_team_season_comparison_query


class FakeLiveClient:
    def teams(self, season: int):
        return [
            {
                "id": 137,
                "name": "San Francisco Giants",
                "abbreviation": "SF",
                "shortName": "San Francisco",
                "clubName": "Giants",
                "franchiseName": "San Francisco",
                "locationName": "San Francisco",
                "league": {"name": "National League"},
                "division": {"name": "National League West"},
            },
        ]


def test_parse_provider_metric_query_rejects_single_game_war() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_provider_metric_query(
        "what is the most WAR gained by a player in a single game?",
        catalog,
        2026,
    )
    assert query is None


def test_parse_provider_metric_query_for_pitching_tera() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_provider_metric_query(
        "who led MLB in tERA in 2025?",
        catalog,
        2026,
    )
    assert query is not None
    assert query.metric.metric_name == "tERA"
    assert query.group_preference is None


def test_parse_team_season_comparison_query_for_historical_vs_current() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE lahman_teams (
            yearid INTEGER,
            name TEXT,
            teamidretro TEXT,
            teamid TEXT,
            franchid TEXT,
            w INTEGER,
            l INTEGER
        )
        """
    )
    connection.execute(
        """
        INSERT INTO lahman_teams (yearid, name, teamidretro, teamid, franchid, w, l)
        VALUES (2004, 'Montreal Expos', 'MON', 'MON', 'WSN', 67, 95)
        """
    )
    query = parse_team_season_comparison_query(
        connection,
        "compare the 2004 Expos to the 2026 Giants through the first 10 games of their seasons",
        FakeLiveClient(),
        2026,
    )
    assert query is not None
    assert query.left.display_name == "2004 Montreal Expos"
    assert query.right.display_name == "2026 San Francisco Giants"
    assert query.first_n_games == 10
