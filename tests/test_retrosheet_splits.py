from pathlib import Path

import pandas as pd

from mlb_history_bot.api import build_snippet_display
from mlb_history_bot.metrics import MetricCatalog
from mlb_history_bot.models import EvidenceSnippet
from mlb_history_bot.query_utils import extract_first_n_games
from mlb_history_bot.retrosheet_splits import aggregate_situational_chunk, parse_team_split_history_query


def test_extract_first_n_games_handles_number_words() -> None:
    assert extract_first_n_games("what team had the worst BA with RISP in the first ten games of a season?") == 10


def test_parse_team_split_history_query_for_risp() -> None:
    catalog = MetricCatalog.load(Path(__file__).resolve().parents[1])
    query = parse_team_split_history_query(
        "what team had the worst BA with RISP in the first ten games of a season?",
        catalog,
    )
    assert query is not None
    assert query.split.key == "risp"
    assert query.metric.metric_name == "BA"
    assert query.first_n_games == 10
    assert query.descriptor == "worst"
    assert query.sort_desc is False


def test_aggregate_situational_chunk_tracks_zero_split_games() -> None:
    frame = pd.DataFrame(
        [
            {
                "gid": "AAA202604010",
                "batteam": "NYM",
                "walk": "0",
                "iw": "0",
                "hbp": "0",
                "sf": "0",
                "k": "0",
                "pa": "1",
                "ab": "1",
                "single": "1",
                "double": "0",
                "triple": "0",
                "hr": "0",
                "rbi": "0",
                "br1_pre": "",
                "br2_pre": "",
                "br3_pre": "",
                "date": "20260401",
                "gametype": "regular",
            },
            {
                "gid": "AAA202604020",
                "batteam": "NYM",
                "walk": "0",
                "iw": "0",
                "hbp": "0",
                "sf": "0",
                "k": "0",
                "pa": "1",
                "ab": "1",
                "single": "0",
                "double": "1",
                "triple": "0",
                "hr": "0",
                "rbi": "1",
                "br1_pre": "",
                "br2_pre": "runner2",
                "br3_pre": "",
                "date": "20260402",
                "gametype": "regular",
            },
        ]
    )
    rows, regular_plays = aggregate_situational_chunk(frame)
    assert regular_plays == 2
    risp_rows = [row for row in rows if row["split_key"] == "risp" and row["team"] == "NYM"]
    assert len(risp_rows) == 3
    first_game = next(row for row in risp_rows if row["gid"] == "AAA202604010" and row["at_bats"] == 0)
    second_game = next(row for row in risp_rows if row["gid"] == "AAA202604020" and row["at_bats"] == 1)
    assert first_game["hits"] == 0
    assert second_game["hits"] == 1
    assert second_game["runs_batted_in"] == 1


def test_build_snippet_display_creates_table_for_team_rankings() -> None:
    snippet = EvidenceSnippet(
        source="Statcast Team Windows",
        title="xBA through first 10 games",
        citation="Public Statcast plate appearance data aggregated to team-game level",
        summary="Across synced Statcast seasons, the lowest team xBA through the first 10 games of a season was the 2019 Athletics at 0.177.",
        payload={
            "analysis_type": "statcast_team_window_ranking",
            "metric": "xBA",
            "metric_label": "xBA",
            "first_n_games": 10,
            "descriptor": "lowest",
            "leaders": [
                {"season": 2019, "team_name": "Athletics", "games_played": 10, "metric_value": 0.177},
                {"season": 2022, "team_name": "Diamondbacks", "games_played": 10, "metric_value": 0.181},
            ],
        },
    )
    display = build_snippet_display(snippet)
    assert display is not None
    assert display["kind"] == "table"
    assert display["columns"][-1]["label"] == "xBA"
    assert display["rows"][0]["rank"] == "1"
    assert display["rows"][0]["team_name"] == "Athletics"
