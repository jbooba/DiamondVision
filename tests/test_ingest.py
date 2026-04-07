from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.ingest import ingest_project_data
from mlb_history_bot.storage import get_connection, table_exists


def _write_csv(path: Path, header: str, rows: list[str]) -> None:
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")


def test_ingest_skips_retrosheet_plays_csv(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    lahman_dir = raw_dir / "lahman"
    retrosheet_dir = raw_dir / "retrosheet"
    sabr_dir = raw_dir / "sabr"

    lahman_dir.mkdir(parents=True)
    retrosheet_dir.mkdir(parents=True)
    sabr_dir.mkdir(parents=True)
    processed_dir.mkdir(parents=True)

    _write_csv(
        lahman_dir / "Teams.csv",
        "yearID,teamID,W,L",
        ["1945,NY1,78,74"],
    )
    _write_csv(
        retrosheet_dir / "batting.csv",
        "game_id,playerID,H,HR",
        ["game1,player1,1,1"],
    )
    _write_csv(
        retrosheet_dir / "plays.csv",
        "game_id,event_cd,bat_id",
        ["game1,20,player1"],
    )

    settings = Settings(
        project_root=Path(__file__).resolve().parent.parent,
        raw_data_dir=raw_dir,
        processed_data_dir=processed_dir,
        database_path=processed_dir / "mlb_history.sqlite3",
        sabr_docs_dir=sabr_dir,
        openai_model="gpt-5.4",
        openai_reasoning_effort="medium",
        live_season=None,
        user_agent="test-agent",
        fielding_bible_api_base="https://example.com",
        fielding_bible_start_season=2003,
    )

    messages = ingest_project_data(
        settings,
        lahman_dir=lahman_dir,
        retrosheet_dir=retrosheet_dir,
    )

    assert any("Imported 1 Lahman CSV file(s)" in message for message in messages)
    assert any("Imported 1 Retrosheet CSV file(s)" in message for message in messages)

    connection = get_connection(settings.database_path)
    try:
        assert table_exists(connection, "lahman_teams")
        assert table_exists(connection, "retrosheet_batting")
        assert not table_exists(connection, "retrosheet_plays")
    finally:
        connection.close()
