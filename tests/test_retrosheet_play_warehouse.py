from pathlib import Path

from mlb_history_bot.config import Settings
from mlb_history_bot.retrosheet_play_warehouse import sync_retrosheet_play_warehouse
from mlb_history_bot.storage import get_connection, get_metadata_value, list_table_columns, table_exists


def _write_csv(path: Path, header: str, rows: list[str]) -> None:
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")


def test_sync_retrosheet_play_warehouse_imports_raw_play_rows(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    retrosheet_dir = raw_dir / "retrosheet"
    sabr_dir = raw_dir / "sabr"

    retrosheet_dir.mkdir(parents=True)
    processed_dir.mkdir(parents=True)
    sabr_dir.mkdir(parents=True)

    _write_csv(
        retrosheet_dir / "plays.csv",
        "gid,date,batter,pitcher,batteam,event,play,pitch_seq,count,gametype",
        [
            "game1,20250401,judgea001,skubt001,NYA,home_run,HR/F9,CCBFX,2-2,regular",
            "game2,20250402,sotoj001,eovaln001,NYM,walk,W,BBBB,3-0,regular",
        ],
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

    messages = sync_retrosheet_play_warehouse(settings, retrosheet_dir=retrosheet_dir, batch_size=1)

    assert any("Imported 2 raw Retrosheet play row(s)" in message for message in messages)

    connection = get_connection(settings.database_path)
    try:
        assert table_exists(connection, "retrosheet_plays")
        assert connection.execute("SELECT COUNT(*) FROM retrosheet_plays").fetchone()[0] == 2
        columns = list_table_columns(connection, "retrosheet_plays")
        assert "pitch_seq" in columns
        assert "play" in columns
        assert get_metadata_value(connection, "retrosheet_play_warehouse_rows") == "2"
        assert get_metadata_value(connection, "retrosheet_play_warehouse_imported_at") is not None
    finally:
        connection.close()
