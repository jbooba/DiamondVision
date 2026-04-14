from __future__ import annotations

from pathlib import Path

from .config import Settings
from .retrosheet_splits import open_retrosheet_plays_stream
from .storage import (
    RETROSHEET_PLAYS_TABLE,
    get_connection,
    import_retrosheet_plays_stream,
    initialize_database,
)


def sync_retrosheet_play_warehouse(
    settings: Settings,
    *,
    retrosheet_dir: Path | None = None,
    batch_size: int = 5000,
) -> list[str]:
    settings.ensure_directories()
    source_dir = retrosheet_dir or settings.raw_data_dir / "retrosheet"
    if not source_dir.exists():
        raise FileNotFoundError(f"Retrosheet directory not found: {source_dir}")

    connection = get_connection(settings.database_path)
    try:
        initialize_database(connection)
        with open_retrosheet_plays_stream(source_dir) as handle:
            source_description = str(getattr(handle, "name", "") or (source_dir / "plays.csv"))
            row_count = import_retrosheet_plays_stream(
                connection,
                handle,
                table_name=RETROSHEET_PLAYS_TABLE,
                source_name="retrosheet",
                dataset_name="plays.csv",
                notes="Imported full raw Retrosheet play/event warehouse from plays.csv.",
                batch_size=batch_size,
            )
    finally:
        connection.close()

    return [
        f"Imported {row_count} raw Retrosheet play row(s) into {RETROSHEET_PLAYS_TABLE} from {source_description}."
    ]
