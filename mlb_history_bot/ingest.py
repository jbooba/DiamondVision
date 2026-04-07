from __future__ import annotations

from pathlib import Path

from .config import Settings
from .contextual_performance import sync_retrosheet_player_count_splits, sync_retrosheet_player_opponent_contexts
from .fielding_bible import snapshot_current_drs_leaderboards, sync_fielding_bible_data
from .metrics import MetricCatalog
from .retrosheet_splits import sync_retrosheet_team_splits
from .statcast_sync import sync_statcast_data
from .storage import (
    chunk_text,
    get_connection,
    import_csv_file,
    initialize_database,
    replace_document_chunks,
    sync_metric_catalog,
)


def ingest_project_data(
    settings: Settings,
    *,
    lahman_dir: Path | None = None,
    retrosheet_dir: Path | None = None,
    sabr_dir: Path | None = None,
    include_drs: bool = False,
    drs_start_season: int | None = None,
    drs_end_season: int | None = None,
    snapshot_current_drs: bool = False,
    include_statcast: bool = False,
    statcast_start_season: int | None = None,
    statcast_end_season: int | None = None,
    statcast_start_date: str | None = None,
    statcast_end_date: str | None = None,
    statcast_chunk_days: int = 7,
    include_retrosheet_splits: bool = False,
    retrosheet_split_chunk_size: int = 250_000,
    include_retrosheet_counts: bool = False,
    retrosheet_count_chunk_size: int = 250_000,
    include_retrosheet_contexts: bool = False,
    retrosheet_context_chunk_size: int = 250_000,
) -> list[str]:
    settings.ensure_directories()
    notes: list[str] = []
    connection = get_connection(settings.database_path)
    initialize_database(connection)
    sync_metric_catalog(connection, MetricCatalog.load(settings.project_root))

    if lahman_dir and lahman_dir.exists():
        count = _import_csv_directory(connection, lahman_dir, prefix="lahman")
        notes.append(f"Imported {count} Lahman CSV file(s) from {lahman_dir}")

    if retrosheet_dir and retrosheet_dir.exists():
        count = _import_csv_directory(connection, retrosheet_dir, prefix="retrosheet")
        notes.append(f"Imported {count} Retrosheet CSV file(s) from {retrosheet_dir}")

    sabr_source_dir = sabr_dir or settings.sabr_docs_dir
    if sabr_source_dir.exists():
        imported_docs = _import_sabr_documents(connection, sabr_source_dir)
        if imported_docs:
            notes.append(f"Indexed {imported_docs} SABR/local document(s) from {sabr_source_dir}")

    connection.close()
    if include_drs:
        notes.extend(
            sync_fielding_bible_data(
                settings,
                start_season=drs_start_season,
                end_season=drs_end_season,
                snapshot_current=snapshot_current_drs,
            )
        )
    elif snapshot_current_drs:
        notes.extend(snapshot_current_drs_leaderboards(settings, season=drs_end_season))
    if include_statcast:
        notes.extend(
            sync_statcast_data(
                settings,
                start_season=statcast_start_season,
                end_season=statcast_end_season,
                start_date=statcast_start_date,
                end_date=statcast_end_date,
                chunk_days=statcast_chunk_days,
            )
        )
    if include_retrosheet_splits:
        notes.extend(
            sync_retrosheet_team_splits(
                settings,
                retrosheet_dir=retrosheet_dir,
                chunk_size=retrosheet_split_chunk_size,
            )
        )
    if include_retrosheet_counts:
        notes.extend(
            sync_retrosheet_player_count_splits(
                settings,
                retrosheet_dir=retrosheet_dir,
                chunk_size=retrosheet_count_chunk_size,
            )
        )
    if include_retrosheet_contexts:
        notes.extend(
            sync_retrosheet_player_opponent_contexts(
                settings,
                retrosheet_dir=retrosheet_dir,
                chunk_size=retrosheet_context_chunk_size,
            )
        )
    if not notes:
        notes.append("No datasets were imported.")
    return notes


def _import_csv_directory(connection, directory: Path, *, prefix: str) -> int:
    imported = 0
    for csv_path in sorted(directory.glob("*.csv")):
        table_name = f"{prefix}_{csv_path.stem.lower()}"
        import_csv_file(
            connection,
            csv_path,
            table_name=table_name,
            source_name=prefix,
            dataset_name=csv_path.name,
            notes=f"Imported from {csv_path}",
        )
        imported += 1
    return imported


def _import_sabr_documents(connection, directory: Path) -> int:
    imported = 0
    for path in sorted(directory.rglob("*")):
        if not path.is_file():
            continue
        text = _extract_document_text(path)
        if not text:
            continue
        chunks = [
            {
                "title": path.stem,
                "citation": str(path),
                "content": chunk,
                "metadata": {"path": str(path)},
            }
            for chunk in chunk_text(text)
        ]
        if not chunks:
            continue
        replace_document_chunks(
            connection,
            source_kind="sabr",
            source_name=str(path),
            chunks=chunks,
        )
        imported += 1
    return imported


def _extract_document_text(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".txt", ".md", ".html", ".htm"}:
        return path.read_text(encoding="utf-8", errors="replace")
    if suffix == ".pdf":
        try:
            from pypdf import PdfReader
        except ImportError:
            return ""
        reader = PdfReader(str(path))
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    return ""
