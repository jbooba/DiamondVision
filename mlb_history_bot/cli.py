from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from .bootstrap import bootstrap_datasets
from .chat import BaseballChatbot
from .config import Settings
from .contextual_performance import (
    sync_retrosheet_player_count_splits,
    sync_retrosheet_player_opponent_contexts,
    sync_retrosheet_player_opponent_pitcher_cohorts,
)
from .fielding_bible import snapshot_current_drs_leaderboards, sync_fielding_bible_data
from .ingest import ingest_project_data
from .retrosheet_play_warehouse import sync_retrosheet_play_warehouse
from .retrosheet_streaks import sync_retrosheet_player_streaks
from .retrosheet_splits import sync_retrosheet_team_splits
from .statcast_history_refresh import refresh_bundled_statcast_history
from .statcast_sync import sync_statcast_data
from .storage import (
    RETROSHEET_PLAYS_TABLE,
    STATCAST_HISTORY_BATTER_TABLE,
    STATCAST_HISTORY_PITCHER_TABLE,
    audit_statcast_history_table,
    get_connection,
    get_metadata_value,
    import_statcast_history_exports,
    table_exists,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MLB history chatbot")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare", help="Download core datasets and ingest them")
    prepare_parser.add_argument("--include-retrosheet-plays", action="store_true")
    prepare_parser.add_argument("--sabr-dir", type=Path)
    prepare_parser.add_argument("--with-drs", action="store_true")
    prepare_parser.add_argument("--drs-start-season", type=int)
    prepare_parser.add_argument("--drs-end-season", type=int)
    prepare_parser.add_argument("--snapshot-current-drs", action="store_true")
    prepare_parser.add_argument("--with-statcast", action="store_true")
    prepare_parser.add_argument("--statcast-start-season", type=int)
    prepare_parser.add_argument("--statcast-end-season", type=int)
    prepare_parser.add_argument("--statcast-start-date", type=str)
    prepare_parser.add_argument("--statcast-end-date", type=str)
    prepare_parser.add_argument("--statcast-chunk-days", type=int, default=7)
    prepare_parser.add_argument("--with-retrosheet-splits", action="store_true")
    prepare_parser.add_argument("--retrosheet-split-chunk-size", type=int, default=250000)
    prepare_parser.add_argument("--with-retrosheet-counts", action="store_true")
    prepare_parser.add_argument("--retrosheet-count-chunk-size", type=int, default=250000)
    prepare_parser.add_argument("--with-retrosheet-contexts", action="store_true")
    prepare_parser.add_argument("--retrosheet-context-chunk-size", type=int, default=250000)
    prepare_parser.add_argument("--with-retrosheet-streaks", action="store_true")
    prepare_parser.add_argument("--retrosheet-streak-chunk-size", type=int, default=250000)
    prepare_parser.add_argument("--with-retrosheet-play-warehouse", action="store_true")
    prepare_parser.add_argument("--retrosheet-play-batch-size", type=int, default=5000)

    bootstrap_parser = subparsers.add_parser("bootstrap", help="Download Lahman and Retrosheet core files")
    bootstrap_parser.add_argument("--include-retrosheet-plays", action="store_true")

    ingest_parser = subparsers.add_parser("ingest", help="Ingest local datasets into SQLite")
    ingest_parser.add_argument("--lahman-dir", type=Path)
    ingest_parser.add_argument("--retrosheet-dir", type=Path)
    ingest_parser.add_argument("--sabr-dir", type=Path)
    ingest_parser.add_argument("--with-drs", action="store_true")
    ingest_parser.add_argument("--drs-start-season", type=int)
    ingest_parser.add_argument("--drs-end-season", type=int)
    ingest_parser.add_argument("--snapshot-current-drs", action="store_true")
    ingest_parser.add_argument("--with-statcast", action="store_true")
    ingest_parser.add_argument("--statcast-start-season", type=int)
    ingest_parser.add_argument("--statcast-end-season", type=int)
    ingest_parser.add_argument("--statcast-start-date", type=str)
    ingest_parser.add_argument("--statcast-end-date", type=str)
    ingest_parser.add_argument("--statcast-chunk-days", type=int, default=7)
    ingest_parser.add_argument("--with-retrosheet-splits", action="store_true")
    ingest_parser.add_argument("--retrosheet-split-chunk-size", type=int, default=250000)
    ingest_parser.add_argument("--with-retrosheet-counts", action="store_true")
    ingest_parser.add_argument("--retrosheet-count-chunk-size", type=int, default=250000)
    ingest_parser.add_argument("--with-retrosheet-contexts", action="store_true")
    ingest_parser.add_argument("--retrosheet-context-chunk-size", type=int, default=250000)
    ingest_parser.add_argument("--with-retrosheet-streaks", action="store_true")
    ingest_parser.add_argument("--retrosheet-streak-chunk-size", type=int, default=250000)
    ingest_parser.add_argument("--with-retrosheet-play-warehouse", action="store_true")
    ingest_parser.add_argument("--retrosheet-play-batch-size", type=int, default=5000)

    sync_drs_parser = subparsers.add_parser("sync-drs", help="Sync exact Fielding Bible/SIS DRS datasets")
    sync_drs_parser.add_argument("--start-season", type=int)
    sync_drs_parser.add_argument("--end-season", type=int)
    sync_drs_parser.add_argument("--snapshot-current", action="store_true")

    snapshot_drs_parser = subparsers.add_parser(
        "snapshot-drs",
        help="Store a timestamped snapshot of the current Fielding Bible/SIS DRS leaderboard",
    )
    snapshot_drs_parser.add_argument("--season", type=int)

    statcast_parser = subparsers.add_parser(
        "sync-statcast",
        help="Sync public Statcast team-game aggregates for expected-metric research",
    )
    statcast_parser.add_argument("--start-season", type=int)
    statcast_parser.add_argument("--end-season", type=int)
    statcast_parser.add_argument("--start-date", type=str)
    statcast_parser.add_argument("--end-date", type=str)
    statcast_parser.add_argument("--chunk-days", type=int, default=7)
    statcast_parser.add_argument("--daily", action="store_true")
    statcast_parser.add_argument("--backfill-days", type=int, default=3)

    statcast_history_parser = subparsers.add_parser(
        "import-statcast-history",
        help="Import season-level Statcast custom leaderboard history CSV exports",
    )
    statcast_history_parser.add_argument("--batter-csv", type=Path)
    statcast_history_parser.add_argument("--pitcher-csv", type=Path)

    bundled_statcast_history_parser = subparsers.add_parser(
        "import-bundled-statcast-history",
        help="Import the bundled Statcast custom leaderboard history CSV exports from the repo",
    )
    bundled_statcast_history_parser.add_argument(
        "--data-dir",
        type=Path,
        help="Optional override for the bundled CSV directory (defaults to data/statcast_history)",
    )

    refresh_bundled_statcast_history_parser = subparsers.add_parser(
        "refresh-bundled-statcast-history",
        help="Fetch fresh bundled Statcast custom-history CSVs from Baseball Savant custom leaderboards",
    )
    refresh_bundled_statcast_history_parser.add_argument(
        "--data-dir",
        type=Path,
        help="Optional override for the bundled CSV directory (defaults to data/statcast_history)",
    )
    refresh_bundled_statcast_history_parser.add_argument(
        "--full-history",
        action="store_true",
        help="Fetch the full Savant history window instead of only the current live season",
    )
    refresh_bundled_statcast_history_parser.add_argument(
        "--season",
        type=int,
        help="Override the season to refresh (defaults to MLB_HISTORY_LIVE_SEASON or the current year)",
    )

    audit_statcast_history_parser = subparsers.add_parser(
        "audit-statcast-history",
        help="Inspect imported Statcast custom-history coverage by table, year, and optional player",
    )
    audit_statcast_history_parser.add_argument(
        "--player",
        type=str,
        help="Optional player name to inspect within the imported Statcast custom-history tables",
    )
    audit_statcast_history_parser.add_argument(
        "--role",
        choices=("batter", "pitcher", "both"),
        default="both",
        help="Which imported Statcast history table(s) to inspect",
    )

    refresh_statcast_parser = subparsers.add_parser(
        "refresh-statcast-daily",
        help="Refresh daily Statcast sync tables and optionally re-import bundled custom-history CSV exports",
    )
    refresh_statcast_parser.add_argument("--chunk-days", type=int, default=3)
    refresh_statcast_parser.add_argument("--backfill-days", type=int, default=3)
    refresh_statcast_parser.add_argument(
        "--history-dir",
        type=Path,
        help="Optional override for the bundled Statcast history CSV directory (defaults to data/statcast_history)",
    )
    refresh_statcast_parser.add_argument(
        "--skip-history",
        action="store_true",
        help="Skip importing bundled Statcast history CSVs after the daily sync finishes",
    )
    refresh_statcast_parser.add_argument(
        "--fetch-history-from-savant",
        action="store_true",
        help="Refresh the bundled Statcast history CSVs from Savant before importing them",
    )
    refresh_statcast_parser.add_argument(
        "--full-history-fetch",
        action="store_true",
        help="When fetching bundled Statcast history from Savant, pull full history instead of only the live season",
    )
    refresh_statcast_parser.add_argument(
        "--history-season",
        type=int,
        help="Optional season override when fetching bundled Statcast history from Savant",
    )

    retrosheet_split_parser = subparsers.add_parser(
        "sync-retrosheet-splits",
        help="Build compact team-game situational split aggregates from Retrosheet plays",
    )
    retrosheet_split_parser.add_argument("--retrosheet-dir", type=Path)
    retrosheet_split_parser.add_argument("--chunk-size", type=int, default=250000)

    retrosheet_count_parser = subparsers.add_parser(
        "sync-retrosheet-counts",
        help="Build compact player terminal-count split aggregates from Retrosheet plays",
    )
    retrosheet_count_parser.add_argument("--retrosheet-dir", type=Path)
    retrosheet_count_parser.add_argument("--chunk-size", type=int, default=250000)

    retrosheet_context_parser = subparsers.add_parser(
        "sync-retrosheet-contexts",
        help="Build compact former-team and future-team opponent context aggregates from Retrosheet batting logs",
    )
    retrosheet_context_parser.add_argument("--retrosheet-dir", type=Path)
    retrosheet_context_parser.add_argument("--chunk-size", type=int, default=250000)

    retrosheet_pitcher_cohort_parser = subparsers.add_parser(
        "sync-retrosheet-pitcher-cohorts",
        help="Build compact hitter-vs-opponent-pitcher cohort aggregates from Retrosheet plays",
    )
    retrosheet_pitcher_cohort_parser.add_argument("--retrosheet-dir", type=Path)
    retrosheet_pitcher_cohort_parser.add_argument("--chunk-size", type=int, default=250000)

    retrosheet_streak_parser = subparsers.add_parser(
        "sync-retrosheet-streaks",
        help="Build compact historical streak records from Retrosheet plays and batting logs",
    )
    retrosheet_streak_parser.add_argument("--retrosheet-dir", type=Path)
    retrosheet_streak_parser.add_argument("--chunk-size", type=int, default=250000)

    retrosheet_play_warehouse_parser = subparsers.add_parser(
        "sync-retrosheet-play-warehouse",
        help="Import full raw Retrosheet plays.csv into SQLite for verbose historical event research",
    )
    retrosheet_play_warehouse_parser.add_argument("--retrosheet-dir", type=Path)
    retrosheet_play_warehouse_parser.add_argument("--batch-size", type=int, default=5000)

    ask_parser = subparsers.add_parser("ask", help="Ask one question")
    ask_parser.add_argument("question", type=str)
    ask_parser.add_argument("--session-id", type=str)

    serve_parser = subparsers.add_parser("serve", help="Run the web API")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", default=8000, type=int)
    serve_parser.add_argument("--reload", action="store_true")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    settings = Settings.from_env()

    if args.command == "prepare":
        result = bootstrap_datasets(
            settings,
            include_retrosheet_plays=(
                args.include_retrosheet_plays
                or args.with_retrosheet_splits
                or args.with_retrosheet_counts
                or args.with_retrosheet_streaks
                or args.with_retrosheet_play_warehouse
            ),
        )
        messages = ingest_project_data(
            settings,
            lahman_dir=result.lahman_dir,
            retrosheet_dir=result.retrosheet_dir,
            sabr_dir=args.sabr_dir,
            include_drs=args.with_drs,
            drs_start_season=args.drs_start_season,
            drs_end_season=args.drs_end_season,
            snapshot_current_drs=args.snapshot_current_drs,
            include_statcast=args.with_statcast,
            statcast_start_season=args.statcast_start_season,
            statcast_end_season=args.statcast_end_season,
            statcast_start_date=args.statcast_start_date,
            statcast_end_date=args.statcast_end_date,
            statcast_chunk_days=args.statcast_chunk_days,
            include_retrosheet_splits=args.with_retrosheet_splits,
            retrosheet_split_chunk_size=args.retrosheet_split_chunk_size,
            include_retrosheet_counts=args.with_retrosheet_counts,
            retrosheet_count_chunk_size=args.retrosheet_count_chunk_size,
            include_retrosheet_contexts=args.with_retrosheet_contexts,
            retrosheet_context_chunk_size=args.retrosheet_context_chunk_size,
            include_retrosheet_streaks=args.with_retrosheet_streaks,
            retrosheet_streak_chunk_size=args.retrosheet_streak_chunk_size,
            include_retrosheet_play_warehouse=args.with_retrosheet_play_warehouse,
            retrosheet_play_batch_size=args.retrosheet_play_batch_size,
        )
        for message in messages:
            print(message)
        return 0

    if args.command == "bootstrap":
        result = bootstrap_datasets(settings, include_retrosheet_plays=args.include_retrosheet_plays)
        print(f"Lahman data downloaded to {result.lahman_dir}")
        print(f"Retrosheet data downloaded to {result.retrosheet_dir}")
        return 0

    if args.command == "ingest":
        messages = ingest_project_data(
            settings,
            lahman_dir=args.lahman_dir or settings.raw_data_dir / "lahman",
            retrosheet_dir=args.retrosheet_dir or settings.raw_data_dir / "retrosheet",
            sabr_dir=args.sabr_dir or settings.sabr_docs_dir,
            include_drs=args.with_drs,
            drs_start_season=args.drs_start_season,
            drs_end_season=args.drs_end_season,
            snapshot_current_drs=args.snapshot_current_drs,
            include_statcast=args.with_statcast,
            statcast_start_season=args.statcast_start_season,
            statcast_end_season=args.statcast_end_season,
            statcast_start_date=args.statcast_start_date,
            statcast_end_date=args.statcast_end_date,
            statcast_chunk_days=args.statcast_chunk_days,
            include_retrosheet_splits=args.with_retrosheet_splits,
            retrosheet_split_chunk_size=args.retrosheet_split_chunk_size,
            include_retrosheet_counts=args.with_retrosheet_counts,
            retrosheet_count_chunk_size=args.retrosheet_count_chunk_size,
            include_retrosheet_contexts=args.with_retrosheet_contexts,
            retrosheet_context_chunk_size=args.retrosheet_context_chunk_size,
            include_retrosheet_streaks=args.with_retrosheet_streaks,
            retrosheet_streak_chunk_size=args.retrosheet_streak_chunk_size,
            include_retrosheet_play_warehouse=args.with_retrosheet_play_warehouse,
            retrosheet_play_batch_size=args.retrosheet_play_batch_size,
        )
        for message in messages:
            print(message)
        return 0

    if args.command == "sync-drs":
        messages = sync_fielding_bible_data(
            settings,
            start_season=args.start_season,
            end_season=args.end_season,
            snapshot_current=args.snapshot_current,
        )
        for message in messages:
            print(message)
        return 0

    if args.command == "snapshot-drs":
        messages = snapshot_current_drs_leaderboards(settings, season=args.season)
        for message in messages:
            print(message)
        return 0

    if args.command == "sync-statcast":
        messages = sync_statcast_data(
            settings,
            start_season=args.start_season,
            end_season=args.end_season,
            start_date=args.start_date,
            end_date=args.end_date,
            chunk_days=args.chunk_days,
            daily=args.daily,
            backfill_days=args.backfill_days,
        )
        for message in messages:
            print(message)
        return 0

    if args.command == "import-statcast-history":
        if not args.batter_csv and not args.pitcher_csv:
            raise SystemExit("Provide --batter-csv and/or --pitcher-csv")
        connection = get_connection(settings.database_path)
        try:
            messages = import_statcast_history_exports(
                connection,
                batter_csv=args.batter_csv,
                pitcher_csv=args.pitcher_csv,
            )
        finally:
            connection.close()
        for message in messages:
            print(message)
        return 0

    if args.command == "import-bundled-statcast-history":
        data_dir = args.data_dir or (settings.project_root / "data" / "statcast_history")
        batter_csv = data_dir / "Batter_Stats_Statcast_History.csv"
        pitcher_csv = data_dir / "Pitcher_Stats_Statcast_History.csv"
        if not batter_csv.exists() and not pitcher_csv.exists():
            raise SystemExit(f"No bundled Statcast history CSVs found in {data_dir}")
        connection = get_connection(settings.database_path)
        try:
            messages = import_statcast_history_exports(
                connection,
                batter_csv=batter_csv if batter_csv.exists() else None,
                pitcher_csv=pitcher_csv if pitcher_csv.exists() else None,
            )
        finally:
            connection.close()
        for message in messages:
            print(message)
        return 0

    if args.command == "refresh-bundled-statcast-history":
        data_dir = args.data_dir or (settings.project_root / "data" / "statcast_history")
        season = args.season or settings.live_season or datetime.now().year
        messages = refresh_bundled_statcast_history(
            data_dir=data_dir,
            user_agent=settings.user_agent,
            current_season=season,
            full_history=args.full_history,
        )
        for message in messages:
            print(message)
        return 0

    if args.command == "refresh-statcast-daily":
        messages = sync_statcast_data(
            settings,
            chunk_days=args.chunk_days,
            daily=True,
            backfill_days=args.backfill_days,
        )
        if not args.skip_history:
            data_dir = args.history_dir or (settings.project_root / "data" / "statcast_history")
            if args.fetch_history_from_savant:
                season = args.history_season or settings.live_season or datetime.now().year
                messages.extend(
                    refresh_bundled_statcast_history(
                        data_dir=data_dir,
                        user_agent=settings.user_agent,
                        current_season=season,
                        full_history=args.full_history_fetch,
                    )
                )
            batter_csv = data_dir / "Batter_Stats_Statcast_History.csv"
            pitcher_csv = data_dir / "Pitcher_Stats_Statcast_History.csv"
            if batter_csv.exists() or pitcher_csv.exists():
                connection = get_connection(settings.database_path)
                try:
                    messages.extend(
                        import_statcast_history_exports(
                            connection,
                            batter_csv=batter_csv if batter_csv.exists() else None,
                            pitcher_csv=pitcher_csv if pitcher_csv.exists() else None,
                        )
                    )
                finally:
                    connection.close()
            else:
                messages.append(
                    f"Skipped bundled Statcast history import: no CSVs found in {data_dir}."
                )
        for message in messages:
            print(message)
        return 0

    if args.command == "audit-statcast-history":
        selected_tables: list[tuple[str, str]] = []
        if args.role in {"batter", "both"}:
            selected_tables.append(("batter", STATCAST_HISTORY_BATTER_TABLE))
        if args.role in {"pitcher", "both"}:
            selected_tables.append(("pitcher", STATCAST_HISTORY_PITCHER_TABLE))
        connection = get_connection(settings.database_path)
        try:
            for role_label, table_name in selected_tables:
                audit = audit_statcast_history_table(
                    connection,
                    table_name,
                    player_name=args.player,
                )
                print(f"[{role_label}] {table_name}")
                if not audit["exists"]:
                    print("  table missing")
                    continue
                print(f"  total rows: {audit['row_count']}")
                if audit["year_counts"]:
                    year_summary = ", ".join(
                        f"{row['season']}: {row['row_count']}" for row in audit["year_counts"]
                    )
                    print(f"  rows by year: {year_summary}")
                else:
                    print("  rows by year: none")
                if args.player:
                    matches = audit["player_matches"]
                    if not matches:
                        print(f"  player matches: none for {args.player}")
                    else:
                        for match in matches:
                            seasons = ", ".join(str(season) for season in match["seasons"])
                            print(
                                f"  player: {match['player_name']} | seasons: {seasons} | "
                                f"first={match['first_season']} last={match['last_season']}"
                            )
                            if match["missing_between"]:
                                missing = ", ".join(str(season) for season in match["missing_between"])
                                print(f"    missing between first/last: {missing}")
        finally:
            connection.close()
        return 0

    if args.command == "sync-retrosheet-splits":
        messages = sync_retrosheet_team_splits(
            settings,
            retrosheet_dir=args.retrosheet_dir or settings.raw_data_dir / "retrosheet",
            chunk_size=args.chunk_size,
        )
        for message in messages:
            print(message)
        return 0

    if args.command == "sync-retrosheet-counts":
        messages = sync_retrosheet_player_count_splits(
            settings,
            retrosheet_dir=args.retrosheet_dir or settings.raw_data_dir / "retrosheet",
            chunk_size=args.chunk_size,
        )
        for message in messages:
            print(message)
        return 0

    if args.command == "sync-retrosheet-contexts":
        messages = sync_retrosheet_player_opponent_contexts(
            settings,
            retrosheet_dir=args.retrosheet_dir or settings.raw_data_dir / "retrosheet",
            chunk_size=args.chunk_size,
        )
        for message in messages:
            print(message)
        return 0

    if args.command == "sync-retrosheet-pitcher-cohorts":
        messages = sync_retrosheet_player_opponent_pitcher_cohorts(
            settings,
            retrosheet_dir=args.retrosheet_dir or settings.raw_data_dir / "retrosheet",
            chunk_size=args.chunk_size,
        )
        for message in messages:
            print(message)
        return 0

    if args.command == "sync-retrosheet-streaks":
        messages = sync_retrosheet_player_streaks(
            settings,
            retrosheet_dir=args.retrosheet_dir or settings.raw_data_dir / "retrosheet",
            chunk_size=args.chunk_size,
        )
        for message in messages:
            print(message)
        return 0

    if args.command == "sync-retrosheet-play-warehouse":
        messages = sync_retrosheet_play_warehouse(
            settings,
            retrosheet_dir=args.retrosheet_dir or settings.raw_data_dir / "retrosheet",
            batch_size=args.batch_size,
        )
        for message in messages:
            print(message)
        connection = get_connection(settings.database_path)
        try:
            if table_exists(connection, RETROSHEET_PLAYS_TABLE):
                row_count = get_metadata_value(connection, "retrosheet_play_warehouse_rows") or "0"
                imported_at = get_metadata_value(connection, "retrosheet_play_warehouse_imported_at") or ""
                print(f"{RETROSHEET_PLAYS_TABLE}: {row_count} rows")
                if imported_at:
                    print(f"imported_at: {imported_at}")
        finally:
            connection.close()
        return 0

    if args.command == "ask":
        bot = BaseballChatbot(settings)
        result = bot.answer(args.question, session_id=args.session_id)
        print(result.answer)
        return 0

    if args.command == "serve":
        try:
            import uvicorn
        except ImportError as exc:
            raise SystemExit("uvicorn is required for the serve command") from exc
        uvicorn.run("mlb_history_bot.api:app", host=args.host, port=args.port, reload=args.reload)
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
