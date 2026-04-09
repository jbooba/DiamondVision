from __future__ import annotations

import argparse
import sys
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
from .retrosheet_streaks import sync_retrosheet_player_streaks
from .retrosheet_splits import sync_retrosheet_team_splits
from .statcast_sync import sync_statcast_data


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
