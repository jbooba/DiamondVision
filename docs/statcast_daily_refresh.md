# Daily Statcast Refresh Runbook

This project now has two different Statcast layers that need different refresh strategies:

1. local daily Statcast warehouse
   - `statcast_team_games`
   - `statcast_pitcher_games`
   - `statcast_batter_games`
   - `statcast_pitch_type_games`
   - `statcast_batter_pitch_type_games`
   - `statcast_events`

2. imported custom leaderboard history tables
   - `statcast_history_batter_seasons`
   - `statcast_history_pitcher_seasons`

The important distinction is:

- `sync-statcast` keeps the local event/summary warehouse fresh
- `import-statcast-history` keeps the imported season-history snapshots fresh

## New One-Command Daily Refresh

The recommended daily command is:

```bash
python -m mlb_history_bot refresh-statcast-daily
```

What it does:

1. runs the local Statcast daily sync with a small rolling backfill window
2. re-imports the bundled custom-history CSVs from `data/statcast_history/` if they exist

If you also want the bundled custom-history CSVs refreshed directly from Baseball Savant before import, use:

```bash
python -m mlb_history_bot refresh-statcast-daily --fetch-history-from-savant
```

That mode fetches only the current live season by default and merges those rows into the existing bundled history files, which keeps the daily refresh fast.

Default behavior:

- `chunk-days=3`
- `backfill-days=3`
- imports:
  - `data/statcast_history/Batter_Stats_Statcast_History.csv`
  - `data/statcast_history/Pitcher_Stats_Statcast_History.csv`

Useful variants:

```bash
python -m mlb_history_bot refresh-statcast-daily --skip-history
```

```bash
python -m mlb_history_bot refresh-statcast-daily --history-dir /path/to/fresh/exports
```

```bash
python -m mlb_history_bot refresh-statcast-daily --backfill-days 5 --chunk-days 2
```

## What You Need To Update Daily

The local warehouse can self-refresh from public Statcast data.

The imported custom-history tables are still snapshots, but the repo can now refresh those bundled CSVs directly from Savant's custom leaderboard pages.

Daily workflow:

1. refresh the bundled custom-history CSVs for the live season:

```bash
python -m mlb_history_bot refresh-bundled-statcast-history
```

2. then run:

```bash
python -m mlb_history_bot refresh-statcast-daily
```

For a one-time full-history rebuild of the bundled CSVs, run:

```bash
python -m mlb_history_bot refresh-bundled-statcast-history --full-history
```

## Railway Workflow

If Railway is the live deployment target:

1. make sure the newest CSVs are in the deployed app at `/app/data/statcast_history/`
2. open a Railway shell
3. run:

```bash
python -m mlb_history_bot refresh-statcast-daily
```

If you want Railway to pull the latest live-season custom-history rows itself before import:

```bash
python -m mlb_history_bot refresh-statcast-daily --fetch-history-from-savant
```

If you only want to refresh the imported history after a CSV update:

```bash
python -m mlb_history_bot import-bundled-statcast-history
```

If you only want the local rolling Statcast warehouse refresh:

```bash
python -m mlb_history_bot sync-statcast --daily --backfill-days 3 --chunk-days 3
```

## Best Long-Term Operating Model

For the broadest daily freshness:

1. keep the local Statcast warehouse on a daily rolling sync
2. refresh the bundled Savant custom-history CSVs once per day for the live season
3. re-import those refreshed CSVs immediately after they land

That gives you:

- fresh live/event/park/pitch-type coverage from the local Statcast warehouse
- fresh season-level leaderboard coverage for the `509` imported Statcast metrics

The expensive full-history bundled refresh should only be needed occasionally. Daily maintenance can stay on the live season only.

## Failure Modes To Watch

- If imported-history queries start returning source gaps again, check whether the history tables still exist:

```bash
python - <<'PY'
import sqlite3
con = sqlite3.connect('/data/processed/mlb_history.sqlite3')
for table in ['statcast_history_batter_seasons', 'statcast_history_pitcher_seasons']:
    try:
        print(table, con.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0])
    except Exception as exc:
        print(table, 'missing', exc)
con.close()
PY
```

- If live/today/yesterday Statcast answers look stale, the local warehouse likely needs the rolling `sync-statcast` refresh.

## Recommendation

Use `refresh-statcast-daily` as the standard maintenance command, and treat fresh custom-history CSV exports as part of the daily data pipeline rather than an occasional manual repair step.
