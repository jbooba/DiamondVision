# Retrosheet Play Warehouse

DiamondVision now supports an explicit full-play Retrosheet warehouse import.

This is separate from the default `ingest` flow on purpose:

- default `ingest` still skips `plays.csv`
- the raw play warehouse is opt-in
- derived compact tables like streaks, count splits, and opponent cohorts still work the same way

## Why this exists

The compact Retrosheet summary tables are great for speed, but they are too narrow for some open-ended historical questions. Importing the full `plays.csv` file into SQLite gives the app a durable event warehouse it can build future comparisons and conditions from without having to restage the raw file every time.

## Commands

One-time direct import:

```bash
python -m mlb_history_bot sync-retrosheet-play-warehouse --retrosheet-dir /tmp/mlb_raw/retrosheet
```

Opt-in during `prepare`:

```bash
python -m mlb_history_bot prepare --with-retrosheet-play-warehouse
```

Opt-in during `ingest`:

```bash
python -m mlb_history_bot ingest --retrosheet-dir /tmp/mlb_raw/retrosheet --with-retrosheet-play-warehouse
```

## Health/debug

`/health` now reports:

- `retrosheet_play_warehouse_exists`
- `retrosheet_play_warehouse_rows`
- `retrosheet_play_warehouse_imported_at`

## Notes

- The import is intentionally explicit because the raw warehouse can be large.
- The app still uses the existing compact Retrosheet tables for many fast historical queries.
- This warehouse is the foundation for broader historical pitch-sequence, event-context, and arbitrary condition queries.
