# MLB History Chatbot

Separate project for an MLB-only chatbot that blends:

- Historical/statistical grounding from Lahman and Retrosheet
- Exact season-level DRS leaderboards from Fielding Bible / Sports Info Solutions
- Public Statcast team-game aggregates for expected-metric and batted-ball window queries
- Public Statcast pitcher-game relationship aggregates for fast threshold-count and pitch-type strikeout queries
- Retrosheet play-by-play situational split aggregates for questions like BA/OBP/SLG/OPS with RISP through a game window
- Sabermetric definitions and formula notes from a curated baseball-only catalog
- Clip-aware play lookup using Baseball Savant sporty-video pages plus MLB game feeds
- Optional SABR/local document search
- Live/current answers from the MLB Stats API and optional `pybaseball` Statcast helpers

## What It Is

This is not a general chatbot with a baseball skin. The design goal is to keep answers constrained to MLB evidence:

- historical tables loaded into SQLite
- official season-level DRS tables and current leaderboard snapshots from Fielding Bible / SIS
- a curated sabermetric catalog
- optional local SABR PDFs/text
- current live standings/schedule/player snapshots from `statsapi.mlb.com`
- replay cards that can surface matching Savant clips for player/date/play-style questions

That means the bot can answer cleanly when it has support, and it should say so when it does not. For metrics like DRS, WAR, or UZR where provider methodology or direct values matter, the bot is set up to acknowledge source limits instead of pretending it can derive exact numbers from public box-score data alone.

## Current Scope

The project currently supports:

- Lahman CSV bootstrap from SABR's current Box share
- Retrosheet main CSV bootstrap from the official download zip
- SQLite ingestion of CSV datasets
- Exact Fielding Bible / SIS player and team DRS ingestion by season
- Public Statcast team-game sync for xBA, xwOBA, xSLG, Hard-Hit Rate, Barrel Rate, and related window research
- Timestamped snapshots of the current Fielding Bible / SIS DRS leaderboard
- Optional indexing of local SABR/text/PDF documents
- Sabermetric glossary lookups
- FanGraphs/DRS component glossary aliases such as `rPM`, `rARM`, `rSB`, `rGDP`, `rBU`, `rHR`, and `Def`
- Expanded FanGraphs-style public metric coverage through `pybaseball`, including `Clutch`, `RE24`, `LOB%`, `tERA`, `WPA`, `Off`, `Bat`, `Fld`, `RAR`, and many other batting/pitching leaderboard columns
- Historical player and team summaries from Lahman
- Historical team-fact lookups from Lahman for prompts like `Who was the Mets manager in 2023?`
- Retrosheet single-game leaderboard lookups for basic supported stats
- Evaluative current-team analysis for prompts like `How bad is the current Giants roster?`
- Current-vs-prior-season team comparison for prompts like `How is the Mets season looking so far compared to last year?`
- Cross-era team-season comparison for prompts like `Compare the 2004 Expos to the 2026 Giants through the first 10 games of their seasons`
- Historical and current DRS leaderboard lookups for infielders, outfielders, catchers, pitchers, and teams
- A clearly labeled `rHR` proxy for date-specific home-run robberies using Baseball Savant play pages plus a public SIS 1.6-run valuation note
- General replay-aware lookup for player/date questions, with relevance-ranked Savant clip cards and plain-English explanations of why each clip matches
- Source-gap planning for unsupported Statcast-style leaderboard requests so complex metric questions fail honestly instead of falling into unrelated live snippets
- Synced Statcast team-window answers for prompts like `What team has the worst xBA through the first 10 games of a season?`
- Fast local Statcast aggregate answers for prompts like `Who threw the most pitches over 100mph last year?` and `Which pitcher has the most strikeouts throwing changeups this year?`
- Historical situational team-window answers for prompts like `What team had the worst BA with RISP in the first ten games of a season?`
- Historical player count-state answers for prompts like `Who has the lowest batting average on 3-0 counts?` and `Who has the highest batting average after 0-2 counts?`
- Historical former-team and future-team matchup answers for prompts like `Who has the most home runs against their former team?`
- Yearless month-day historical answers for prompts like `How many home runs were hit on August 1st?` and `Which pitcher had the best historical line on June 27th?`
- Live scoreboard, standings, and player season snapshots from the MLB Stats API
- Optional `pybaseball` helpers for Statcast, team logs, and Lahman metadata fallback surfaces
- CLI plus a retro-styled FastAPI web app ("DiamondVision 64")
- A query-family planning matrix in [`docs/query_coverage_matrix.md`](docs/query_coverage_matrix.md) for systematic expansion across routine, comparative, contextual, Statcast, salary, historical, and replay-driven prompts

## Why This Approach

For this use case, retrieval over curated baseball datasets is more useful than fine-tuning on raw tables:

- the source set stays baseball-only
- historical data can be updated locally
- live data stays live
- the assistant can cite exactly which source family supported the answer

## Setup

```powershell
cd C:\Users\jesse\Documents\Playground\mlb_history_chatbot
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Set `OPENAI_API_KEY` in `.env` if you want full natural-language answers. Without it, the bot still returns grounded fallback summaries from the retrieved context.

## Prepare The Data

Bootstrap Lahman and Retrosheet, then ingest them:

```powershell
python -m mlb_history_bot prepare
```

To include exact Fielding Bible / SIS DRS seasons during the same setup:

```powershell
python -m mlb_history_bot prepare --with-drs
```

To include a Statcast sync during setup:

```powershell
python -m mlb_history_bot prepare --with-statcast --statcast-start-season 2025 --statcast-end-season 2026
```

To also build compact Retrosheet situational split tables from `plays.csv` during setup:

```powershell
python -m mlb_history_bot prepare --with-retrosheet-splits
```

If you also want Retrosheet `plays.csv`, include it explicitly:

```powershell
python -m mlb_history_bot prepare --include-retrosheet-plays
```

That file is used for the derived split/count/context builders, not for a persisted
`retrosheet_plays` SQLite table. Base `ingest` intentionally skips importing raw
`plays.csv` into the runtime database to avoid huge SQLite volume growth during setup.

To ingest local SABR or other baseball research docs that you already have:

1. Put them in `data\raw\sabr\`
2. Run:

```powershell
python -m mlb_history_bot ingest
```

To sync or refresh exact DRS data separately:

```powershell
python -m mlb_history_bot sync-drs --start-season 2003 --end-season 2026
python -m mlb_history_bot snapshot-drs
```

To sync public Statcast team-game aggregates separately:

```powershell
python -m mlb_history_bot sync-statcast --start-season 2025 --end-season 2026
python -m mlb_history_bot sync-statcast --start-date 2026-03-27 --end-date 2026-04-05 --chunk-days 3
```

`sync-statcast` now stores both team-game expected-metric rows and pitcher-game relationship summary rows. If you want older heavy Statcast aggregate queries to become instant locally, rerun `sync-statcast` for the seasons you care about so those pitcher summaries are backfilled too.

For a lightweight daily refresh of the current season with a small overlap for corrections:

```powershell
python -m mlb_history_bot sync-statcast --daily --backfill-days 3 --chunk-days 3
```

To rebuild historical team situational split tables from Retrosheet `plays.csv` separately:

```powershell
python -m mlb_history_bot sync-retrosheet-splits --chunk-size 200000
```

To rebuild historical player count-state and former-team/future-team context tables separately:

```powershell
python -m mlb_history_bot sync-retrosheet-counts --chunk-size 200000
python -m mlb_history_bot sync-retrosheet-contexts --chunk-size 200000
```

## Ask Questions

One-off CLI:

```powershell
python -m mlb_history_bot ask "What is FIP and how is it calculated?"
python -m mlb_history_bot ask "Who were the 1954 Cleveland Indians and how good were they?"
python -m mlb_history_bot ask "What happened in MLB today?"
python -m mlb_history_bot ask "Which infielders led DRS in 2013?"
python -m mlb_history_bot ask "Who leads MLB in DRS this season?"
python -m mlb_history_bot ask "What is rARM?"
python -m mlb_history_bot ask "Who leads MLB in rARM this season?"
python -m mlb_history_bot ask "What was Adolis Garcia's rHR on July 29, 2025?"
python -m mlb_history_bot ask "Show me Mike Trout clips from July 29, 2025."
python -m mlb_history_bot ask "Show me Jo Adell's defensive clips tonight."
python -m mlb_history_bot ask "How bad is the current Giants roster?"
python -m mlb_history_bot ask "Are the current Dodgers legit?"
python -m mlb_history_bot ask "How is the Mets season looking so far compared to last year?"
python -m mlb_history_bot ask "What team has the worst xBA through the first 10 games of a season?"
python -m mlb_history_bot ask "What team has the highest hard-hit rate through the first 10 games of a season?"
python -m mlb_history_bot ask "What team had the worst BA with RISP in the first ten games of a season?"
python -m mlb_history_bot ask "Who has the lowest batting average on 3-0 counts?"
python -m mlb_history_bot ask "Who has the highest batting average after 0-2 counts?"
python -m mlb_history_bot ask "Who has the most home runs against their former team?"
python -m mlb_history_bot ask "How many home runs were hit on August 1st?"
python -m mlb_history_bot ask "Which pitcher has the best stats on June 27th?"
python -m mlb_history_bot ask "Who led MLB in tERA in 2025?"
python -m mlb_history_bot ask "Who led MLB in Clutch in 2025?"
python -m mlb_history_bot ask "Compare the 2004 Expos to the 2026 Giants through the first 10 games of their seasons"
```

Start the local web app:

```powershell
python -m mlb_history_bot serve --host 127.0.0.1 --port 8000
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000).

The current web UI is intentionally styled like a late-90s baseball console: research feed on the left, replay vault on the right, and separate evidence tabs for glossary, historical data, replay evidence, and live context.

## Railway Deployment

This repo is set up to deploy on Railway with the included `Dockerfile`, `railway.json`, and `scripts/railway_start.sh`.

Recommended Railway setup:

1. Create a volume and mount it at `/data`.
2. Set `OPENAI_API_KEY`.
3. Optionally set `MLB_HISTORY_LIVE_SEASON` if you want to pin the current live season.
4. Leave the defaults alone for:
   `MLB_HISTORY_RAW_DATA_DIR=/data/raw`
   `MLB_HISTORY_PROCESSED_DIR=/data/processed`
   `MLB_HISTORY_DATABASE_PATH=/data/processed/mlb_history.sqlite3`

The web service will boot without a database, but historical queries will be limited until the mounted volume is populated.

For external testing, the cleanest one-time prep flow is:

```bash
python -m mlb_history_bot prepare --with-drs --with-statcast --statcast-start-season 2025 --statcast-end-season 2026 --with-retrosheet-splits --with-retrosheet-counts --with-retrosheet-contexts
```

If you want to keep startup fast, run that as a one-off Railway shell/task against the mounted volume rather than during normal web boot.

## Important Limitations

- Exact season-level DRS can be imported from Fielding Bible / SIS, but exact single-game DRS is still not publicly exposed there.
- Statcast window answers only cover the Statcast seasons you have actually synced locally; pre-2015 support is naturally unavailable.
- Exact `rHR` is still not exposed in the synced public SIS leaderboard feed; the bot uses a clearly labeled proxy only for date-specific robbery questions.
- Replay support depends on public Baseball Savant sporty-video pages existing for the play IDs we can recover from MLB game feeds; not every play has a public clip page.
- The current single-game leaderboard support is intentionally narrow and tied to the columns present in imported Retrosheet tables.
- Advanced metrics like WAR, wRC+, UZR, and provider-specific DRS values are best treated as sourced outputs, not reverse-engineered box-score formulas.
- For proprietary or provider-owned stats, the bot is supposed to say "I don't have the exact source for that" rather than guess.
- FanGraphs can be a helpful explanatory cross-check for DRS terminology, but the exact leaderboard values in this project come from Fielding Bible / SIS syncs and snapshots.

## Good Next Steps

- Add user-supplied Baseball-Reference or StatMuse export ingestion for advanced metric leaderboards
- Expand the stat formula catalog with park-adjusted constants and era-specific notes
- Add richer Retrosheet play-by-play support for WPA/RE24/expectancy workflows
- Experiment with DRS proxy models and compare them against imported provider outputs and stored current-leaderboard snapshots
