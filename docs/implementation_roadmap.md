# DiamondVision Relationship Roadmap

This roadmap is for turning DiamondVision from a large collection of MLB query
handlers into a more unified baseball relationship engine.

The design goal is:

1. Parse the relationship shape.
2. Resolve the entities, cohorts, and time scope.
3. Route to deterministic data layers.
4. Compute the answer.
5. Explain why it matters.
6. Attach clips or narrative context when relevant.

## Guiding Principle

The model should not be the primary source of baseball facts.

- The database and sync layers should hold the facts.
- Shared query ontology helpers should normalize the user intent.
- The LLM should eventually help with planning, ambiguity resolution, and
  response synthesis only when the rules-based path is weak.

## Phase 1: Shared Relationship Grammar

Status: started

Goal:

- centralize common query dimensions that are currently scattered across many
  modules

Core dimensions:

- ranking intent: `best`, `worst`, `highest`, `lowest`, `most`, `fewest`
- subject role: hitter, batter, pitcher, starter, reliever, fielder, defender,
  catcher, manager, team, roster, franchise
- frame: offense, pitching, defense, overall, salary, replay, anomaly
- time scope: today, tonight, yesterday, last night, this week, last week, this
  season, right now, season-to-date, exact date, yearless calendar date, career
- entity scope: player, team, roster, park, country, award cohort, manager era
- comparison mode: lookup, leaderboard, comparison, evaluation, narrative

Immediate deliverables:

- shared query ontology helpers
- current-team state routing
- current-team roster-leader routing
- broader current-season phrasing support such as `how are the Giants doing so far`

Unlocked families:

- `how are the Giants doing so far`
- `who is the best hitter on the Mets right now`
- `who is the best pitcher on the Dodgers this year`
- `who is the worst defender on the Yankees right now`

## Phase 2: Cohort And Timeline Warehouse

Goal:

- make entity relationships reusable instead of ad hoc

Data layers:

- manager tenures by team-season
- award winners by season and award
- country-of-birth cohorts
- player-team tenure windows
- franchise and roster membership history
- team aliases and park aliases

Unlocked families:

- `best offensive player for the Mets under Buck Showalter`
- `best OPS against Cy Young winners`
- `most home runs against a former team`
- `best Dominican-born slugger by salary efficiency`

## Phase 3: Situation Engine

Goal:

- normalize baseball context questions into reusable filters

Core contexts:

- count state
- reached-count state
- base state
- base-out state
- inning
- home/away
- handedness
- starter vs reliever
- score differential bucket
- leverage bucket
- regular season vs postseason

Unlocked families:

- `lowest batting average after 0-2 counts`
- `best OPS with RISP and two outs`
- `most strikeouts by relievers in one-run games`
- `best offense against lefties late in games`

## Phase 4: Statcast Relationship Warehouse

Goal:

- make filtered Statcast research cheap and reusable

Core summary layers:

- batter-game summaries
- pitcher-game summaries
- pitch-type summaries
- batter-vs-pitch-type summaries
- compact event index
- location/direction buckets
- park-era buckets
- anomaly flags

Unlocked families:

- `highest EV home runs to right field at Oracle Park`
- `show me base hits on middle middle fastballs`
- `who threw the most sliders last year`
- `show me Pete Alonso home runs off curveballs`

## Phase 5: Provider Snapshot Warehouse

Goal:

- stop depending on fragile live scraping for high-value provider metrics

Provider targets:

- Fangraphs leaderboard snapshots
- Baseball Reference seasonal snapshot tables
- advanced model-backed metrics like Stuff+, Location+, Pitching+, WAR snapshots

Unlocked families:

- `which Orioles pitcher has the best Stuff+ with a minimum of 35 starts`
- `who led MLB in Pitching+ in 2025`
- `how does this year’s WAR leader compare at the same point historically`

## Phase 6: Economic And Transaction Layer

Goal:

- support value, contract, and transaction relationships

Data layers:

- public salary and earnings history
- richer contract sources when available
- transaction history and team-change windows
- draft and prospect metadata

Unlocked families:

- `how much is Trout making per hit`
- `which Dominican-born player has the highest career earnings`
- `who performed worst against a team they were later signed by`

## Phase 7: Anomaly And Narrative Layer

Goal:

- make abstract prompts deterministic enough to trust

Scoring layers:

- weirdness
- coolness / highlight score
- defensive brilliance
- offensive explosion
- pitching dominance
- historical rarity

Unlocked families:

- `did anything weird happen this week`
- `what were the sickest plays last night`
- `who had the best defensive performance yesterday`

## Phase 8: Agent Planner Layer

Goal:

- use the model only where the deterministic stack is weakest

Responsibilities:

- interpret ambiguous natural language
- choose the best internal tools
- request tables, comparisons, clips, and historical framing
- synthesize the final answer when multiple deterministic layers contribute

Non-goals:

- store baseball facts in model weights
- replace deterministic DB-backed answers with free-form generation

## Recommended Build Order

1. Shared relationship grammar
2. Cohort and timeline warehouse
3. Situation engine
4. Statcast relationship warehouse
5. Provider snapshot warehouse
6. Economic and transaction layer
7. Anomaly and narrative layer
8. Agent planner layer

## Current Workstream

Current priority:

- broaden season-aware leaderboard routing so historical Lahman data,
  local Statcast summaries, and provider-backed advanced season metrics
  can all answer through one shared highest/lowest grammar

Implemented in this pass:

- generalized season metric leaderboard layer for historical Lahman player/team
  seasons
- local Statcast season leaderboards for synced batter/team metrics
- provider-backed season metric bridge for advanced stats such as
  `FIP`, `xFIP`, `SIERA`, `wRC+`, and other Fangraphs season leaderboard metrics
- source cascading so compatible questions can widen from historical -> Statcast
  -> provider instead of dying on the first source-family miss
- DB migration fix for new Statcast event columns on existing databases

Implemented since then:

- reusable cohort/timeline leaderboard layer for:
  - manager eras
  - country-of-birth cohorts
  - shared `best / worst / highest / lowest` metric routing across those cohorts
- generalized Statcast event-set leaderboard layer for:
  - event-set + metric + aggregation + qualifier queries
  - exact-day event leaderboards such as `highest EV hits from 07/23/17`
  - player-level aggregate event questions such as
    `lowest average home run distance with at least 10 HR`
  - better bounded raw fallback for narrow date windows when the local compact
    event index is missing a window

Tester-derived failure classes now tracked explicitly:

- aggregate vs single-opponent relationships
  - example shape: `across all former teams` vs `against one former team`
- cohort joins
  - example shape: `against Cy Young winners`, `on an opponent's birthday`
- set refinement over a prior answer
  - example shape: `eliminate anyone hitting .000 from the pool`
- career/event aggregates with minimum qualifiers
  - example shape: `lowest average home run distance with at least 10 HR`
- event-set leaderboards with many orthogonal filters
  - example shape: date + EV + event type + park + direction + pitch type + count
- event-set aggregation and qualifier grammar
  - example shape: `player average home run distance with at least 10 HR`
- exact historical slash-date queries with short years
  - example shape: `from 07/23/17`
- same metric grammar across historical, synced local modern, and provider-backed
  season data

Why this first:

- high user-visible value
- low sync cost
- unlocks a broad class of natural MLB questions
- reduces repeated parser drift across team-facing modules
