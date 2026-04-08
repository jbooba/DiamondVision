# Query Coverage Matrix

This project is not aiming for a finite list of hardcoded questions. The real target is a combinatorial MLB query system that can compose:

- entity: player, pitcher, batter, fielder, catcher, manager, team, roster, franchise, league, park, country, award cohort
- time scope: today, tonight, yesterday, last night, this week, last week, this season, season to date, exact date, yearless calendar date, season, multi-season span, career, Statcast era, all history
- metric family: traditional stats, sabermetrics, Statcast metrics, public provider metrics, defensive metrics, salary/earnings, standings/record, replay/video evidence
- context filter: count, base-out state, RISP, park, pitch type, pitch location, handedness, opponent quality, former/future team, manager era, roster segment, first-N-games, same-point season start
- intent: lookup, leaderboard, comparison, evaluation, anomaly search, summary, explanation, clip search, historical framing

The coverage strategy should always be:

1. Parse the relationship shape, not just the exact words.
2. Resolve the entities and scope.
3. Route to the best factual source.
4. Compute or aggregate the answer.
5. Explain why it matters.
6. Attach clips when relevant.

## Core Query Families

| Family | Examples | Main sources |
| --- | --- | --- |
| Live team/game status | `did the Red Sox win today`, `how did the Yankees play last night` | MLB Stats API |
| Live player day/window stats | `did Stanton homer today`, `did Yordan get any hits today`, `how many RBI does Soto have this week` | MLB Stats API game feeds |
| Current season player analysis | `how has Cal Raleigh been so far this year`, `analyze Pete Alonso's 2026 season` | MLB Stats API |
| Current season team analysis | `how bad are the Giants`, `are the Dodgers legit`, `how is the Mets season looking so far compared to last year` | MLB Stats API + Lahman/Retrosheet context |
| Historical player/team lookups | `who was the Mets manager in 2023`, `how good were the 1979 Indians` | Lahman + Retrosheet |
| Historical date/day lookups | `how many home runs were hit on August 1st`, `which pitcher had the best stats on June 27th` | Retrosheet |
| Team window leaderboards | `lowest team BA through first 10 games`, `greatest xwOBA through first 10 games` | Retrosheet + Statcast team tables |
| Player same-point start comparisons | `compare Pete Alonso's OPS through the 2026 season with his previous season starts` | MLB Stats API logs |
| Player/player season comparisons | `compare Pete Alonso 2022 to Cal Raleigh 2025` | pybaseball + local context |
| Team/team season comparisons | `compare the 2004 Expos to the 2026 Giants through 10 games` | Retrosheet + MLB Stats API |
| Roster/staff comparisons | `compare the 1976 Giants pitching staff to the 2026 Giants pitching staff` | Lahman + MLB Stats API |
| Situational split history | `worst BA with RISP through first ten games`, `highest OBP after 0-2 counts` | Retrosheet split tables |
| Relationship history | `most home runs against a former team`, `best offensive player for the Mets under Buck Showalter` | Retrosheet + Lahman + pybaseball |
| Salary/earnings relationships | `highest-paid Dominican-born player`, `how much is Trout making per hit` | Lahman salaries |
| Provider metric leaderboards | `current WAR leader`, `highest Stuff+ with 35 starts`, `who led MLB in Clutch in 2025` | pybaseball / FanGraphs |
| Raw Statcast event relationships | `show me sliders over 2500 rpm`, `show me base hits on middle middle fastballs`, `Pete Alonso home runs off curveballs` | raw Statcast via pybaseball |
| Defensive metric lookups | `Pete Alonso OAA`, `who led DRS in 2013`, `show me Jo Adell home run robberies` | OAA / SIS / replay proxy |
| Highlight discovery | `coolest plays last night`, `weirdest things this week`, `best defensive plays yesterday` | MLB Stats API + Savant clips |
| Clip-backed research | `show me Pete Alonso clips from June 1, 2022`, `show me Jo Adell home run robberies` | Savant sporty video pages |

## Relationship Dimensions The Bot Should Support

These relationships should be treated as reusable dimensions, not one-off bespoke handlers.

### Entity-to-Entity

- player vs player
- player vs own prior seasons
- player vs own same-point season starts
- player vs team
- player vs opponent team
- player vs former team
- player vs future team
- player vs manager era
- player vs award-winner cohort
- pitcher vs batter
- batter vs pitch type
- batter vs pitch location
- batter vs count
- batter vs base state
- batter vs park
- team vs team
- team vs own franchise history
- roster vs roster
- pitching staff vs pitching staff
- manager vs franchise
- country-of-birth cohort vs league history

### Time Relationships

- exact date
- exact year
- yearless month/day across all history
- today / tonight
- yesterday / last night
- this week / last week
- this season / so far / to date / right now
- first N games
- pre/post event split
- under manager tenure
- before / after joining a team
- before / after winning an award
- career-to-date
- Statcast era
- all imported history

### Metric Relationships

- direct lookup: `what is X`
- leaderboard: `highest`, `lowest`, `most`, `fewest`, `greatest`, `weakest`
- comparison: `better than`, `worse than`, `how does`
- rate vs volume: `OPS` vs `HR`, `K%` vs `K`, `salary per hit`
- same metric across entities: `FIP vs xFIP`
- cross-metric framing: `production vs salary`, `defense vs record`, `stuff vs results`
- context-relative framing: league average, percentile, franchise rank, historical percentile, era rank

## Representative Query Grid

The parser surface should be able to interpret variants across each cell below.

### Routine

- `did the Red Sox win today`
- `how did the Yankees play last night`
- `did Stanton homer today`
- `did Yordan Alvarez get any hits today`
- `how has Cal Raleigh been so far this year`

### Historical

- `who was the Mets manager in 2023`
- `how many home runs were hit on August 1st`
- `which pitcher had the best stats on June 27th`
- `how good were the 1979 Cleveland Indians`
- `which teams have had similar starts to the 2026 Giants`

### Comparison

- `compare Pete Alonso 2022 to Cal Raleigh 2025`
- `were the 2004 Expos worse than the 2026 Giants`
- `compare the 1976 Giants pitching staff to the 2026 Giants pitching staff`
- `compare Pete Alonso's OPS through the 2026 season with his previous season starts`
- `compare Zack Wheeler FIP in 2024 to Jacob deGrom FIP in 2021`

### Contextual / Situational

- `highest OBP after 0-2 counts`
- `fewest strikeouts against a future team`
- `worst BA with RISP through the first ten games`
- `best offensive player for the Mets under Buck Showalter`
- `highest OPS against MVP winners`

### Statcast / Event Relationships

- `show me sliders with spin rates over 2500 rpm`
- `show me base hits on high fastballs`
- `show me base hits on middle middle fastballs`
- `show me Pete Alonso home runs off curveballs`
- `what are the highest exit velocity home runs to right field at Oracle Park in the Statcast era`

### Abstract / Narrative

- `how bad is the current Giants roster`
- `did anything sick happen last night`
- `what were the best defensive plays yesterday`
- `what weird or unusual things happened this week`
- `who was the best offensive player for the Mets under Buck Showalter`

### Economic / Biographical Relationships

- `which Dominican-born player has the highest career earnings`
- `how much money is Mike Trout making per hit`
- `who has made the most money among Japanese-born players`
- `analyze the relationship between Pete Alonso's salary and his offensive production`

## Efficient Implementation Pattern

To scale coverage efficiently, new query families should usually reuse these building blocks:

- shared ranking intent: `best / worst / highest / lowest / fewest / greatest`
- shared current-scope semantics: `today / this season / so far / right now`
- shared entity extraction: player names, teams, managers, parks, award cohorts
- shared date semantics: exact date, yearless calendar date, week windows, first-N-game windows
- shared evidence payloads: table, leaderboard, comparison rows, clips, historical percentile blurbs

This is much more maintainable than writing:

- one parser for `highest X`
- another parser for `greatest X`
- another parser for `most X`

The goal is a grammar of baseball relationships, not a bag of prompt templates.

## Major Remaining Expansion Targets

- generalized cohort joins and entity-set joins
  - award winners, nationality cohorts, manager eras, former/future teams,
    opponent birthdays, transaction windows
- generalized set refinement
  - follow-ups like `remove the .000 hitters`, `only among starters`,
    `minimum 10 HR`, `exclude current team`
- generalized season/career metric warehouse
  - one grammar for highest/lowest by player/team for historical Lahman,
    synced Statcast summaries, and provider-backed advanced season metrics
- generalized event-set leaderboards
  - one grammar for event-type + metric + park + direction + pitch family +
    count + date-window combinations
- broader base-out / score-state / leverage-context splits
- handedness and platoon relationships
- weather / rain-delay / resumed-game relationships
- transaction- and roster-move-aware relationship tables
- award-winner lookup sync for exact opponent-award queries
- richer contract sources beyond Lahman annual salary history
- more generalized park-era and venue-normalized historical comparisons
- broader replay clustering by game, player, theme, and historical relevance
