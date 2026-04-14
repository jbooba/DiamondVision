# Statcast History Coverage Matrix

This matrix tracks how the imported Statcast custom-history tables currently plug into DiamondVision's broader query engine.

Primary imported tables:

- `statcast_history_batter_seasons`
- `statcast_history_pitcher_seasons`

Current imported totals:

- Batter-season queryable metrics: `193`
- Pitcher-season queryable metrics: `316`
- Total queryable imported metrics: `509`

Status legend:

- `Strong`: reliable natural-language path exists today
- `Partial`: data exists and some routes work, but routing/qualifiers/follow-ups still miss in places
- `Separate Warehouse`: covered well, but by a different local Statcast table family rather than the imported season-history tables
- `Gap`: imported data exists but is not yet exposed cleanly in the current query grammar

## Query Surface Matrix

| Query surface | Coverage | Source path | Notes |
| --- | --- | --- | --- |
| Direct player-season metric lookup | `Strong` | `player_metric_lookup.py` -> imported Statcast history fallback | Works for prompts like `what was Tarik Skubal's Whiff% last year?`, `what was Aaron Judge's xwOBA in 2024?` |
| Single-season player leaderboards | `Strong` | `season_metric_leaderboards.py` -> `statcast_history_*_seasons` | Works for highest/lowest/most/fewest on imported batter and pitcher metrics |
| Multi-season / span / career player leaderboards | `Strong` | `season_metric_leaderboards.py` aggregated imported-history rows | Weighted averages and cumulative sums work across ranges like `2017-2026`, `last 10 seasons`, `career` |
| Explicit hitter / pitcher role filtering | `Strong` | historical + imported-history role-aware routing | Prevents pitcher batting rows from hijacking hitter prompts |
| Direct player follow-ups (`how many of those hits were HR?`) | `Partial` | `chat.py` follow-up rewriting + `player_span_metrics.py` | Much better than before, but still not universal for every imported-history metric family |
| Handedness cohorts (`left`, `right`, `switch`) | `Partial` | `cohort_metric_leaderboards.py` + local Statcast handedness / imported fallback | Switch/right/left hitter EV and related season metrics are supported, but some wording still needs better alias/routing coverage |
| Award / nationality / manager-era cohorts using imported history | `Partial` | mixed cohort warehouse + imported-history fallback | Basic cohort system exists, but imported-history routing is strongest for handedness and weaker for other reusable cohort families |
| Team-filtered imported-history leaderboards | `Gap` | n/a | Imported season-history tables are player-season snapshots and are not yet a first-class team-filtered leaderboard surface |
| Direct player span metric lookup (`between 2017 and 2026`) | `Partial` | `player_span_metrics.py` + historical tables, some imported-history overlap | Span answers work well for many traditional stats; imported-history-specific direct player span lookup needs broader explicit routing |
| Pitch mix / repertoire lookup | `Separate Warehouse` | `statcast_pitch_type_games` + arsenal/provider adapters | Works well, but uses the synced pitch-type warehouse rather than imported season-history tables |
| Event / park / direction / pitch-type event filters | `Separate Warehouse` | `statcast_events` and event leaderboards | Queries like `hits to left field at Oracle Park` are handled here, not by imported season-history tables |
| Live / today / yesterday Statcast answers | `Separate Warehouse` | daily synced local Statcast summaries + MLB game feed | Daily freshness is handled by `sync-statcast`, not by the imported season-history CSVs |

## Imported Metric Family Matrix

### Batter Metrics

| Family | Count | Current status | Reliable query shapes | Main gaps |
| --- | ---: | --- | --- | --- |
| Bat Tracking / Swing Traits | `6` | `Partial` | player-season lookups, single-season leaderboards | some aliasing and cohort coverage still thin |
| Batted Ball Quality | `25` | `Strong` | Avg EV, launch angle, hard-hit%, barrel%, sweet-spot%, xBAcon/xwOBAcon leaderboards and lookups | team-filtered imported-history routing still thin |
| Plate Discipline / Swing Decisions | `30` | `Strong` | Whiff%, Swing%, Z/O-zone swing and miss metrics, in-zone / meatball / edge rates | some conversational aliases still need broadening |
| Rate / Expected Outcome Stats | `15` | `Strong` | AVG, OBP, SLG, OPS, ISO, xBA, xSLG, xwOBA, xOBP, xISO | ambiguous shorthand can still choose a historical route first |
| Result / Counting Stats | `63` | `Strong` | HR, hits, walks, strikeouts, PA, AB, total bases, sacrifices, etc. | follow-up chaining across spans still improving |
| Other | `37` | `Partial` | niche export fields are queryable if named closely | many low-semantic export columns still need better aliases or should stay internal-only |

### Pitcher Metrics

| Family | Count | Current status | Reliable query shapes | Main gaps |
| --- | ---: | --- | --- | --- |
| Bat Tracking / Swing Traits | `6` | `Partial` | player-season lookups and some leaderboards | conversational naming is still narrower than hitter-side coverage |
| Batted Ball Quality | `25` | `Strong` | contact-quality-allowed, hard-hit%, barrel%, avg EV allowed, launch-angle-allowed | some user phrasing still falls back to generic pitcher descriptions |
| Pitch Arsenal Shape | `105` | `Partial` | many imported pitch-shape fields are queryable in season leaderboards | repertoire/mix UX is stronger through the separate pitch-type warehouse than through these imported season snapshots |
| Pitch Counts | `15` | `Partial` | pitch-family counts and volume leaderboards | grammar for count-vs-rate phrasing still needs expansion |
| Plate Discipline / Swing Decisions | `30` | `Strong` | Whiff%, swing miss, zone/contact, chase-style metrics | some prompt aliases still lean hitter-centric |
| Rate / Expected Outcome Stats | `16` | `Strong` | ERA, xBA allowed, xSLG allowed, xwOBA allowed, opponent slash metrics | team-filtered and cohort-filtered versions still expanding |
| Result / Counting Stats | `81` | `Strong` | wins, losses, walks, strikeouts, home runs allowed, opponent results, innings-style rate metrics | direct player-span imported-history lookups still not as broad as leaderboard support |
| Other | `21` | `Partial` | niche export fields are available when asked explicitly | some are low-semantic export artifacts and should not be primary NL targets |

## What Is Already Reliable

These imported-history families are good bets today:

- hitter and pitcher season leaderboards for imported Statcast metrics
- direct player-season lookups for imported Statcast metrics
- multi-season / last-N-seasons / career-style leaderboards for imported metrics
- core quality-of-contact metrics
- expected-stats families (`xBA`, `xSLG`, `xwOBA`, `xOBP`, `xISO`)
- plate-discipline metrics like `Whiff%`, `Swing%`, `Z Swing Miss%`, `OZ Swing Miss%`
- many pitch-shape and arsenal-style season metrics

## What Still Needs Broader Routing

These are the highest-value next expansions:

- team-filtered imported-history leaderboards
- richer cohort joins using imported-history metrics beyond handedness
- broader direct player-span imported-history lookup
- more natural aliases for the `Other` columns and some pitch-shape families
- follow-up chaining that keeps imported-history context alive across multiple turns

## Practical Rule Of Thumb

When a question is:

- about a player's season-level Statcast metric: imported history should usually answer it
- about a season leaderboard for a Statcast metric: imported history should usually answer it
- about a park / direction / event / count / pitch-location filter: `statcast_events` should usually answer it
- about repertoire / pitch mix: `statcast_pitch_type_games` should usually answer it
- about live/today/yesterday performance: the daily synced Statcast warehouse plus MLB game feeds should answer it
