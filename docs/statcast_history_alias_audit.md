# Statcast History Alias Audit

This audit tracks the imported Statcast custom-history columns that now have
intentional baseball-language coverage, plus the smaller set that still need
better semantics or broader aliasing.

## Recently Resolved With Domain Confirmation

These families are now explicitly mapped in the imported-history layer.

### Catcher Pop Time / Throwing

- `pop_2b_sba_count`
- `pop_2b_sba`
- `pop_2b_sb`
- `pop_2b_cs`
- `pop_3b_sba_count`
- `pop_3b_sba`
- `pop_3b_sb`
- `pop_3b_cs`
- `exchange_2b_3b_sba`
- `maxeff_arm_2b_3b_sba`

Current coverage:

- `pop time to second`
- `pop time to third`
- `on stolen base attempts`
- `on steals`
- `on caught stealing`
- `exchange time`
- `catch to throw exchange`
- `arm strength`
- `max arm strength`
- `max throw velocity`
- `catcher throw velocity`

### Inherited / Bequeathed Runners

- `p_inh_runner` -> `inherited runners`
- `p_inh_runner_scored` -> `inherited runners scored`
- `p_beq_runner` -> `bequeathed runners`
- `p_beq_runner_scored` -> `bequeathed runners scored`

### Scoring-Context Counts

- `b_ab_scoring` -> hitter at-bats with runners in scoring position
- `p_ab_scoring` -> pitcher at-bats against with runners in scoring position
- `p_hit_scoring` -> pitcher hits allowed with runners in scoring position

Current coverage includes:

- `with runners in scoring position`
- `with a runner in scoring position`
- `with RISP`
- `allowed hits with RISP`

### Miscellaneous Baseball-Specific Shorthand

- `b_reached_on_int` -> `reached on interference`
- `r_defensive_indiff` / `p_defensive_indiff` -> `defensive indifference`
- `n_fieldout_5stars` -> `5-star field outs`, `5-star catches`, `5-star plays`
- `opp` families -> `opportunities`

### Pickoff Families

These are now intentionally kept distinct:

- `pickoff_attempt_*` -> pickoff attempts
- `pickoff_error_*` -> pickoff errors

The alias layer no longer collapses attempts into errors.

## Strong Alias Families

These imported-history categories now behave well in both leaderboard and
direct player-metric paths:

- Count-style batted-ball stats:
  - `linedrives`
  - `groundballs`
  - `flyballs`
  - `popups`
- Called-strike families:
  - `b_called_strike`
  - `p_called_strike`
- Sacrifice families:
  - `b_sac_bunt`, `p_sac_bunt`
  - `b_sac_fly`, `p_sac_fly`
  - `b_total_sacrifices`, `p_total_sacrifices`
- Cross-role remapping:
  - hitter-side stats in the batter import and pitcher-side stats in the
    pitcher import resolve by player role instead of whichever table matched
    first
- Basic baseball count phrasing:
  - `home run` / `hr`
  - `walk` / `bb`
  - `strikeout` / `so`
  - `single` / `double` / `triple`
  - `plate appearances`, `at bats`

## Remaining Lower-Confidence Areas

These are the main areas that still deserve a broader semantics pass rather
than one-off alias additions.

### Scoring-Context Nuance

The basic aliases are in place, but the exact baseball semantics should still
be treated carefully in answers:

- `b_ab_scoring`
- `p_ab_scoring`
- `p_hit_scoring`

They are currently surfaced as RISP-context counts, which is directionally
correct and useful, but the exported Statcast labels remain less explicit than
some other columns.

### Opportunity / Star-Catch Families Beyond 5 Stars

We now interpret `opp` as `opportunity`, but the broader star-opportunity
families still have thinner natural-language coverage than core batting and
pitching metrics.

Examples:

- `n_opp_1stars`
- `n_opp_2stars`
- `n_opp_3stars`
- `n_opp_4stars`
- `n_opp_5stars`

## Recommended Next Steps

1. Keep broadening category-level alias families rather than adding prompt-only
   regressions.
2. Expand direct support for the rest of the catcher-defense / arm-strength
   export families around pop time and opportunities.
3. Continue unifying imported Statcast-history metrics with the newer entity
   aggregation path so richer local tables consistently outrank stale generic
   leaderboard routes.
