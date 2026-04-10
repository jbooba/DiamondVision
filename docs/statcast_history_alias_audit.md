# Statcast History Alias Audit

This audit tracks the imported Statcast custom-history columns that still need
better natural-language coverage after the broader alias pass.

## Improved In The Latest Pass

These families are now intentionally handled in both leaderboard and direct
player-metric paths:

- Count-style batted-ball stats:
  - `linedrives` -> `line drive`, `line drives`
  - `groundballs` -> `ground ball`, `ground balls`
  - `flyballs` -> `fly ball`, `fly balls`
- Called-strike families:
  - `b_called_strike`
  - `p_called_strike`
  - aliases include `called strike`, `called strikes`, `taken called strikes`
- Sacrifice families:
  - `b_sac_bunt`, `p_sac_bunt`
  - `b_sac_fly`, `p_sac_fly`
  - `b_total_sacrifices`, `p_total_sacrifices`
  - aliases include `sac`, `sacrifice`, `sac bunt`, `sac fly`, `total sacrifices`
- Cross-role remapping:
  - hitter-side stats in the batter import and pitcher-side stats in the pitcher import
    now resolve by player role instead of whichever table matched first
- Basic baseball count phrasing:
  - `home run` / `home runs` / `hr`
  - `walk` / `walks` / `bb`
  - `strikeout` / `strikeouts` / `so`
  - `single` / `double` / `triple`
  - `plate appearances`, `at bats`

## Remaining Ambiguous Export Fields

These are the main columns where the export shorthand is not obvious enough to
expand confidently without domain confirmation.

### Baserunning / Throwing

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

Questions:
- Does `sba` here mean `stolen base attempts`?
- Should `pop_2b_*` and `pop_3b_*` be surfaced as catcher pop-time metrics?
- Should `exchange_2b_3b_sba` be described as exchange time on steal attempts?

### Inherited / Bequeathed Runner Tracking

- `p_inh_runner`
- `p_inh_runner_scored`
- `p_beq_runner`
- `p_beq_runner_scored`

Questions:
- Should these be surfaced as `inherited runners`, `inherited runners scored`,
  `bequeathed runners`, and `bequeathed runners scored`?

### Scoring-Context Totals

- `b_ab_scoring`
- `p_ab_scoring`
- `p_hit_scoring`

Questions:
- Are these best described as `at bats in scoring situations` / `hits in scoring situations`?
- Or are they specifically tied to plate appearances with a runner scoring?

### Miscellaneous Possible Baseball-Specific Shorthand

- `b_reached_on_int`
- `r_interference`
- `r_defensive_indiff`
- `n_fieldout_5stars`

Questions:
- Is `reached_on_int` specifically `reached on interference`?
- Is `defensive_indiff` best surfaced as `defensive indifference`?
- Should `n_fieldout_5stars` be phrased as `5-star fielding outs` or `5-star catches`?

## Lower Priority Cleanup

These are understandable but could still use more human-friendly aliases:

- `gnd_into_dp` -> `grounded into double play`, `gidp`
- `gnd_into_tp` -> `grounded into triple play`
- `played_dh` -> `games as designated hitter`, `DH games`
- `game_finished` -> `games finished`
- `game_in_relief` -> `relief appearances`
- `pickoff_attempt_*` / `pickoff_error_*`

## Recommended Next Steps

1. Confirm the ambiguous export families above.
2. Add those meanings into the imported-history alias layer.
3. Expand direct player-metric and leaderboard regressions by category:
   - baserunning / catcher pop-time
   - inherited-runner pitching context
   - scoring-context counts
   - pickoff / interference / defensive-indifference families
