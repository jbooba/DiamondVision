# Statcast History Metric Inventory

Generated from the imported Statcast custom-history tables used by DiamondVision.

## Summary

- Batter-season queryable metrics: `176`
- Pitcher-season queryable metrics: `299`
- Total queryable imported metrics: `475`

Sample basis notes:
- `pa`: plate appearances
- `ab`: at-bats
- `batted_ball`: batted-ball events
- `pitch_count`: total pitches
- `n_*_formatted`: pitch-type specific pitch count

## Batter History Metrics

### Bat Tracking / Swing Traits (`6`)

| Metric | Column | Sample Basis | Minimum Sample | Format |
| --- | --- | --- | ---: | --- |
| attack angle | `attack_angle` | `pa` | 20 | `.1f` |
| attack direction | `attack_direction` | `pa` | 20 | `.2f` |
| Blasts Contact | `blasts_contact` | `pa` | 1 | `.0f` |
| ideal angle rate | `ideal_angle_rate` | `pa` | 1 | `.0f` |
| Squared Up Contact | `squared_up_contact` | `pa` | 1 | `.0f` |
| swords | `swords` | `pa` | 1 | `.0f` |

### Batted Ball Quality (`25`)

| Metric | Column | Sample Basis | Minimum Sample | Format |
| --- | --- | --- | ---: | --- |
| average exit velocity | `exit_velocity_avg` | `batted_ball` | 1 | `.0f` |
| average launch angle | `launch_angle_avg` | `batted_ball` | 1 | `.0f` |
| bacon | `bacon` | `batted_ball` | 10 | `.2f` |
| Barrel | `barrel` | `batted_ball` | 1 | `.0f` |
| barrel rate | `barrel_batted_rate` | `batted_ball` | 1 | `.0f` |
| Flareburner Percent | `flareburner_percent` | `batted_ball` | 10 | `.1f` |
| Flyballs | `flyballs` | `batted_ball` | 1 | `.0f` |
| Flyballs Percent | `flyballs_percent` | `batted_ball` | 10 | `.1f` |
| Groundballs | `groundballs` | `batted_ball` | 1 | `.0f` |
| Groundballs Percent | `groundballs_percent` | `batted_ball` | 10 | `.1f` |
| hard-hit rate | `hard_hit_percent` | `batted_ball` | 10 | `.1f` |
| Linedrives | `linedrives` | `batted_ball` | 1 | `.0f` |
| Linedrives Percent | `linedrives_percent` | `batted_ball` | 10 | `.1f` |
| Opposite Percent | `opposite_percent` | `batted_ball` | 10 | `.1f` |
| Poorlytopped Percent | `poorlytopped_percent` | `batted_ball` | 10 | `.1f` |
| Poorlyunder Percent | `poorlyunder_percent` | `batted_ball` | 10 | `.1f` |
| Poorlyweak Percent | `poorlyweak_percent` | `batted_ball` | 10 | `.1f` |
| Popups | `popups` | `batted_ball` | 1 | `.0f` |
| Popups Percent | `popups_percent` | `batted_ball` | 10 | `.1f` |
| Pull Percent | `pull_percent` | `batted_ball` | 10 | `.1f` |
| Straightaway Percent | `straightaway_percent` | `batted_ball` | 10 | `.1f` |
| sweet spot rate | `sweet_spot_percent` | `batted_ball` | 10 | `.1f` |
| wOBAcon | `wobacon` | `batted_ball` | 10 | `.3f` |
| xBAcon | `xbacon` | `batted_ball` | 10 | `.3f` |
| xwOBAcon | `xwobacon` | `batted_ball` | 10 | `.3f` |

### Other (`37`)

| Metric | Column | Sample Basis | Minimum Sample | Format |
| --- | --- | --- | ---: | --- |
| Ab | `ab` | `` | 1 | `.0f` |
| Ab Scoring | `b_ab_scoring` | `` | 1 | `.0f` |
| Avg Best Speed | `avg_best_speed` | `` | 1 | `.1f` |
| Avg Hyper Speed | `avg_hyper_speed` | `` | 1 | `.1f` |
| bb percent | `bb_percent` | `pa` | 20 | `.1f` |
| Catcher Interf | `b_catcher_interf` | `` | 1 | `.0f` |
| Defensive Indiff | `r_defensive_indiff` | `` | 1 | `.0f` |
| Exchange 2B 3B Sba | `exchange_2b_3b_sba` | `` | 1 | `.0f` |
| Interference | `b_interference` | `` | 1 | `.0f` |
| Interference | `r_interference` | `` | 1 | `.0f` |
| k percent | `k_percent` | `pa` | 20 | `.1f` |
| Maxeff Arm 2B 3B Sba | `maxeff_arm_2b_3b_sba` | `` | 1 | `.0f` |
| N 1Star Percent | `n_1star_percent` | `` | 1 | `.1f` |
| N 2Star Percent | `n_2star_percent` | `` | 1 | `.1f` |
| N 3Star Percent | `n_3star_percent` | `` | 1 | `.1f` |
| N 4Star Percent | `n_4star_percent` | `` | 1 | `.1f` |
| N 5Star Percent | `n_5star_percent` | `` | 1 | `.1f` |
| Pa | `pa` | `` | 1 | `.0f` |
| Pitch Count | `pitch_count` | `pitch_count` | 1 | `.0f` |
| Pitch Count Breaking | `pitch_count_breaking` | `pitch_count` | 1 | `.0f` |
| Pitch Count Offspeed | `pitch_count_offspeed` | `pitch_count` | 1 | `.0f` |
| Played Dh | `b_played_dh` | `` | 1 | `.0f` |
| Pop 2B Cs | `pop_2b_cs` | `` | 1 | `.0f` |
| Pop 2B Sb | `pop_2b_sb` | `` | 1 | `.0f` |
| Pop 2B Sba | `pop_2b_sba` | `` | 1 | `.0f` |
| Pop 2B Sba Count | `pop_2b_sba_count` | `` | 1 | `.0f` |
| Pop 3B Cs | `pop_3b_cs` | `` | 1 | `.0f` |
| Pop 3B Sb | `pop_3b_sb` | `` | 1 | `.0f` |
| Pop 3B Sba | `pop_3b_sba` | `` | 1 | `.0f` |
| Pop 3B Sba Count | `pop_3b_sba_count` | `` | 1 | `.0f` |
| Reached On Int | `b_reached_on_int` | `` | 1 | `.0f` |
| Sac Bunt | `b_sac_bunt` | `` | 1 | `.0f` |
| Sac Fly | `b_sac_fly` | `` | 1 | `.0f` |
| Total Bases | `b_total_bases` | `` | 1 | `.0f` |
| Total Pitches | `b_total_pitches` | `` | 1 | `.0f` |
| Total Sacrifices | `b_total_sacrifices` | `` | 1 | `.0f` |
| Total Strike | `b_total_strike` | `` | 1 | `.0f` |

### Plate Discipline / Swing Decisions (`30`)

| Metric | Column | Sample Basis | Minimum Sample | Format |
| --- | --- | --- | ---: | --- |
| average swing length | `avg_swing_length` | `pitch_count` | 50 | `.3f` |
| average swing speed | `avg_swing_speed` | `pitch_count` | 50 | `.1f` |
| Blasts Swing | `blasts_swing` | `pitch_count` | 1 | `.0f` |
| Edge | `edge` | `pitch_count` | 1 | `.0f` |
| Edge Percent | `edge_percent` | `pitch_count` | 50 | `.1f` |
| F Strike Percent | `f_strike_percent` | `pitch_count` | 50 | `.1f` |
| fast swing rate | `fast_swing_rate` | `pitch_count` | 1 | `.0f` |
| In Zone | `in_zone` | `pitch_count` | 1 | `.0f` |
| In Zone Percent | `in_zone_percent` | `pitch_count` | 50 | `.1f` |
| In Zone Swing | `in_zone_swing` | `pitch_count` | 1 | `.0f` |
| In Zone Swing Miss | `in_zone_swing_miss` | `pitch_count` | 1 | `.0f` |
| Iz Contact Percent | `iz_contact_percent` | `pitch_count` | 50 | `.1f` |
| Meatball Percent | `meatball_percent` | `pitch_count` | 50 | `.1f` |
| Meatball Swing Percent | `meatball_swing_percent` | `pitch_count` | 50 | `.1f` |
| Out Zone | `out_zone` | `pitch_count` | 1 | `.0f` |
| Out Zone Percent | `out_zone_percent` | `pitch_count` | 50 | `.1f` |
| Out Zone Swing | `out_zone_swing` | `pitch_count` | 1 | `.0f` |
| Out Zone Swing Miss | `out_zone_swing_miss` | `pitch_count` | 1 | `.0f` |
| Oz Contact Percent | `oz_contact_percent` | `pitch_count` | 50 | `.1f` |
| Oz Swing Miss Percent | `oz_swing_miss_percent` | `pitch_count` | 50 | `.1f` |
| Oz Swing Percent | `oz_swing_percent` | `pitch_count` | 50 | `.1f` |
| Solidcontact Percent | `solidcontact_percent` | `pitch_count` | 50 | `.1f` |
| Squared Up Swing | `squared_up_swing` | `pitch_count` | 1 | `.0f` |
| Swing Percent | `swing_percent` | `pitch_count` | 50 | `.1f` |
| Swinging Strike | `b_swinging_strike` | `pitch_count` | 1 | `.0f` |
| Total Swinging Strike | `b_total_swinging_strike` | `pitch_count` | 1 | `.0f` |
| vertical swing path | `vertical_swing_path` | `pitch_count` | 1 | `.0f` |
| Whiff Percent | `whiff_percent` | `pitch_count` | 50 | `.1f` |
| Z Swing Miss Percent | `z_swing_miss_percent` | `pitch_count` | 50 | `.1f` |
| Z Swing Percent | `z_swing_percent` | `pitch_count` | 50 | `.1f` |

### Rate / Expected Outcome Stats (`15`)

| Metric | Column | Sample Basis | Minimum Sample | Format |
| --- | --- | --- | ---: | --- |
| Avg | `batting_avg` | `ab` | 20 | `.3f` |
| Babip | `babip` | `` | 1 | `.3f` |
| iso | `isolated_power` | `ab` | 20 | `.3f` |
| obp | `on_base_percent` | `pa` | 20 | `.1f` |
| ops | `on_base_plus_slg` | `pa` | 20 | `.3f` |
| slg | `slg_percent` | `ab` | 20 | `.1f` |
| wOBA | `woba` | `pa` | 20 | `.3f` |
| Wobadiff | `wobadiff` | `pa` | 20 | `.3f` |
| xBA | `xba` | `ab` | 20 | `.3f` |
| Xbadiff | `xbadiff` | `ab` | 20 | `.3f` |
| xISO | `xiso` | `ab` | 20 | `.3f` |
| xOBP | `xobp` | `pa` | 20 | `.3f` |
| xSLG | `xslg` | `ab` | 20 | `.3f` |
| Xslgdiff | `xslgdiff` | `ab` | 20 | `.3f` |
| xwOBA | `xwoba` | `pa` | 20 | `.3f` |

### Result / Counting Stats (`63`)

| Metric | Column | Sample Basis | Minimum Sample | Format |
| --- | --- | --- | ---: | --- |
| Ball | `b_ball` | `` | 1 | `.0f` |
| Batted Ball | `batted_ball` | `` | 1 | `.0f` |
| Called Strike | `b_called_strike` | `` | 1 | `.0f` |
| Caught Stealing 2B | `r_caught_stealing_2b` | `` | 1 | `.0f` |
| Caught Stealing 3B | `r_caught_stealing_3b` | `` | 1 | `.0f` |
| Caught Stealing Home | `r_caught_stealing_home` | `` | 1 | `.0f` |
| Double | `double` | `` | 1 | `.0f` |
| Foul | `b_foul` | `` | 1 | `.0f` |
| Foul Tip | `b_foul_tip` | `` | 1 | `.0f` |
| Game | `b_game` | `` | 1 | `.0f` |
| Gnd Into Dp | `b_gnd_into_dp` | `` | 1 | `.0f` |
| Gnd Into Tp | `b_gnd_into_tp` | `` | 1 | `.0f` |
| Gnd Rule Double | `b_gnd_rule_double` | `` | 1 | `.0f` |
| Hit | `hit` | `` | 1 | `.0f` |
| Hit By Pitch | `b_hit_by_pitch` | `` | 1 | `.0f` |
| Hit Fly | `b_hit_fly` | `` | 1 | `.0f` |
| Hit Ground | `b_hit_ground` | `` | 1 | `.0f` |
| Hit Into Play | `b_hit_into_play` | `` | 1 | `.0f` |
| Hit Line Drive | `b_hit_line_drive` | `` | 1 | `.0f` |
| Hit Popup | `b_hit_popup` | `` | 1 | `.0f` |
| Home Run | `home_run` | `` | 1 | `.0f` |
| Intent Ball | `b_intent_ball` | `` | 1 | `.0f` |
| Intent Walk | `b_intent_walk` | `` | 1 | `.0f` |
| Lob | `b_lob` | `` | 1 | `.0f` |
| N Fieldout 1Stars | `n_fieldout_1stars` | `` | 1 | `.0f` |
| N Fieldout 2Stars | `n_fieldout_2stars` | `` | 1 | `.0f` |
| N Fieldout 3Stars | `n_fieldout_3stars` | `` | 1 | `.0f` |
| N Fieldout 4Stars | `n_fieldout_4stars` | `` | 1 | `.0f` |
| N Fieldout 5Stars | `n_fieldout_5stars` | `` | 1 | `.0f` |
| N Opp 1Stars | `n_opp_1stars` | `` | 1 | `.0f` |
| N Opp 2Stars | `n_opp_2stars` | `` | 1 | `.0f` |
| N Opp 3Stars | `n_opp_3stars` | `` | 1 | `.0f` |
| N Opp 4Stars | `n_opp_4stars` | `` | 1 | `.0f` |
| N Opp 5Stars | `n_opp_5stars` | `` | 1 | `.0f` |
| N Outs Above Average | `n_outs_above_average` | `` | 1 | `.0f` |
| Out Fly | `b_out_fly` | `` | 1 | `.0f` |
| Out Ground | `b_out_ground` | `` | 1 | `.0f` |
| Out Line Drive | `b_out_line_drive` | `` | 1 | `.0f` |
| Out Popup | `b_out_popup` | `` | 1 | `.0f` |
| Pickoff 1B | `r_pickoff_1b` | `` | 1 | `.0f` |
| Pickoff 2B | `r_pickoff_2b` | `` | 1 | `.0f` |
| Pickoff 3B | `r_pickoff_3b` | `` | 1 | `.0f` |
| Pinch Hit | `b_pinch_hit` | `` | 1 | `.0f` |
| Pinch Run | `b_pinch_run` | `` | 1 | `.0f` |
| Pitch Count Fastball | `pitch_count_fastball` | `pitch_count` | 1 | `.0f` |
| Pitchout | `b_pitchout` | `` | 1 | `.0f` |
| Player Age | `player_age` | `` | 1 | `.2f` |
| Rbi | `b_rbi` | `` | 1 | `.0f` |
| Reached On Error | `b_reached_on_error` | `` | 1 | `.0f` |
| Run | `r_run` | `` | 1 | `.0f` |
| Single | `single` | `` | 1 | `.0f` |
| Stolen Base 2B | `r_stolen_base_2b` | `` | 1 | `.0f` |
| Stolen Base 3B | `r_stolen_base_3b` | `` | 1 | `.0f` |
| Stolen Base Home | `r_stolen_base_home` | `` | 1 | `.0f` |
| Stolen Base Pct | `r_stolen_base_pct` | `` | 1 | `.0f` |
| Strikeout | `strikeout` | `` | 1 | `.0f` |
| Total Ball | `b_total_ball` | `` | 1 | `.0f` |
| Total Caught Stealing | `r_total_caught_stealing` | `` | 1 | `.0f` |
| Total Pickoff | `r_total_pickoff` | `` | 1 | `.0f` |
| Total Stolen Base | `r_total_stolen_base` | `` | 1 | `.0f` |
| Triple | `triple` | `` | 1 | `.0f` |
| Walk | `walk` | `` | 1 | `.0f` |
| Walkoff | `b_walkoff` | `` | 1 | `.0f` |

## Pitcher History Metrics

### Bat Tracking / Swing Traits (`6`)

| Metric | Column | Sample Basis | Minimum Sample | Format |
| --- | --- | --- | ---: | --- |
| attack angle | `attack_angle` | `pa` | 20 | `.1f` |
| attack direction | `attack_direction` | `pa` | 20 | `.2f` |
| Blasts Contact | `blasts_contact` | `pa` | 1 | `.0f` |
| ideal angle rate | `ideal_angle_rate` | `pa` | 1 | `.0f` |
| Squared Up Contact | `squared_up_contact` | `pa` | 1 | `.0f` |
| swords | `swords` | `pa` | 1 | `.0f` |

### Batted Ball Quality (`25`)

| Metric | Column | Sample Basis | Minimum Sample | Format |
| --- | --- | --- | ---: | --- |
| average exit velocity | `exit_velocity_avg` | `batted_ball` | 1 | `.0f` |
| average launch angle | `launch_angle_avg` | `batted_ball` | 1 | `.0f` |
| bacon | `bacon` | `batted_ball` | 10 | `.2f` |
| Barrel | `barrel` | `batted_ball` | 1 | `.0f` |
| barrel rate | `barrel_batted_rate` | `batted_ball` | 1 | `.0f` |
| Flareburner Percent | `flareburner_percent` | `batted_ball` | 10 | `.1f` |
| Flyballs | `flyballs` | `batted_ball` | 1 | `.0f` |
| Flyballs Percent | `flyballs_percent` | `batted_ball` | 10 | `.1f` |
| Groundballs | `groundballs` | `batted_ball` | 1 | `.0f` |
| Groundballs Percent | `groundballs_percent` | `batted_ball` | 10 | `.1f` |
| hard-hit rate | `hard_hit_percent` | `batted_ball` | 10 | `.1f` |
| Linedrives | `linedrives` | `batted_ball` | 1 | `.0f` |
| Linedrives Percent | `linedrives_percent` | `batted_ball` | 10 | `.1f` |
| Opposite Percent | `opposite_percent` | `batted_ball` | 10 | `.1f` |
| Poorlytopped Percent | `poorlytopped_percent` | `batted_ball` | 10 | `.1f` |
| Poorlyunder Percent | `poorlyunder_percent` | `batted_ball` | 10 | `.1f` |
| Poorlyweak Percent | `poorlyweak_percent` | `batted_ball` | 10 | `.1f` |
| Popups | `popups` | `batted_ball` | 1 | `.0f` |
| Popups Percent | `popups_percent` | `batted_ball` | 10 | `.1f` |
| Pull Percent | `pull_percent` | `batted_ball` | 10 | `.1f` |
| Straightaway Percent | `straightaway_percent` | `batted_ball` | 10 | `.1f` |
| sweet spot rate | `sweet_spot_percent` | `batted_ball` | 10 | `.1f` |
| wOBAcon | `wobacon` | `batted_ball` | 10 | `.3f` |
| xBAcon | `xbacon` | `batted_ball` | 10 | `.3f` |
| xwOBAcon | `xwobacon` | `batted_ball` | 10 | `.3f` |

### Other (`21`)

| Metric | Column | Sample Basis | Minimum Sample | Format |
| --- | --- | --- | ---: | --- |
| Ab | `ab` | `` | 1 | `.0f` |
| Ab Scoring | `p_ab_scoring` | `` | 1 | `.0f` |
| Arm Angle | `arm_angle` | `` | 1 | `.1f` |
| Avg Best Speed | `avg_best_speed` | `` | 1 | `.1f` |
| Avg Hyper Speed | `avg_hyper_speed` | `` | 1 | `.1f` |
| bb percent | `bb_percent` | `pa` | 20 | `.1f` |
| Catcher Interf | `p_catcher_interf` | `` | 1 | `.0f` |
| Defensive Indiff | `p_defensive_indiff` | `` | 1 | `.0f` |
| k percent | `k_percent` | `pa` | 20 | `.1f` |
| Missed Bunt | `p_missed_bunt` | `` | 1 | `.0f` |
| Pa | `pa` | `` | 1 | `.0f` |
| Pitch Count | `pitch_count` | `pitch_count` | 1 | `.0f` |
| Pitch Count Breaking | `pitch_count_breaking` | `pitch_count` | 1 | `.0f` |
| Pitch Count Offspeed | `pitch_count_offspeed` | `pitch_count` | 1 | `.0f` |
| Sac Bunt | `p_sac_bunt` | `` | 1 | `.0f` |
| Sac Fly | `p_sac_fly` | `` | 1 | `.0f` |
| Starting P | `p_starting_p` | `` | 1 | `.0f` |
| Total Bases | `p_total_bases` | `` | 1 | `.0f` |
| Total Pitches | `p_total_pitches` | `` | 1 | `.0f` |
| Total Sacrifices | `p_total_sacrifices` | `` | 1 | `.0f` |
| Total Strike | `p_total_strike` | `` | 1 | `.0f` |

### Pitch Arsenal Shape (`105`)

| Metric | Column | Sample Basis | Minimum Sample | Format |
| --- | --- | --- | ---: | --- |
| Breaking Ball Avg Break X | `breaking_avg_break_x` | `n_breaking_formatted` | 10 | `.1f` |
| Breaking Ball Avg Break Z | `breaking_avg_break_z` | `n_breaking_formatted` | 10 | `.1f` |
| Breaking Ball Break | `breaking_avg_break` | `n_breaking_formatted` | 10 | `.1f` |
| Breaking Ball Ivb | `breaking_avg_break_z_induced` | `n_breaking_formatted` | 10 | `.1f` |
| Breaking Ball Speed Range | `breaking_range_speed` | `n_breaking_formatted` | 1 | `.0f` |
| Breaking Ball Spin | `breaking_avg_spin` | `n_breaking_formatted` | 10 | `.0f` |
| Breaking Ball Velocity | `breaking_avg_speed` | `n_breaking_formatted` | 10 | `.1f` |
| Changeup Avg Break X | `ch_avg_break_x` | `n_ch_formatted` | 10 | `.1f` |
| Changeup Avg Break Z | `ch_avg_break_z` | `n_ch_formatted` | 10 | `.1f` |
| Changeup Break | `ch_avg_break` | `n_ch_formatted` | 10 | `.1f` |
| Changeup Ivb | `ch_avg_break_z_induced` | `n_ch_formatted` | 10 | `.1f` |
| Changeup Speed Range | `ch_range_speed` | `n_ch_formatted` | 1 | `.0f` |
| Changeup Spin | `ch_avg_spin` | `n_ch_formatted` | 10 | `.0f` |
| Changeup Velocity | `ch_avg_speed` | `n_ch_formatted` | 10 | `.1f` |
| Curveball Avg Break X | `cu_avg_break_x` | `n_cu_formatted` | 10 | `.1f` |
| Curveball Avg Break Z | `cu_avg_break_z` | `n_cu_formatted` | 10 | `.1f` |
| Curveball Break | `cu_avg_break` | `n_cu_formatted` | 10 | `.1f` |
| Curveball Ivb | `cu_avg_break_z_induced` | `n_cu_formatted` | 10 | `.1f` |
| Curveball Speed Range | `cu_range_speed` | `n_cu_formatted` | 1 | `.0f` |
| Curveball Spin | `cu_avg_spin` | `n_cu_formatted` | 10 | `.0f` |
| Curveball Velocity | `cu_avg_speed` | `n_cu_formatted` | 10 | `.1f` |
| Cutter Avg Break X | `fc_avg_break_x` | `n_fc_formatted` | 10 | `.1f` |
| Cutter Avg Break Z | `fc_avg_break_z` | `n_fc_formatted` | 10 | `.1f` |
| Cutter Break | `fc_avg_break` | `n_fc_formatted` | 10 | `.1f` |
| Cutter Ivb | `fc_avg_break_z_induced` | `n_fc_formatted` | 10 | `.1f` |
| Cutter Speed Range | `fc_range_speed` | `n_fc_formatted` | 1 | `.0f` |
| Cutter Spin | `fc_avg_spin` | `n_fc_formatted` | 10 | `.0f` |
| Cutter Velocity | `fc_avg_speed` | `n_fc_formatted` | 10 | `.1f` |
| Fastball Avg Break X | `fastball_avg_break_x` | `n_fastball_formatted` | 10 | `.1f` |
| Fastball Avg Break Z | `fastball_avg_break_z` | `n_fastball_formatted` | 10 | `.1f` |
| Fastball Break | `fastball_avg_break` | `n_fastball_formatted` | 10 | `.1f` |
| Fastball Ivb | `fastball_avg_break_z_induced` | `n_fastball_formatted` | 10 | `.1f` |
| Fastball Speed Range | `fastball_range_speed` | `n_fastball_formatted` | 1 | `.0f` |
| Fastball Spin | `fastball_avg_spin` | `n_fastball_formatted` | 10 | `.0f` |
| Fastball Velocity | `fastball_avg_speed` | `n_fastball_formatted` | 10 | `.1f` |
| Forkball Avg Break X | `fo_avg_break_x` | `n_fo_formatted` | 10 | `.1f` |
| Forkball Avg Break Z | `fo_avg_break_z` | `n_fo_formatted` | 10 | `.1f` |
| Forkball Break | `fo_avg_break` | `n_fo_formatted` | 10 | `.1f` |
| Forkball Ivb | `fo_avg_break_z_induced` | `n_fo_formatted` | 10 | `.1f` |
| Forkball Speed Range | `fo_range_speed` | `n_fo_formatted` | 1 | `.0f` |
| Forkball Spin | `fo_avg_spin` | `n_fo_formatted` | 10 | `.0f` |
| Forkball Velocity | `fo_avg_speed` | `n_fo_formatted` | 10 | `.1f` |
| Four-Seam Fastball Avg Break X | `ff_avg_break_x` | `n_ff_formatted` | 10 | `.1f` |
| Four-Seam Fastball Avg Break Z | `ff_avg_break_z` | `n_ff_formatted` | 10 | `.1f` |
| Four-Seam Fastball Break | `ff_avg_break` | `n_ff_formatted` | 10 | `.1f` |
| Four-Seam Fastball Ivb | `ff_avg_break_z_induced` | `n_ff_formatted` | 10 | `.1f` |
| Four-Seam Fastball Speed Range | `ff_range_speed` | `n_ff_formatted` | 1 | `.0f` |
| Four-Seam Fastball Spin | `ff_avg_spin` | `n_ff_formatted` | 10 | `.0f` |
| Four-Seam Fastball Velocity | `ff_avg_speed` | `n_ff_formatted` | 10 | `.1f` |
| Knuckleball Avg Break X | `kn_avg_break_x` | `n_kn_formatted` | 10 | `.1f` |
| Knuckleball Avg Break Z | `kn_avg_break_z` | `n_kn_formatted` | 10 | `.1f` |
| Knuckleball Break | `kn_avg_break` | `n_kn_formatted` | 10 | `.1f` |
| Knuckleball Ivb | `kn_avg_break_z_induced` | `n_kn_formatted` | 10 | `.1f` |
| Knuckleball Speed Range | `kn_range_speed` | `n_kn_formatted` | 1 | `.0f` |
| Knuckleball Spin | `kn_avg_spin` | `n_kn_formatted` | 10 | `.0f` |
| Knuckleball Velocity | `kn_avg_speed` | `n_kn_formatted` | 10 | `.1f` |
| Offspeed Pitch Avg Break X | `offspeed_avg_break_x` | `n_offspeed_formatted` | 10 | `.1f` |
| Offspeed Pitch Avg Break Z | `offspeed_avg_break_z` | `n_offspeed_formatted` | 10 | `.1f` |
| Offspeed Pitch Break | `offspeed_avg_break` | `n_offspeed_formatted` | 10 | `.1f` |
| Offspeed Pitch Ivb | `offspeed_avg_break_z_induced` | `n_offspeed_formatted` | 10 | `.1f` |
| Offspeed Pitch Speed Range | `offspeed_range_speed` | `n_offspeed_formatted` | 1 | `.0f` |
| Offspeed Pitch Spin | `offspeed_avg_spin` | `n_offspeed_formatted` | 10 | `.0f` |
| Offspeed Pitch Velocity | `offspeed_avg_speed` | `n_offspeed_formatted` | 10 | `.1f` |
| Screwball Avg Break X | `sc_avg_break_x` | `n_sc_formatted` | 10 | `.1f` |
| Screwball Avg Break Z | `sc_avg_break_z` | `n_sc_formatted` | 10 | `.1f` |
| Screwball Break | `sc_avg_break` | `n_sc_formatted` | 10 | `.1f` |
| Screwball Ivb | `sc_avg_break_z_induced` | `n_sc_formatted` | 10 | `.1f` |
| Screwball Speed Range | `sc_range_speed` | `n_sc_formatted` | 1 | `.0f` |
| Screwball Spin | `sc_avg_spin` | `n_sc_formatted` | 10 | `.0f` |
| Screwball Velocity | `sc_avg_speed` | `n_sc_formatted` | 10 | `.1f` |
| Sinker Avg Break X | `si_avg_break_x` | `n_si_formatted` | 10 | `.1f` |
| Sinker Avg Break Z | `si_avg_break_z` | `n_si_formatted` | 10 | `.1f` |
| Sinker Break | `si_avg_break` | `n_si_formatted` | 10 | `.1f` |
| Sinker Ivb | `si_avg_break_z_induced` | `n_si_formatted` | 10 | `.1f` |
| Sinker Speed Range | `si_range_speed` | `n_si_formatted` | 1 | `.0f` |
| Sinker Spin | `si_avg_spin` | `n_si_formatted` | 10 | `.0f` |
| Sinker Velocity | `si_avg_speed` | `n_si_formatted` | 10 | `.1f` |
| Slider Avg Break X | `sl_avg_break_x` | `n_sl_formatted` | 10 | `.1f` |
| Slider Avg Break Z | `sl_avg_break_z` | `n_sl_formatted` | 10 | `.1f` |
| Slider Break | `sl_avg_break` | `n_sl_formatted` | 10 | `.1f` |
| Slider Ivb | `sl_avg_break_z_induced` | `n_sl_formatted` | 10 | `.1f` |
| Slider Speed Range | `sl_range_speed` | `n_sl_formatted` | 1 | `.0f` |
| Slider Spin | `sl_avg_spin` | `n_sl_formatted` | 10 | `.0f` |
| Slider Velocity | `sl_avg_speed` | `n_sl_formatted` | 10 | `.1f` |
| Slurve Avg Break X | `sv_avg_break_x` | `n_sv_formatted` | 10 | `.1f` |
| Slurve Avg Break Z | `sv_avg_break_z` | `n_sv_formatted` | 10 | `.1f` |
| Slurve Break | `sv_avg_break` | `n_sv_formatted` | 10 | `.1f` |
| Slurve Ivb | `sv_avg_break_z_induced` | `n_sv_formatted` | 10 | `.1f` |
| Slurve Speed Range | `sv_range_speed` | `n_sv_formatted` | 1 | `.0f` |
| Slurve Spin | `sv_avg_spin` | `n_sv_formatted` | 10 | `.0f` |
| Slurve Velocity | `sv_avg_speed` | `n_sv_formatted` | 10 | `.1f` |
| Splitter Avg Break X | `fs_avg_break_x` | `n_fs_formatted` | 10 | `.1f` |
| Splitter Avg Break Z | `fs_avg_break_z` | `n_fs_formatted` | 10 | `.1f` |
| Splitter Break | `fs_avg_break` | `n_fs_formatted` | 10 | `.1f` |
| Splitter Ivb | `fs_avg_break_z_induced` | `n_fs_formatted` | 10 | `.1f` |
| Splitter Speed Range | `fs_range_speed` | `n_fs_formatted` | 1 | `.0f` |
| Splitter Spin | `fs_avg_spin` | `n_fs_formatted` | 10 | `.0f` |
| Splitter Velocity | `fs_avg_speed` | `n_fs_formatted` | 10 | `.1f` |
| Sweeper Avg Break X | `st_avg_break_x` | `n_st_formatted` | 10 | `.1f` |
| Sweeper Avg Break Z | `st_avg_break_z` | `n_st_formatted` | 10 | `.1f` |
| Sweeper Break | `st_avg_break` | `n_st_formatted` | 10 | `.1f` |
| Sweeper Ivb | `st_avg_break_z_induced` | `n_st_formatted` | 10 | `.1f` |
| Sweeper Speed Range | `st_range_speed` | `n_st_formatted` | 1 | `.0f` |
| Sweeper Spin | `st_avg_spin` | `n_st_formatted` | 10 | `.0f` |
| Sweeper Velocity | `st_avg_speed` | `n_st_formatted` | 10 | `.1f` |

### Pitch Counts (`15`)

| Metric | Column | Sample Basis | Minimum Sample | Format |
| --- | --- | --- | ---: | --- |
| Breaking Ball Count | `n_breaking_formatted` | `n_breaking_formatted` | 1 | `.0f` |
| Changeup Count | `n_ch_formatted` | `n_ch_formatted` | 1 | `.0f` |
| Curveball Count | `n_cu_formatted` | `n_cu_formatted` | 1 | `.0f` |
| Cutter Count | `n_fc_formatted` | `n_fc_formatted` | 1 | `.0f` |
| Fastball Count | `n_fastball_formatted` | `n_fastball_formatted` | 1 | `.0f` |
| Forkball Count | `n_fo_formatted` | `n_fo_formatted` | 1 | `.0f` |
| Four-Seam Fastball Count | `n_ff_formatted` | `n_ff_formatted` | 1 | `.0f` |
| Knuckleball Count | `n_kn_formatted` | `n_kn_formatted` | 1 | `.0f` |
| Offspeed Pitch Count | `n_offspeed_formatted` | `n_offspeed_formatted` | 1 | `.0f` |
| Screwball Count | `n_sc_formatted` | `n_sc_formatted` | 1 | `.0f` |
| Sinker Count | `n_si_formatted` | `n_si_formatted` | 1 | `.0f` |
| Slider Count | `n_sl_formatted` | `n_sl_formatted` | 1 | `.0f` |
| Slurve Count | `n_sv_formatted` | `n_sv_formatted` | 1 | `.0f` |
| Splitter Count | `n_fs_formatted` | `n_fs_formatted` | 1 | `.0f` |
| Sweeper Count | `n_st_formatted` | `n_st_formatted` | 1 | `.0f` |

### Plate Discipline / Swing Decisions (`30`)

| Metric | Column | Sample Basis | Minimum Sample | Format |
| --- | --- | --- | ---: | --- |
| average swing length | `avg_swing_length` | `pitch_count` | 50 | `.3f` |
| average swing speed | `avg_swing_speed` | `pitch_count` | 50 | `.1f` |
| Blasts Swing | `blasts_swing` | `pitch_count` | 1 | `.0f` |
| Edge | `edge` | `pitch_count` | 1 | `.0f` |
| Edge Percent | `edge_percent` | `pitch_count` | 50 | `.1f` |
| F Strike Percent | `f_strike_percent` | `pitch_count` | 50 | `.1f` |
| fast swing rate | `fast_swing_rate` | `pitch_count` | 1 | `.0f` |
| In Zone | `in_zone` | `pitch_count` | 1 | `.0f` |
| In Zone Percent | `in_zone_percent` | `pitch_count` | 50 | `.1f` |
| In Zone Swing | `in_zone_swing` | `pitch_count` | 1 | `.0f` |
| In Zone Swing Miss | `in_zone_swing_miss` | `pitch_count` | 1 | `.0f` |
| Iz Contact Percent | `iz_contact_percent` | `pitch_count` | 50 | `.1f` |
| Meatball Percent | `meatball_percent` | `pitch_count` | 50 | `.1f` |
| Meatball Swing Percent | `meatball_swing_percent` | `pitch_count` | 50 | `.1f` |
| Out Zone | `out_zone` | `pitch_count` | 1 | `.0f` |
| Out Zone Percent | `out_zone_percent` | `pitch_count` | 50 | `.1f` |
| Out Zone Swing | `out_zone_swing` | `pitch_count` | 1 | `.0f` |
| Out Zone Swing Miss | `out_zone_swing_miss` | `pitch_count` | 1 | `.0f` |
| Oz Contact Percent | `oz_contact_percent` | `pitch_count` | 50 | `.1f` |
| Oz Swing Miss Percent | `oz_swing_miss_percent` | `pitch_count` | 50 | `.1f` |
| Oz Swing Percent | `oz_swing_percent` | `pitch_count` | 50 | `.1f` |
| Solidcontact Percent | `solidcontact_percent` | `pitch_count` | 50 | `.1f` |
| Squared Up Swing | `squared_up_swing` | `pitch_count` | 1 | `.0f` |
| Swing Percent | `swing_percent` | `pitch_count` | 50 | `.1f` |
| Swinging Strike | `p_swinging_strike` | `pitch_count` | 1 | `.0f` |
| Total Swinging Strike | `p_total_swinging_strike` | `pitch_count` | 1 | `.0f` |
| vertical swing path | `vertical_swing_path` | `pitch_count` | 1 | `.0f` |
| Whiff Percent | `whiff_percent` | `pitch_count` | 50 | `.1f` |
| Z Swing Miss Percent | `z_swing_miss_percent` | `pitch_count` | 50 | `.1f` |
| Z Swing Percent | `z_swing_percent` | `pitch_count` | 50 | `.1f` |

### Rate / Expected Outcome Stats (`16`)

| Metric | Column | Sample Basis | Minimum Sample | Format |
| --- | --- | --- | ---: | --- |
| Avg | `batting_avg` | `ab` | 20 | `.3f` |
| Babip | `babip` | `` | 1 | `.3f` |
| iso | `isolated_power` | `ab` | 20 | `.3f` |
| obp | `on_base_percent` | `pa` | 20 | `.1f` |
| opponent batting average | `p_opp_batting_avg` | `ab` | 20 | `.3f` |
| ops | `on_base_plus_slg` | `pa` | 20 | `.3f` |
| slg | `slg_percent` | `ab` | 20 | `.1f` |
| wOBA | `woba` | `pa` | 20 | `.3f` |
| Wobadiff | `wobadiff` | `pa` | 20 | `.3f` |
| xBA | `xba` | `ab` | 20 | `.3f` |
| Xbadiff | `xbadiff` | `ab` | 20 | `.3f` |
| xISO | `xiso` | `ab` | 20 | `.3f` |
| xOBP | `xobp` | `pa` | 20 | `.3f` |
| xSLG | `xslg` | `ab` | 20 | `.3f` |
| Xslgdiff | `xslgdiff` | `ab` | 20 | `.3f` |
| xwOBA | `xwoba` | `pa` | 20 | `.3f` |

### Result / Counting Stats (`81`)

| Metric | Column | Sample Basis | Minimum Sample | Format |
| --- | --- | --- | ---: | --- |
| Automatic Ball | `p_automatic_ball` | `` | 1 | `.0f` |
| Balk | `p_balk` | `` | 1 | `.0f` |
| Ball | `p_ball` | `` | 1 | `.0f` |
| Batted Ball | `batted_ball` | `` | 1 | `.0f` |
| Beq Runner | `p_beq_runner` | `` | 1 | `.0f` |
| Beq Runner Scored | `p_beq_runner_scored` | `` | 1 | `.0f` |
| Blown Save | `p_blown_save` | `` | 1 | `.0f` |
| Called Strike | `p_called_strike` | `` | 1 | `.0f` |
| Caught Stealing 2B | `p_caught_stealing_2b` | `` | 1 | `.0f` |
| Caught Stealing 3B | `p_caught_stealing_3b` | `` | 1 | `.0f` |
| Caught Stealing Home | `p_caught_stealing_home` | `` | 1 | `.0f` |
| Complete Game | `p_complete_game` | `` | 1 | `.0f` |
| Double | `double` | `` | 1 | `.0f` |
| Earned Run | `p_earned_run` | `` | 1 | `.0f` |
| era | `p_era` | `pa` | 20 | `.2f` |
| Foul | `p_foul` | `` | 1 | `.0f` |
| Foul Tip | `p_foul_tip` | `` | 1 | `.0f` |
| Game | `p_game` | `` | 1 | `.0f` |
| Game Finished | `p_game_finished` | `` | 1 | `.0f` |
| Game In Relief | `p_game_in_relief` | `` | 1 | `.0f` |
| Gnd Into Dp | `p_gnd_into_dp` | `` | 1 | `.0f` |
| Gnd Into Tp | `p_gnd_into_tp` | `` | 1 | `.0f` |
| Gnd Rule Double | `p_gnd_rule_double` | `` | 1 | `.0f` |
| Hit | `hit` | `pa` | 1 | `.0f` |
| Hit By Pitch | `p_hit_by_pitch` | `pa` | 1 | `.0f` |
| Hit Fly | `p_hit_fly` | `pa` | 1 | `.0f` |
| Hit Ground | `p_hit_ground` | `pa` | 1 | `.0f` |
| Hit Into Play | `p_hit_into_play` | `pa` | 1 | `.0f` |
| Hit Line Drive | `p_hit_line_drive` | `pa` | 1 | `.0f` |
| Hit Scoring | `p_hit_scoring` | `pa` | 1 | `.0f` |
| Hold | `p_hold` | `` | 1 | `.0f` |
| Home Run | `home_run` | `pa` | 1 | `.0f` |
| Inh Runner | `p_inh_runner` | `` | 1 | `.0f` |
| Inh Runner Scored | `p_inh_runner_scored` | `` | 1 | `.0f` |
| Intent Ball | `p_intent_ball` | `` | 1 | `.0f` |
| Intent Walk | `p_intent_walk` | `pa` | 1 | `.0f` |
| Lob | `p_lob` | `` | 1 | `.0f` |
| Loss | `p_loss` | `` | 1 | `.0f` |
| opponent on base average | `p_opp_on_base_avg` | `pa` | 20 | `.3f` |
| Out | `p_out` | `` | 1 | `.0f` |
| Out Fly | `p_out_fly` | `` | 1 | `.0f` |
| Out Ground | `p_out_ground` | `` | 1 | `.0f` |
| Out Line Drive | `p_out_line_drive` | `` | 1 | `.0f` |
| Passed Ball | `p_passed_ball` | `` | 1 | `.0f` |
| Pickoff 1B | `p_pickoff_1b` | `` | 1 | `.0f` |
| Pickoff 2B | `p_pickoff_2b` | `` | 1 | `.0f` |
| Pickoff 3B | `p_pickoff_3b` | `` | 1 | `.0f` |
| Pickoff Attempt 1B | `p_pickoff_attempt_1b` | `` | 1 | `.0f` |
| Pickoff Attempt 2B | `p_pickoff_attempt_2b` | `` | 1 | `.0f` |
| Pickoff Attempt 3B | `p_pickoff_attempt_3b` | `` | 1 | `.0f` |
| Pickoff Error 1B | `p_pickoff_error_1b` | `` | 1 | `.0f` |
| Pickoff Error 2B | `p_pickoff_error_2b` | `` | 1 | `.0f` |
| Pickoff Error 3B | `p_pickoff_error_3b` | `` | 1 | `.0f` |
| Pitch Count Fastball | `pitch_count_fastball` | `pitch_count` | 1 | `.0f` |
| Pitchout | `p_pitchout` | `` | 1 | `.0f` |
| Player Age | `player_age` | `` | 1 | `.2f` |
| Quality Start | `p_quality_start` | `` | 1 | `.0f` |
| Rbi | `p_rbi` | `` | 1 | `.0f` |
| Reached On Error | `p_reached_on_error` | `` | 1 | `.0f` |
| Relief No Out | `p_relief_no_out` | `` | 1 | `.0f` |
| Run | `p_run` | `` | 1 | `.0f` |
| Run Support | `p_run_support` | `` | 1 | `.0f` |
| Save | `p_save` | `` | 1 | `.0f` |
| Shutout | `p_shutout` | `` | 1 | `.0f` |
| Single | `single` | `` | 1 | `.0f` |
| Stolen Base 2B | `p_stolen_base_2b` | `` | 1 | `.0f` |
| Stolen Base 3B | `p_stolen_base_3b` | `` | 1 | `.0f` |
| Stolen Base Home | `p_stolen_base_home` | `` | 1 | `.0f` |
| Strikeout | `strikeout` | `pa` | 1 | `.0f` |
| Total Ball | `p_total_ball` | `` | 1 | `.0f` |
| Total Caught Stealing | `p_total_caught_stealing` | `` | 1 | `.0f` |
| Total Pickoff | `p_total_pickoff` | `` | 1 | `.0f` |
| Total Pickoff Attempt | `p_total_pickoff_attempt` | `` | 1 | `.0f` |
| Total Pickoff Error | `p_total_pickoff_error` | `` | 1 | `.0f` |
| Total Stolen Base | `p_total_stolen_base` | `` | 1 | `.0f` |
| Triple | `triple` | `` | 1 | `.0f` |
| Unearned Run | `p_unearned_run` | `` | 1 | `.0f` |
| Walk | `walk` | `pa` | 1 | `.0f` |
| Walkoff | `p_walkoff` | `pa` | 1 | `.0f` |
| Wild Pitch | `p_wild_pitch` | `` | 1 | `.0f` |
| Win | `p_win` | `` | 1 | `.0f` |
