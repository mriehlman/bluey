# Signal Token Glossary

This doc explains the condition token format used in deployed pattern rules, such as:

- `away_rest_days:Q1`
- `role_dependency_band:STAR_LED`
- `rest_advantage_delta:EVEN`
- `!home_lineup_certainty:Q1`

## Token format

General form:

- `<feature_name>:<bucket_or_label>`

Optional negation:

- `!<feature_name>:<bucket_or_label>` means the condition is **NOT** in that bucket/label.

## Common bucket/label meanings

### Quantile buckets (`Q#`)

- `Q1`: lowest quintile (bottom 20% of historical distribution)
- `Q2`: lower-middle quintile
- `Q3`: middle quintile
- `Q4`: upper-middle quintile
- `Q5`: highest quintile (top 20%)

These are relative buckets after binning a raw feature value.

### Special labels

- `EVEN`: near-balanced difference (roughly no edge in either direction)
- `LOW` / `MID` / `HIGH`: coarse bucket labels
- `LATE` / `MID` (season fields): season-timing buckets
- `CHAOTIC`: high uncertainty / high injury-noise environment
- `STAR_LED`: game/team profile dominated by star players
- `WINNING_SIDE`: leans toward team currently on stronger trend/streak
- `LOSING_SIDE`: leans toward team currently on weaker trend/streak
- `0`: exactly zero for that feature
- `3_PLUS`: value is three or more

## Common token families in your patterns

### Rest and schedule

- `home_rest_days`, `away_rest_days`: days since previous game
- `rest_advantage_delta`: difference between home/away rest

### Injuries and lineup uncertainty

- `injury_environment`: combined injury-context state
- `injury_noise`: volatility/noise in injury context
- `home_injury_out`, `away_injury_out`: out-injury burden by side
- `home_injury_questionable`, `away_injury_questionable`: questionable burden
- `injury_out_delta`, `injury_questionable_delta`: side-to-side differences
- `home_lineup_certainty`, `away_lineup_certainty`: lineup stability confidence
- `lineup_certainty`, `lineup_certainty_delta`: aggregate and delta forms
- `home_late_scratch_risk`, `away_late_scratch_risk`, `late_scratch_risk_delta`: late-scratch risk context

### Team form and momentum

- `home_streak`, `away_streak`: streak state bucket
- `streak_delta`: side-to-side momentum difference
- `team_form_volatility`: stability of recent team form

### Strength and style profile

- `home_net_rating`, `away_net_rating`: net-rating bucket
- `home_rank_off`, `home_rank_def`, `away_rank_def`: ranking buckets
- `home_oppg`, `away_oppg`: opponent points allowed profile
- `home_fg3_rate`, `away_fg3_rate`: three-point rate profile

### Role and creation concentration

- `home_role_dependency`, `away_role_dependency`: dependence on top usage players
- `role_dependency_band`: consolidated role-dependency state
- `home_creation_burden`, `away_creation_burden`: creation responsibility load
- `home_playmaking_resilience`, `away_playmaking_resilience`: ability to sustain creation under absences

### Recency and coverage quality

- `home_top3_last5ppg`, `away_top3_last5ppg`: recent scoring trend for top players
- `data_completeness`: confidence that all required data inputs are present
- `season_progress`, `season_phase`: where game lies in season timeline

## Reading examples

- `away_rest_days:Q1`
  - Away team has very low rest relative to league/game distribution.

- `role_dependency_band:STAR_LED`
  - Matchup profile indicates outcomes are strongly driven by star-centric usage.

- `rest_advantage_delta:EVEN`
  - Home/away rest differential is near zero.

- `!home_lineup_certainty:Q1`
  - Home lineup certainty is **not** in the lowest bucket (i.e., not extremely uncertain).

## Notes

- Buckets are model-feature bins, not direct raw-stat thresholds.
- Exact cut points can evolve when feature bins are rebuilt.
- Interpretation is best done together with `uses`, `scoreAbsSum`, and `avgPosterior` from the report CSV.
