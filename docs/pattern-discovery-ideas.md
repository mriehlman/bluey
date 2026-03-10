# Pattern Discovery Ideas & Roadmap

## The edge requirement (most important step)

Develop an edge that is **explainable and measurable**. It cannot be vibes-based.

**Valid edges**: weather modeling, injury-info timing, in-game pricing inefficiencies, market making on exchanges, bottom-up modeling of markets, top-down identification of bad prices.

**The standard**: "The correct price for this line is X, based on [reason], but I can get it at a discount at [book]."

**Not an edge**: "I like the Clippers tonight because they're playing well lately and the other team is load-managing a good player."

If you can't state your edge in quantitative terms — correct price, rationale, and where you're getting value — you don't have one.

**Market selection**: Books move slower on niche props, alt lines, and smaller markets — less attention, fewer sharp bettors. Main spreads/totals are efficient; edge is more likely in player props, alt spreads, alt totals, and obscure leagues.

---

## Cross-validation strategy

### Current
- Single temporal split: train (2023), validation (2024), forward (2025+).
- "Forward" = future holdout.

### Proposed: structural cross-validation
- **Leave-one-season-out**: For each season S, train on all other seasons, validate on S.
- Train and validate both use all seasons; split is by fold, not by time.
- A game in validation may be before some training games; temporal order is not enforced.
- Aggregation: keep patterns with positive edge in ≥N folds, or rank by mean/min edge across folds.
- Benefits: more robust patterns, better use of data, less single-season overfitting.
- Caveat: still use a temporal holdout for final deployment decisions; monitor live performance.

---

## New feature dimensions

### 1. Relative date in season (early vs mid vs late)
- Early (games 1–20): higher variance, roles forming.
- Mid (games 21–60): stable identities.
- Late (games 61–82): load management, tanking, playoff positioning.
- Implementation: derive `game_number_in_season` per team, bin into tokens (e.g. `season_progress:early`, `season_progress:mid`, `season_progress:late`).

### 2. Series game number (1st vs 2nd vs 3rd vs 4th meeting)
- 1st meeting: no prior H2H data.
- 2nd–4th meetings: adjustment, revenge, rubber-match effects.
- Playoffs: elimination games, series momentum.
- Implementation: derive meeting number per (team A, team B, season), tokenize (e.g. `series_meeting:1`, `series_meeting:2`, `series_meeting:3`).

### 3. H2H record + home/away split (extend existing)
- `GameContext` already has `h2hHomeWins`, `h2hAwayWins`.
- Add tokens for:
  - Series state: `h2h_lead:home`, `h2h_tied`, `h2h_lead:away`
  - Side-specific: e.g. "home team leads 2–0" vs "trailing 0–2"
  - Context: first meeting (0–0), rubber match (1–1 in 4-game series), etc.
- Implementation: bin H2H into interpretable buckets rather than raw counts.

### 4. Player prop regression / bounce-back (game-to-game)
- **Hypothesis**: A player's bad day on a prop (e.g. under hit, missed line by a lot) might predict a bounce-back (over hit) the next game; vice versa, a very good day might predict regression (under) the next game.
- Could be mean reversion, motivation, or matchup/usage swing after a clunker.
- Implementation considerations:
  - Need per-player, per-prop "last game" outcome (hit/miss, margin vs line).
  - Tokenize: e.g. `prop_last:points_under`, `prop_last:points_over_by_5plus`, `prop_last:points_under_by_5plus`.
  - Outcome: same prop for *next* game (over/under).
- Variations: "last 2 games" trend, "streak of overs/unders", "big miss" (e.g. missed by 10+ pts) vs "close miss".

### 5. Prop line vs player average (line quality / reachability)
- **Idea**: Train on how close the prop line is to the player's average. Avoid predictions where the player would need to underperform by a lot (e.g. line far above average for an over, or line far below average for an under).
- **Problem case**: A 15 ppg player with a points line of 12 — hitting the over is trivial (they'd need to score 2+ below average); hitting the under requires them to underperform by 3+. We should not surface "over" picks that are essentially gimmes, or "under" picks that rely on a severe dropoff without signal.
- **Implementation**: Tokenize `line_vs_avg:over_by_2`, `line_vs_avg:under_by_3`, etc. (how many points above/below the player's rolling average is the line). Set a threshold (or learn it) for "too easy" or "too speculative" — e.g. filter out overs where line is >X points below average, unders where line is >X above.
- **Goal**: Ensure we're not recommending props that are either trivial (line way below avg for over) or unreasonably speculative (line way above avg for over) without compensating signal.

### 6. Time of game and travel / jetlag
- **Hypothesis**: Tip-off time (early afternoon, late-night, etc.) and time-zone travel can affect performance. West coast team playing noon ET in Boston = body-clock disadvantage; back-to-back after cross-country flight = fatigue.
- **Time of game**: Tokenize tip time (local for each team) — e.g. `tip_local:early` (before 5pm), `tip_local:prime` (7–10pm), `tip_local:late` (10pm+).
- **Travel / jetlag**: Days since last game, miles traveled, cross-timezone direction (e.g. East→West vs West→East), back-to-back. DARKO already models rest/travel; we could derive tokens for `rest:0`, `rest:1`, `rest:2plus`, `travel:cross_country`, `jetlag:west_to_east`, etc.
- **Implementation**: Need schedule data (tip time, venue, prior game location). Common in NBA APIs.

---

## External data / projection sources

### DARKO ([apanalytics.shinyapps.io/DARKO](https://apanalytics.shinyapps.io/DARKO/))
- **Daily Adjusted and Regressed Kalman Optimized** — MLB-style projection system for NBA box-score stats.
- Daily projections per player, per stat (points, rebounds, assists, FG3%, USG%, etc.), updated each day.
- Uses exponential decay (β^t) to weight games by recency; stat-specific β values (e.g. 0.98–0.9999).
- Adjustments for rest/travel, opponent, aging, seasonality, team changes.
- Trained on ~736k player-game logs since 2000–01; has outperformed DFS sites in MAE tests.
- **Use cases**:
  - Replace or complement rolling average for "line vs. player projection" (section 5). Line vs. DARKO projection may be more stable than simple rolling avg.
  - Baseline for bounce-back logic (section 4) — DARKO’s prior already captures full career + recency; deviations from it could be signals.
- **ID mapping**: Uses Basketball-Reference IDs; need mapping to our internal Player IDs.
- **Access**: Shiny app has download; check if bulk/API exists for ingestion.

---

## Methodology notes

### Regression vs classification for betting
- **Pitfall**: Regression (predict spread/margin) optimized for R², MAE, MSE can beat the sportsbook on those metrics yet fail to show profit. Those metrics reward accurate point predictions, not edge or calibrated probability.
- **Alternative**: Classification (predict cover/don’t cover) + Inv CDF to convert probabilities to spread. Optimize on **log loss** for better calibration.
- **Why it helps**: Log loss targets probability accuracy; classification + Inv CDF can yield better MAE/MSE on spread predictions *and* profit, because the model is optimized for what matters (calibration) rather than raw point error.
- **Takeaway**: For betting, prefer log loss / calibration over R²/MAE/MSE; classification → Inv CDF can outperform regression even when regression looks better on paper.

### Brier score for calibration evaluation
- **Definition**: Brier score = mean of (predicted_prob − outcome)² over predictions. Lower is better. Strictly proper scoring rule.
- **Use cases**:
  - **Evaluation**: Single metric for probability calibration. Compare across patterns, market types, time windows.
  - **Optimization**: If tuning meta-model or pattern weights, minimizing Brier pushes toward calibration.
  - **Diagnostics**: Brier decomposes into reliability (calibration) + resolution (discrimination) + uncertainty — distinguishes miscalibration from lack of signal.
- **Pattern ranking**: Include Brier of a pattern's P(cover) vs outcomes alongside hit rate and edge; well-calibrated patterns score better.
- **Vs log loss**: Both proper. Log loss penalizes extreme wrong predictions more; Brier is simpler and avoids log(0). For diagnostics, Brier is often easier; for training, either works.

### Kalman filter for past observations
- **Concept**: Each game is a noisy measurement of "true" talent. The Kalman filter combines new observations with prior beliefs and updates the estimate; the size of the update depends on how noisy the observation is.
- **Vs. exponential decay (β^t)**: Both smooth over time. Kalman models observation noise and state variance explicitly — how much can talent change between steps — which can help with irregular or uneven data.
- **Potential uses in bluey**:
  - Pattern posterior hit rate: treat each game as an observation; smooth pattern edge over time.
  - Player rolling averages: infer talent rather than raw box-score averages.
  - Edge / expectancy estimates: smooth noisy edge estimates across dates.

### Staking methods
- **Flat**: Fixed unit per bet. Simple, ignores edge and bankroll. Useful for backtests when measuring ROI.
- **Martingale**: Double stake after each loss. Recover losses on next win but blows up on long streaks. High variance, no edge-awareness — avoid for algorithmic betting.
- **Fractional Kelly (e.g. 0.25 Kelly)**: Kelly stake = (p×b − q) / b where b = decimal odds − 1, p = P(win), q = 1 − p. Use 0.25× or 0.5× Kelly for lower variance. Industry standard; scales stake with edge and bankroll.
- **Fibonacci**: Stake steps through 1, 1, 2, 3, 5, 8… on loss streak. Similar downside to Martingale, no edge-awareness — avoid.
- **Recommendation for bluey**: Use fractional Kelly (0.25–0.5) with our probability estimates. Flat for backtesting ROI. Avoid Martingale and Fibonacci.

### P-value: variance vs losing edge
- **Question**: Am I losing due to bad luck (variance) or because the edge doesn't exist?
- **Null hypothesis**: True edge = 0 (breakeven). P-value = P(observing results this bad or worse | true edge = 0).
- **Low P (< 0.05) + negative results**: Unlikely to be variance → likely a real problem; stop or fix.
- **High P (> 0.10) + negative results**: Plausible under breakeven → could be variance; keep monitoring, need more data.
- **Use cases**:
  - **Live monitoring**: After N bets, compute P vs null. Low P + losing → act. High P → may be variance.
  - **Backtest validation**: P that backtest edge > 0. Low P → statistically significant. High P → overfitting risk.
  - **Pattern quality**: Binomial test — is hit rate ≠ 50% (or market implied)? Filter out patterns with high P.
- **Implementation**: Binomial test for N bets, W wins, null p₀. One-sided P = P(W ≤ observed | p = p₀) for losses. For ROI, approximate with normal test using SE from bet-level variance.
- **Caveat**: P-values measure significance, not effect size. Use confidence intervals for magnitude. Small samples → low power.

---

## Takeaways from algobetting community

Source: [Reddit r/algobetting — €30k profit in 2022 FAQ](https://www.reddit.com/r/algobetting/comments/10dqn0y/i_made_a_profit_of_30000_algobetting_in_2022_faq/) (Hurthaba, multi-sport pre-match bettor)

### Prediction is ~50%; edge & bankroll are the rest
- "Prediction algorithms are 50% and the rest is determining optimal risk."
- He spent longer on edge determination and stake sizing than on the probability model. "An ok-version took almost a year to complete... modelling could be explained in a page or two, but I could write a book of the edge-determining alone."
- **Bluey implication**: Invest in edge / confidence logic, Kelly / stake sizing, and calibration—not just raw probability output.

### Handicaps and O/U over moneyline
- Bets mostly handicaps and over/under, not 1X2. "Both allow me to pick the optimal risk/return ratio."
- NBA spreads and totals fit this; higher volume where odds are ~2.0 and P ≈ 0.5.
- **Bluey implication**: Keep focus on spreads and totals; moneyline/winner is lower volume, less edge.

### Don’t optimize projected scoreline alone
- "What use would knowing the most likely outcome of a match be? You need the probability of each wager outcome straight away."
- He simulates P(line beaten) vs P(line not beaten), not just mean prediction.
- **Bluey implication**: Our pattern→probability per market approach is aligned. Don’t chase a “best spread” metric; focus on calibrated P(cover) per line.

### Simple, broad data beats fancy, narrow data
- "Better to have certain, simple data for 10000 matches than detailed, error-prone data for 1000 matches. Overfitting is the worst thing."
- "None of my models use any 'fancy' data, since it is only available for highest leagues—the model would be unusable in 95% of matches."
- **Bluey implication**: Favor features that exist across leagues/games; avoid features that only exist for top leagues or small samples.

### Maximize absolute income, not ROI
- "1000 worth of bets with ROI 1.05 is less income than 10000 with ROI 1.01."
- Aims ROI ~1.04; prioritizes volume when margin is sufficient.
- **Bluey implication**: Trade sharpness for volume when it increases expected profit; avoid chasing only the highest-ROI, low-volume picks.

### Backtest and validate before live
- "I did not put a single cent in betting until I knew for certain that what I did was profitable by backtesting and calculating confidence intervals."
- **Bluey implication**: Use leave-one-season-out or similar CV; temporal holdout before deployment; confidence intervals on edge.

### Sports where prediction works well
- Handball, basketball, volleyball: many scores per game → less luck.
- Football/soccer: sharp market + high variance (few goals) → harder.
- Baseball: "beaten by other statisticians."
- **Bluey implication**: NBA is well-suited; many possessions, relatively predictable. Stay away from markets that are already heavily modeled unless we have a clear edge.

### Differentiate sports in the model
- "Women's and men's basketball are essentially different sports... it was pure stupidity to try to fit them in the same model."
- **Bluey implication**: Consider separate pipelines or explicit sport/league/context tokens for NBA vs G League, men vs women, etc.

### Be different to have an edge
- "Don't try to copy other people: if something has been already done, you are not going to be the best at it. Read about pre-existing methods and their limitations; solve those limitations."
- **Bluey implication**: Pattern discovery is a different angle than pure Elo/xG. Lean into our unique approach rather than replicating public models.

### Weighting historical data is a core research topic
- "I got data from all the way since 90's, but I am constantly experimenting with how to weight it... how much to weight and based on what? That is where most of my research nowadays goes."
- **Bluey implication**: Aligns with our CV and recency discussion. Recency weighting and structural CV (leave-one-season-out) deserve ongoing attention.

---

## NBA player-props model (2-year dev, r/algobetting)

Source: Reddit user building NBA player points model + website over 2 years; 54%→63% accuracy on niche props; 11% ROI on top picks.

### Testing bugs inflate results
- Initial backtest looked great; live testing revealed a classic bug. Real performance: 54% accuracy, 1–2% ROI.
- **Bluey implication**: Rigorous validation (temporal holdout, no look-ahead), sanity checks, and conservative estimates before trusting any backtest.

### Curation beats raw model output
- Model alone: 5–6% ROI. Manual selection of ~10 picks/day: 13% ROI. Filtered top picks: 11% ROI.
- **Bluey implication**: Pipeline matters: pattern → probability → edge → **filter** → rank → top N picks. Filtering/curation can outweigh raw model gains.

### Confidence vs book inconsistency
- First "confidence score" (classification) highlighted bookie line inconsistencies, not true prediction confidence.
- **Bluey implication**: Separate "book is soft" from "model is confident." Confidence should reflect model certainty, not just market noise.

### Regression + distribution for confidence
- Switched to regression-based confidence model that outputs a distribution.
- **Bluey implication**: Output P(cover) / full distribution, not binary; use for calibration and edge.

### Context-aware filtering
- E.g. avoid "under" bets when key teammate's absence would boost the player's usage.
- **Bluey implication**: Our features (rest, injury context, usage) support this; add filtering rules that use context (teammate out, matchup, etc.).

### Niche props can have edge
- High-confidence rebound model: 63% accuracy. Smaller markets = less book attention.
- **Bluey implication**: Aligns with "books move slower on niche props." Player props, alt lines, rebounds/assists—worth exploring.

### Bayesian transfer to new leagues
- WNBA: only ~5 games per team. NBA accuracy as prior, WNBA data updates posterior.
- **Bluey implication**: For G League, international, or early-season: use NBA/similar prior + sparse new data.

### Pipeline: confidence → filter → edge → top N
- ML confidence → filter risky situations → map confidence to historical accuracy → Kelly edge → top 5 picks daily.
- **Bluey implication**: Pattern → probability → filter → map to historical edge → Kelly → rank → top N.

### Backtest caveat
- "Uses information from the future to estimate the past." Even with conservative estimates, look-ahead bias is a risk.
- **Bluey implication**: Use strict temporal holdout; avoid using future data in any backtest.

### Kelly with bankroll cap
- Caps bets at 10% of bankroll for safety.
- **Bluey implication**: Fractional Kelly + max-stake cap; conservative limits.

---

## Next steps
- [ ] Implement leave-one-season-out cross-validation in `discoverPatternsV2`.
- [ ] Add `game_number_in_season` and `season_progress` extractors.
- [ ] Add `series_meeting_number` extractor (requires per-opponent meeting order).
- [ ] Extend H2H context into token buckets.
- [ ] Explore player prop regression/bounce-back (last-game prop outcome → next-game prop).
- [ ] Add line-vs-average tokenization; filter or down-rank trivial/speculative prop picks (line far from player avg).
- [ ] Evaluate DARKO as projection source for line-vs-avg and bounce-back logic; check bulk/API access.
- [ ] Re-run discovery with new features and CV strategy.
