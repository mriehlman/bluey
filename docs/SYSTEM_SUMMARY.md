# Bluey System Summary

## What the system does

Bluey predicts NBA outcomes using discovered pattern rules plus market/context features, then serves those picks in the dashboard and simulator.

Core flow:

1. Sync and build data (games, stats, odds, injuries, lineups, features/tokens)
2. Discover and validate patterns (`PatternV2`)
3. Snapshot active model version (`ModelVersion`)
4. Generate picks for dates/games
5. Persist picks and outcomes to `SuggestedPlayLedger`
6. Dashboard and simulator read from ledger + canonical prediction tables

---

## Main data objects

- `Game`: schedule, teams, scores, status
- `GameContext` + related context tables: pregame stats and signals
- `GameFeatureToken`: tokenized conditions used by pattern matching
- `PatternV2`: discovered/validated/deployed pattern rules
- `ModelVersion`: frozen snapshot of deployed patterns, bins, config, and activation status
- `CanonicalPrediction`: run outputs tied to game and run context
- `SuggestedPlayLedger`: model-scoped picks with actionable flags, prices, hit/miss, pnl

---

## Prediction pipeline

### 1) Data sync and context

Data is refreshed from multiple ingest jobs (stats, odds, props, injuries, lineups).  
For `/api/predictions`, if today/future has no games, the API can auto-sync and retry.

### 2) Model load behavior

`/api/predictions` loads:

- Active `ModelVersion` snapshot if one is active
- Otherwise falls back to live deployed `PatternV2`

That means activating a snapshot immediately changes which pattern set powers picks.

### 3) Game prediction

For each game:

- Build effective game context (team form + injuries/lineup signals + odds)
- Run `generateGamePredictions(...)`
- Produce:
  - discovery matches
  - suggested plays
  - suggested bet picks (the picks used for ledger and bankroll)

### 4) Result grading

When final scores/outcome events exist, picks are graded:

- `settledResult`: `HIT` / `MISS` / `PENDING`
- `settledHit`: boolean/null
- Explanation fields are attached where possible

### 5) Ledger upsert

`SuggestedPlayLedger` is upserted per date, model version, and gate mode.

Important scoping fields:

- `modelVersionName`
- `gateMode` (`legacy` or `strict`)
- `laneTag` (moneyline/spread/total/player lanes)
- `isActionable`

This scoping is what keeps dashboard metrics aligned to the selected model/filter.

---

## Discovery and model lifecycle

1. `search:discover-patterns-v2` creates candidates/pattern rows
2. `validate:patterns-v2` decides status transitions (`candidate`, `validated`, `deployed`, `retired`)
3. `snapshot:model-version` freezes current deployed patterns into a named version
4. `activate:model-version` marks that version active

If snapshot fails with "No deployed patterns found", discovery produced rows but none are currently deployed yet.

---

## How the dashboard UI displays data

## Main page data calls

`apps/dashboard/src/app/page.tsx` fetches with `cache: "no-store"`:

- `/api/predictions?...`
- `/api/model-versions`
- `/api/perfect-dates?...`

This prevents stale client cache from showing old states.

### `/api/predictions`

Returns per-game picks plus summary objects:

- selected model version used
- season-to-date hit metrics
- wager tracking metrics
- game-level picks and resolved status

It also writes/refreshes ledger rows (subject to date/refresh rules).

### `/api/model-versions`

Returns:

- model versions with `isActive`
- coverage stats (`gradedDates`, `gradedPicks`) derived from canonical+ledger joins

Used by model selector and coverage display in UI.

### `/api/perfect-dates`

Returns calendar dates where all graded picks for selected scope hit.

Scope filters include:

- month
- filter (`game`, `player`, `all`)
- ML filter (`all`, `ml_only`, `no_ml`)
- lane
- model version
- gate mode

This drives purple perfect-day highlighting in the calendar.

---

## Simulator behavior

Two simulator paths exist:

- `/api/simulator`: reads from canonical + ledger, supports model/gate/lane filters, returns day-level simulated history
- `/api/pattern-simulator`: runs selected pattern IDs across a season range; can load explicit model version or active version

Practical note:

- If simulator is run with model=`all`, it can mix data across versions.
- For model-accurate comparison, always pick a specific model version.

---

## Why UI and model can appear mismatched

Most mismatches come from one of these:

1. Active model changed, but ledger not backfilled for that model/date range
2. UI model filter != gate mode used during ledger generation
3. Simulator/model set to `all` instead of a specific model
4. Old cached responses (mitigated by `no-store`)
5. Past rows still pending and not re-graded yet (fixed by pending-row refresh logic)

---

## Production operating checklist

1. Discover + validate patterns
2. Confirm non-zero deployed count
3. Snapshot and activate model version
4. Backfill ledger for intended season/date range and selected model
5. Verify ledger counts by `modelVersionName` + `gateMode`
6. Open dashboard with matching model/gate filters
7. Check:
   - daily pick volume
   - resolved hit/miss updates
   - perfect-day calendar matches filtered scope

---

## Current architecture intent

- `ModelVersion` controls what logic is active
- `SuggestedPlayLedger` is the source of truth for UI performance and calendar views
- Dashboard correctness depends on strict model/gate/lane scoping in queries
- Simulator correctness depends on not mixing model scopes unintentionally
