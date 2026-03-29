# Bluey System Summary

This document explains what Bluey does, how data flows through the system, and how the dashboard UI displays results.

## What Bluey Is

Bluey is an NBA prediction platform with two major parts:

- **Engine + data pipeline** in `packages/core` and CLI commands in `apps/cli`
- **Dashboard web app** in `apps/dashboard` (Next.js + API routes)

The engine ingests game/stats/odds/injury data, discovers and validates patterns, and generates predictions.  
The dashboard reads those results, runs controlled sync/simulation actions, and visualizes picks, hit rates, and governance lineage.

## Core Architecture

- **`apps/cli`**: command router for batch jobs (build context/features, discover/validate patterns, run predictions)
- **`apps/dashboard`**: Next.js UI plus API endpoints used by that UI
- **`packages/core`**: prediction engine, discovery logic, ingest/sync implementations
- **`packages/db`**: shared Prisma schema + DB client (`@bluey/db`)
- **Database**: Postgres (single source of truth for games, contexts, predictions, ledger, patterns)

## End-to-End Data Flow

1. **Ingest/sync data**
   - Stats and games sync from NBA sources.
   - Odds + player props sync from odds providers.
   - Injuries and projected lineups sync separately.

2. **Build modeling artifacts**
   - Game context and event rows are generated.
   - Feature bins and quantized tokens are built.
   - Candidate patterns are discovered and scored.

3. **Validate + deploy patterns**
   - Pattern quality checks (LOSO/forward behavior, thresholds) determine deployed set.
   - Deployed patterns are stored in `PatternV2` (or model snapshots in `ModelVersion`).

4. **Generate picks**
   - For each game/date, engine computes:
     - discovery evidence matches
     - model picks (accuracy-oriented)
     - suggested/bettable picks (market-aware/actionability filtered)

5. **Grade + track**
   - Finished games are graded using outcomes/final scores/player stats.
   - Picks are persisted to `SuggestedPlayLedger` with hit/miss, stake, ROI fields.
   - Canonical run metadata is available via `CanonicalPrediction` tables.

## Dashboard API Layer (How Backend Powers UI)

### `GET /api/predictions`

Primary endpoint used by the predictions page.

- Loads games for a target date (Eastern date handling)
- Auto-syncs when no games are found for today/future
- Loads active model snapshot when one is enabled; otherwise uses live deployed patterns
- Builds team/player context and injury/lineup signals
- Runs `generateGamePredictions(...)`
- Evaluates outcomes for completed games and annotates picks with result status
- Upserts/refreshes `SuggestedPlayLedger` snapshots for tracking
- Returns:
  - per-game cards data
  - suggested bet picks + model picks
  - day and season hit metrics
  - wager tracking (stake/PnL/ROI)

When called with `?governance=1`, it returns canonical prediction lineage rows for governance workflows.

### `POST /api/sync`

Runs sync steps for one or two dates (requested date + optionally today):

- stats
- games
- odds
- player props
- injuries
- lineups

Returns step-level success/failure details for the UI status message.

### `GET /api/simulator`

Builds historical simulation data from canonical predictions + graded ledger rows, grouped by date/season, with filters for:

- model version
- odds mode
- gate mode
- lane type

### `POST /api/pattern-simulator`

Runs a pattern-subset simulation (user-selected deployed patterns only) across a season/date range and returns:

- day summaries
- pick-level outcomes
- aggregate hit-rate summary

### Supporting endpoints

- `GET/POST /api/model-versions`: list snapshots, active version switch, coverage counts
- `GET /api/perfect-dates`: month-level perfect-day badges (all graded picks hit under filters)
- `GET /api/discovery-v2`: fetch deployed pattern catalog for selection/exploration
- Governance detail endpoints:
  - `/api/predictions/runs`
  - `/api/predictions/rejections`
  - `/api/predictions/[predictionId]`

## How the UI Displays the System

## Layout and access

- `layout.tsx` wraps every route with providers, nav, and framed content.
- Nav is session-aware (Google/Apple/dev auth), hiding app sections when signed out.
- `/` acts as auth landing; authenticated users are redirected to `/predictions`.

## Main Predictions UI (`/predictions`)

This is the operational cockpit:

- **Game cards** show matchup, score/status, team context snippets, and compact pick badges.
- **Expanded game view** shows:
  - bettable picks (market EV + grading)
  - model picks (accuracy-focused)
  - discovery evidence blocks (posterior/edge/conditions)
- **Right sidebar controls**:
  - date calendar
  - sync trigger
  - model version selector
  - filters (pick type, ML-involved, gate mode, lane)
- **Parlay builder**:
  - click badges to add/remove legs
  - computes combined odds and payout client-side

It also surfaces:

- auto-sync state
- daily hit summary and season-to-date stats
- perfect-date indicators in calendar

## Simulator UI (`/simulator`)

Historical bankroll/performance analysis:

- pulls day-level graded picks from `/api/simulator`
- filters by season/model/gate/lane/pick-type/ML vote
- computes:
  - flat-bet PnL and ROI
  - daily parlay PnL and ROI
- includes a "run season picks" action to generate missing canonical runs for selected season/model settings

## Pattern Simulator UI (`/pattern-simulator`)

Controlled "what if only these patterns were active?" workflow:

- lists deployed patterns with search and multi-select
- posts selected pattern IDs + season to `/api/pattern-simulator`
- renders:
  - summary cards (days/picks/resolved/hit rate)
  - day table
  - pick-level table with hit/pending states

## Governance UI (`/predictions/governance`)

Read-only lineage and audit surface:

- loads run list, predictions, and rejection diagnostics
- supports run-vs-run compare:
  - added/removed/changed predictions
  - confidence/edge deltas
  - vote/supporting-pattern changes
- exports prediction/rejection JSON snapshots
- drills into one prediction's run context, votes, supporting patterns, and feature snapshot payload

## What Is Authoritative vs Display-Only

- **Authoritative pipeline logic** lives in CLI + core engine.
- **Dashboard is non-authoritative for pipeline state**:
  - it can trigger sync/simulation endpoints
  - it reads and displays data from shared DB
  - it should not replace core pipeline build/validation workflows

## Practical Mental Model

Think of Bluey as:

- a **data + pattern engine** that continuously transforms raw NBA inputs into deployable signals and graded picks
- plus a **dashboard control plane** that lets you inspect today, backtest historical behavior, simulate selected pattern sets, and audit prediction lineage end-to-end
