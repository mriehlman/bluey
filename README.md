# Bluey

NBA nightly event detection and pattern search engine. Ingests game and player stats, computes nightly events, and discovers recurring multi-event patterns across seasons.

## Prerequisites

- [Bun](https://bun.sh) (v1.0+)
- [Docker](https://www.docker.com/) (for Postgres)

## Setup

```bash
# 1. Install dependencies
bun install

# 2. Start Postgres
bun run db:up

# 3. Create .env from example
cp .env.example .env

# 4. Run database migrations
bun run prisma:migrate
```

## Data

Place your data files in `./data/` (or set `DATA_DIR` in `.env`):

- `raw/day/YYYY-MM-DD.json` — daily all-in-one bundle (games, stats, odds, props)

## Commands

Run the ingest pipeline in order:

```bash
# Build daily bundles from raw source files
bun run build:day-bundles

# Ingest day bundles into Postgres
bun run ingest:day-bundles
```

### Stats queries

```bash
# Player totals (with optional filters)
bun run stats:player -- --playerId 236 --season 2024

# Team totals (with optional filters)
bun run stats:team -- --teamId 1 --season 2024 --homeAway home
```

## Pattern Discovery & Prediction Workflow (v2)

Use this pipeline when you want fresh patterns, validation, and picks.

### Full rebuild (recommended after data/model changes)

```bash
bun run build:game-context -- --from-season 2023 --to-season 2026
bun run build:game-events -- --from-season 2023 --to-season 2026
bun run build:feature-bins -- --from-season 2023 --to-season 2026
bun run build:quantized-game-features -- --from-season 2023 --to-season 2026
bun run search:discover-patterns-v2 -- --cv loso --forward-from 2025
bun run validate:patterns-v2
```

### What each step does

1. `build:game-context`  
   Builds pregame team/player context (`GameContext`, `PlayerGameContext`) such as record, rest, ranks, streak, and player profile context.

2. `build:game-events`  
   Writes `GameEvent` rows from the event catalog (`condition` + `outcome` events).

3. `build:feature-bins`  
   Builds quantile/fixed bin definitions in `FeatureBin` for numeric features (e.g., spread/total buckets).

4. `build:quantized-game-features`  
   Converts each game into tokenized features and upserts `GameFeatureToken`.

5. `search:discover-patterns-v2`  
   Generates and scores candidate patterns, then stores top candidates in `PatternV2`.
   - `--cv loso` uses leave-one-season-out validation across pre-forward seasons.
   - `--forward-from 2025` treats 2025+ as forward holdout.

6. `validate:patterns-v2`  
   Applies deployment thresholds/FDR/decay checks, updates `PatternV2.status`, and rebuilds `PatternV2Hit` for deployed patterns.

### Daily run (faster)

If your feature definitions and historical context are already built:

```bash
bun run search:discover-patterns-v2 -- --cv loso --forward-from 2025
bun run validate:patterns-v2
bun run predict:today
```

### Evaluate known pattern ideas (hypothesis testing)

You can test human-written hypotheses (not just auto-discovered patterns):

```bash
bun run evaluate:pattern-ideas
```

By default it reads `config/pattern-ideas.json`, evaluates enabled ideas with LOSO + forward holdout, and prints PASS/FAIL with train/val/forward stats.

Useful flags:

```bash
# Use a custom ideas file
bun run evaluate:pattern-ideas -- --file config/my-ideas.json

# Override split settings
bun run evaluate:pattern-ideas -- --cv loso --forward-from 2025 --min-loso-folds-pass 2

# Tighten thresholds
bun run evaluate:pattern-ideas -- --min-forward-edge 0.008 --min-forward-posterior 0.52
```

### Dashboard/API picks

- Predictions endpoint (single date):  
  `/api/predictions?date=YYYY-MM-DD`
- Force ledger recalculation for that date:  
  `/api/predictions?date=YYYY-MM-DD&refreshLedger=1`

### Important metric distinction

- `PatternV2Hit` metrics (e.g., season hit rate in v2 summary) measure **deployed pattern hit/miss history**.
- `SuggestedPlayLedger` metrics measure **actionable surfaced picks** after market filters (EV, odds caps, etc.).
- These are intentionally different views and can have different counts/hit rates.

### Filter flags

All stats commands accept these optional flags:

| Flag | Description |
|------|-------------|
| `--season` | Filter by season year |
| `--dateFrom` | Start date (YYYY-MM-DD) |
| `--dateTo` | End date (YYYY-MM-DD) |
| `--opponentTeamId` | Filter by opponent team ID |
| `--homeAway` | `home`, `away`, or `either` |
| `--minMinutes` | Minimum minutes played (per game) |
| `--stage` | Game stage (2 = regular season) |

### Infrastructure

```bash
bun run db:up       # Start Postgres container
bun run db:down     # Stop Postgres container
```

## Architecture

```
src/
  cli/index.ts            # CLI entry point and command router
  db/prisma.ts            # PrismaClient singleton
  ingest/
    buildDailyDataBundles.ts  # Builds one JSON bundle per date
    ingestDayBundles.ts       # High-throughput day-bundle ingestion
    syncNbaStats.ts           # nba_api-based stat/game sync
    syncOdds.ts               # Odds API sync
    syncPlayerProps.ts        # Player props sync
    utils.ts                  # Shared date/time helpers
  stats/
    filters.ts            # RollupFilters type + Prisma where builders
    playerRollup.ts       # Player aggregate stats
    teamRollup.ts         # Team aggregate stats
  features/
    eventCatalog.ts       # Nightly event definitions (11 events)
    buildNightEvents.ts   # Runs catalog over all game dates
  patterns/
    scoring.ts            # Pattern stability scoring
    searchPatterns.ts     # Combo generation + pattern discovery
```

## Event Catalog

| Event | Trigger |
|-------|---------|
| SLATE_GAMES_GE_7 | 7+ games on the slate |
| HOME_TEAMS_WIN_GE_5 | 5+ home teams win |
| ANY_TEAM_130_PLUS | Any team scores 130+ |
| ANY_GAME_TOTAL_260_PLUS | Any game total 260+ |
| TRIPLE_DOUBLE_EXISTS | Any triple-double recorded |
| TWO_30PT_SCORERS | 2+ players score 30+ |
| PLAYER_40_PLUS_POINTS | Any player scores 40+ |
| THREE_TEAMS_HAVE_3x20PT_SCORERS | 3+ teams each have 3+ players scoring 20+ |
| TEAM_WITH_60_REBOUNDS | Any team grabs 60+ rebounds |
| ANY_PLAYER_15_PLUS_ASSISTS | Any player records 15+ assists |
| ANY_PLAYER_20_PLUS_REBOUNDS | Any player records 20+ rebounds |
