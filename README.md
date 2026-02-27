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

- `games.json` — game records (API-Sports format with `response[]` wrapper or top-level array)
- `playerstats.json` — player stat lines (API-Sports format with `response[]` wrapper or top-level array)

## Commands

Run the full pipeline in order:

```bash
# Ingest games (upserts teams + games, skips preseason)
bun run ingest:games

# Ingest player stats (upserts players, skips DNP/orphans)
bun run ingest:playerstats

# Build nightly events from the event catalog
bun run build:nightly-events

# Search for recurring patterns across seasons
bun run search:patterns
```

### Stats queries

```bash
# Player totals (with optional filters)
bun run stats:player -- --playerId 236 --season 2024

# Team totals (with optional filters)
bun run stats:team -- --teamId 1 --season 2024 --homeAway home
```

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
    utils.ts              # JSON reading, date/time parsing, chunking
    games.ts              # Game ingestion pipeline
    playerstats.ts        # Player stat ingestion pipeline
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
