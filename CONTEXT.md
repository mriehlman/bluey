# Bluey Project Context

Last updated: 2026-02-27

## Project Overview

Bluey is an NBA betting analytics system that:
1. Collects historical NBA game and player stats
2. Syncs live and historical betting odds
3. Identifies patterns and generates predictions
4. Provides a dashboard for analysis

## Data Sources

### 1. NBA Stats (Player/Game Data)
- **Source**: Python `nba_api` library (scrapes NBA.com)
- **Script**: `scripts/nba_fetch.py`
- **Storage**: Day bundles in `data/raw/day/YYYY-MM-DD.json`
- **Commands**:
  - `bun run build:day-bundles` - Build day bundles from raw sources
  - `bun run ingest:day-bundles` - Ingest bundles into PostgreSQL

### 2. The Odds API (Betting Lines)
- **API Key**: Stored in `.env` as `ODDS_API_KEY`
- **Current Plan**: $30/month (20K credits) - includes historical + player props
- **Free Tier**: 500 credits/month, includes live odds + player props (fewer bookmakers)

**Credit Costs**:
- Live odds: 1 credit per request
- Historical odds: 10 credits × regions × markets (30 credits for us + h2h,spreads,totals)
- Player props: 1 credit per event

**Commands**:
- `bun run sync:odds` - Sync live game odds (spreads, totals, moneylines)
- `bun run sync:odds -- --date 2025-01-15` - Sync historical odds for a date
- `bun run sync:player-props` - Sync live player prop odds
- `bun run backfill:odds -- --from 2024-10-22 --to 2025-02-25` - Bulk historical backfill

### 3. Balldontlie API (Supplemental)
- Used for basic game schedules
- API key in `.env` as `BALLDONTLIE_API_KEY`

## Database Schema (Key Tables)

```
Team (id, name, code, city)
Player (id, firstname, lastname, jerseyNum, slug)
Game (id, sourceGameId, nbaGameId, date, homeTeamId, awayTeamId, homeScore, awayScore, status, ...)
PlayerGameStat (id, playerId, gameId, points, rebounds, assists, minutes, fgPct, fg3Pct, ...)
GameOdds (id, gameId, source, spreadHome, spreadAway, totalOver, totalUnder, mlHome, mlAway)
PlayerPropOdds (id, gameId, playerId, source, market, line, overPrice, underPrice)
```

## Key Files Modified This Session

### src/ingest/syncPlayerProps.ts
- Syncs player prop odds from The Odds API
- Auto-creates games from Odds API events if they don't exist in DB
- Maps team names to database team IDs
- Handles Over/Under parsing (description = player name, name = Over/Under)

### src/ingest/syncOdds.ts
- Syncs game odds (spreads, totals, moneylines)
- Also auto-creates games from Odds API events
- Supports both live and historical odds

### src/api/oddsApi.ts
- API client for The Odds API
- `fetchNbaOdds()` - live odds
- `fetchHistoricalOdds(date)` - historical snapshot

### src/cli/index.ts
- Primary ingestion commands: `build:day-bundles`, `ingest:day-bundles`

## Cron Job Setup (Windows)

Files for automated daily syncing:
- `scripts/daily-sync.bat` - Runs sync:odds and sync:player-props
- `scripts/setup-scheduled-task.ps1` - Creates Windows Task Scheduler job (once daily at 10 AM)
- `scripts/setup-scheduled-task-twice.ps1` - Creates two jobs (10 AM + 5 PM)
- `logs/sync.log` - Sync output logs

To set up (run as Administrator):
```powershell
cd "c:\Users\Michael Riehlman\Documents\Source\bluey"
.\scripts\setup-scheduled-task.ps1
```

## Cost-Effective Strategy

1. **One-time**: Pay $59 for 100K credits, backfill all historical NBA odds (~30K credits)
2. **Ongoing**: Downgrade to free tier (500 credits/month)
3. **Daily**: Cron job syncs live odds + player props (~60 credits/month)

## Current Data Status

- **Player Stats**: Backfilling via `nba_fetch.py` (2024-25 season in progress)
- **Game Odds**: 48+ rows synced for upcoming games
- **Player Props**: 1,419 props synced (points, rebounds, assists, threes, etc. from 6 bookmakers)
- **Historical Odds**: Partially started (Jan 15, 2025 tested successfully)

## Environment Variables (.env)

```
DATABASE_URL=postgresql://bluey:bluey@localhost:5432/bluey
DATA_DIR=./data
BALLDONTLIE_API_KEY=<key>
ODDS_API_KEY=<key>
```

## Common Commands

```bash
# Database
bun run db:up          # Start PostgreSQL container
bun run db:down        # Stop PostgreSQL container

# Data sync
bun run sync:odds                              # Live game odds
bun run sync:player-props                      # Live player props
bun run sync:upcoming                          # Sync today's games + odds

# Historical backfill
bun run build:day-bundles                       # Build all daily bundles
bun run ingest:day-bundles -- --concurrency 1   # Ingest all daily bundles
bun run backfill:odds -- --from 2024-10-22      # Historical odds

# Analysis
bun run predict:today                          # Run predictions for today
bun run search:patterns                        # Find betting patterns
```

## Notes

- The Odds API key must be set correctly in `.env` - restart terminal if env vars are cached
- NBA API (nba_fetch.py) has rate limits - script handles with 0.6s delays and is resumable
- Games from Odds API are auto-created with negative `sourceGameId` to avoid conflicts
- Player props use `description` field for player name, `name` field for Over/Under
