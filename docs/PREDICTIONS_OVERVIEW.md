# Bluey Predictions System — High-Level Overview

## What It Does

The system discovers **predictive patterns** in NBA game data and uses them to generate **actionable predictions** for upcoming or past games. Patterns take the form: *"When conditions X, Y, Z are true, outcome O tends to happen at rate R."*

Example: *"When the home team has bottom-10 defense AND is on a 5-game losing streak, the away team's top rebounder gets 10+ rebounds in ~72% of games."*

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA INGESTION                                      │
│  games.json, playerstats.json, odds APIs → Prisma → Game, Player, GameContext,   │
│  PlayerGameContext, GameOdds, PlayerPropOdds                                     │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            BUILD GAME EVENTS                                     │
│  For each completed game with context: evaluate condition & outcome events       │
│  from the catalog → GameEvent rows (condition/outcome per game)                  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          SEARCH GAME PATTERNS                                    │
│  Mine condition→outcome combinations from GameEvent; score & persist as          │
│  GamePattern; record hits in GamePatternHit                                      │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          PREDICTIONS API                                         │
│  For a given date: load games, compute team snapshots, match patterns,           │
│  enrich with player targets, prop lines, recent performance, outcome results     │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          DASHBOARD UI                                            │
│  Date picker, game cards, expandable predictions with hit/miss, player names,    │
│  prop lines, recent performance                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Concepts

### 1. Conditions

**Conditions** are pre-game facts about the matchup, derived from `GameContext` and team stats:

- **Team tiers**: `TOP_5_OFF`, `TOP_10_DEF`, `BOTTOM_10_DEF`, etc. (based on offensive/defensive rankings)
- **Streaks**: `WIN_STREAK_5`, `LOSING_STREAK_5`
- **Rest/B2B**: `ON_B2B:home`, `BOTH_ON_B2B:game`, `RESTED_3_PLUS:away`
- **Scoring**: `HIGH_SCORING:home`, `STINGY_DEF:away`, `BOTH_HIGH_SCORING:game`
- **Win records**: `WINNING_RECORD:home`, `WIN_PCT_OVER_600:away`

Each condition is tied to a **side**: `home`, `away`, or `game`.

### 2. Outcomes

**Outcomes** are things that happened in the game (or could happen). They come in two flavors:

- **Game-level**: `HOME_COVERED`, `OVER_HIT`, `MARGIN_UNDER_10`, `UNDERDOG_COVERED`
- **Player-level (team-specific)**: `HOME_TOP_REBOUNDER_10_PLUS`, `AWAY_TOP_SCORER_25_PLUS`, `HOME_TOP_PLAYMAKER_8_PLUS`

The catalog filters out **generic player outcomes** (e.g. "any player gets 10+ rebounds") because they are not actionable—you need to know *which* team’s player.

### 3. Game Event Catalog

`gameEventCatalog.ts` defines all condition and outcome events. Each event has:

- A **key** (e.g. `TOP_10_DEF`)
- A **type**: `condition` or `outcome`
- **Sides** it applies to
- A **compute function** that takes `GameEventContext` and returns `{ hit: boolean, meta?: object }`

Conditions use `GameContext` (wins, losses, ppg, oppg, ranks, streaks, rest, B2B). Outcomes use final scores, odds, and player stats.

### 4. Team Snapshots

For **upcoming games**, there is no `GameContext` yet. The predictions API builds **team snapshots** on the fly:

- Uses historical games (same season, before game date)
- Computes wins, losses, ppg, oppg
- Derives offense/defense rankings and streaks

Team IDs can differ between schedule data and historical data; the API looks up teams by **code** (e.g. `WAS`) and queries all matching IDs to get correct history.

---

## Data Flow (Detail)

### Step 1: Build Game Events

**CLI**: `bun run cli build:game-events`

- Loads games with context, player contexts, stats, odds
- For each game, runs every event in `GAME_EVENT_CATALOG`
- Writes `GameEvent` rows: `(gameId, season, eventKey, type, side, meta)`

### Step 2: Search Game Patterns

**CLI**: `bun run cli search:discover-patterns-v2 -- --cv loso --forward-from 2025`

- Loads all `GameEvent` rows
- Builds inverted indexes: condition key → set of game IDs, outcome key → set of game IDs
- Enumerates condition combinations (1–3 legs) with enough frequency
- For each combo, intersects game sets with each outcome
- Computes hit rate, sample size, per-season breakdown
- Filters by min hit rate (58%), min sample (15)
- Scores patterns (confidence, value)
- Writes `GamePattern` and `GamePatternHit` rows

### Step 3: Predictions API

**Endpoint**: `GET /api/predictions?date=YYYY-MM-DD`

1. Load games for the date (with teams, odds, context)
2. Compute team snapshots for all teams in those games
3. Load all `GamePattern`s
4. For each game, determine which conditions are active from snapshots
5. Match patterns where *all* conditions are active
6. **Deduplicate** by outcome (keep best pattern per outcome)
7. **Enrich** each prediction:
   - **Player target**: For player outcomes, resolve top scorer/rebounder/playmaker from `PlayerGameContext`
   - **Prop line**: If `PlayerPropOdds` exist, attach the relevant line
   - **Recent performance**: Last 30 days hit/miss from `GamePatternHit`
   - **Result**: For completed games, check `GameEvent` for the outcome and compute hit/miss
8. Return JSON for the dashboard

---

## Dashboard Features

| Feature | Description |
|--------|-------------|
| **Date picker** | View predictions for any date |
| **Game cards** | Matchup, score (if final), odds, team context (record, ranks, streak) |
| **Matching patterns** | One prediction per unique outcome; conditions + outcome + hit rate + edge |
| **Player target** | For player outcomes: "Jakob Poeltl (8.2 rpg)" |
| **Prop line** | Betting line when available, e.g. "Line: 9.5 rebounds" |
| **Recent performance** | Last 30 days hit rate (e.g. "8/12") vs historical |
| **High value badge** | Underdog/favorite cover patterns, or 65%+ hit rate with 100+ sample |
| **Hit/Miss** | For completed games: green/red badge + explanation |

---

## Important Implementation Notes

- **Game date**: All game dates use **Eastern time** (America/New_York). A game that tips at 10:30 PM PT on March 1 is stored as March 1 (same calendar day in ET). This matches NBA/TV convention.
- **Season definition**: Season = year it *starts* (e.g. 2025-26 season → 2025)
- **Team ID mapping**: Schedule and historical data can use different team IDs; lookups use team code
- **Player outcomes**: Only team-specific outcomes (e.g. `HOME_TOP_REBOUNDER_10_PLUS`) are shown
- **Deduplication**: One pattern per outcome to avoid redundant "close game" predictions

---

## Key Files

| File | Purpose |
|------|---------|
| `packages/core/src/features/gameEventCatalog.ts` | Event definitions (conditions + outcomes) |
| `packages/core/src/features/buildGameEvents.ts` | Builds `GameEvent` from completed games |
| `packages/core/src/patterns/discoveryV2.ts` | Mines and persists pattern candidates |
| `apps/dashboard/src/app/api/predictions/route.ts` | Predictions API |
| `apps/dashboard/src/app/page.tsx` | Predictions UI |

---

## Recommended Pipeline (Data Freshness)

**One command to catch up** (no args needed):

```bash
bun run cli sync:catchup
```

This auto-detects the gap and syncs stats + odds from the last synced date through yesterday.

**Data sources:** Stats and games use **nba_api** (Python); odds use **Odds API**. BallDontLie is not required.

**Individual commands** (when run without `--date`, they auto-backfill):

| Command | Purpose |
|---------|---------|
| `bun run cli sync:stats` | Sync player stats via nba_api. No args = backfill from last stats date through yesterday |
| `bun run cli sync:odds` | Sync game odds via Odds API. No args = fetch live + backfill historical gap |
| `bun run cli sync:stats -- --date 2026-03-01` | Sync stats for a specific date |
| `bun run cli sync:odds -- --date 2026-03-01` | Sync odds for a specific date |

**Full pipeline** (run after catchup):

| Command | Purpose |
|---------|---------|
| `bun run cli build:game-context` | Build `GameContext` and `PlayerGameContext` for completed games |
| `bun run cli build:game-events` | Compute condition/outcome events for pattern mining |
| `bun run cli search:discover-patterns-v2 -- --cv loso --forward-from 2025` | Mine and persist new patterns |

**Note:** For upcoming games, the predictions API computes team snapshots and player context on-the-fly when `PlayerGameContext` is missing, so player names will still appear.
