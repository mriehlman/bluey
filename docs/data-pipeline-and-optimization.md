# Data Pipeline & Pattern Discovery Optimization

## Pipeline Order

Pattern discovery depends on data being built in this order:

```
1. build:game-context      → GameContext, PlayerGameContext
2. build:game-events       → GameEvent (condition + outcome rows)
3. build:feature-bins      → FeatureBin (quantile/fixed bins for features)
4. build:quantized-game-features → GameFeatureToken (tokens per game)
5. search:discover-patterns-v2   → PatternV2 (candidates)
6. validate:patterns-v2    → PatternV2 status updates, PatternV2Hit rebuild
```

**Quick full run (from project root):**
```bash
bun run build:game-context -- --from-season 2023 --to-season 2026
bun run build:game-events -- --from-season 2023 --to-season 2026
bun run build:feature-bins -- --from-season 2023 --to-season 2026
bun run build:quantized-game-features -- --from-season 2023 --to-season 2026
bun run search:discover-patterns-v2 -- --cv loso --forward-from 2025
bun run validate:patterns-v2
```

---

## Data Cleanup Opportunities

### 1. GameEvent: condition vs outcome usage

- **Discovery** only uses `GameEvent` rows with `type = 'outcome'` (eventKey:side as outcomeType).
- **Condition events** are computed and stored but not used by pattern discovery.
- **Option A**: Stop storing condition events if nothing else uses them (check `predictGames`, `dailyPicks`).
- **Option B**: Keep them if needed for future features or other pipelines.

### 2. OrphanPlayerStat

- Logs stats that couldn't be matched to games.
- Consider pruning rows older than N seasons to keep table small.

### 3. Legacy tables

- `GamePattern` / `GamePatternHit` – v1 patterns. If fully migrated to PatternV2, consider archiving or removing.

### 4. Redundant indexes

- Run `EXPLAIN ANALYZE` on hot queries and ensure indexes exist for:
  - `GameEvent(gameId, type)` for outcome-only lookups
  - `GameFeatureToken(season, date)` for date-range discovery

### 5. Raw data (data/raw)

- `data/raw/odds/player-props/` and `data/raw/odds/historical/` can grow large.
- Consider compressing old files (gzip) or archiving seasons no longer needed for backfill.

---

## Performance Improvements (Implemented)

### PatternV2Hit rebuild – batch inserts

- **Before**: One `INSERT` per (pattern, game) match → O(n) round-trips.
- **After**: Collect matches in memory, bulk insert in batches of 2000.
- **Impact**: Large reduction in DB round-trips during validate.

### buildQuantizedGameFeatures – batch upserts

- **Before**: One upsert per game.
- **After**: Batch of 500 games per `INSERT ... ON CONFLICT`.
- **Impact**: Faster feature token build step.

---

## Further Optimizations (Future)

### loadGamesForFeatures

- Use `select` to fetch only needed columns (id, season, date, context, odds, playerContexts).
- Add streaming/chunking if memory is tight for large season ranges.

### loadTokenizedGames

- Single query with JOIN instead of two queries + in-memory merge:
  ```sql
  SELECT gft."gameId", gft.season, gft.date, gft.tokens, ge."eventKey", ge.side
  FROM "GameFeatureToken" gft
  JOIN "GameEvent" ge ON ge."gameId" = gft."gameId" AND ge.type = 'outcome'
  ```
- Add index on `GameEvent(gameId, type)` if missing.

### Discovery scoring loop

- Pre-index games by token sets for faster `matchesConditions` lookups.
- Profile tree vs FPGrowth; one may dominate runtime.

---

## Tuning Config

Central defaults now live in `src/config/tuning.ts`:

- `DISCOVERY_DEFAULTS` for `search:discover-patterns-v2`
- `VALIDATION_DEFAULTS` for `validate:patterns-v2`
- `PREDICTION_TUNING` for pick gating in dashboard predictions

You can tune max allowed negative odds via env:

```bash
SUGGESTED_PLAY_MAX_NEGATIVE_ODDS=-150
```

Rules:
- `-150` allows `-150`, `-140`, `+100`, etc.
- It blocks more expensive prices like `-160`, `-200`.
