# Vote Weight Input Diagnosis v1

This note documents where dynamic vote-weight inputs come from, where they can collapse to neutral, and what to audit.

## Input sources and neutral behavior

### 1) `reliabilityWeight`
- **Source data:** `PickSourceReliability` rows loaded via `loadSourceReliabilitySnapshot()` in `pickQuality.ts`.
- **Runtime mapping:** snapshot rows are transformed into a map in `buildVoteReliabilityMap()` in `predictionEngine.ts`.
- **Vote source mapping:** vote `model_id` is normalized by `normalizeVoteSourceFromModelId()` in `voteWeighting.ts`.
- **Computation:** `rawReliabilityWeight = reliabilityWeightFromHitRate(hitRate)` with conservative clamp `0.7..1.3`.
- **Neutral numeric value:** `1.0`.
- **Falls to neutral when:**
  - no reliability row matches vote source family
  - source family naming differs between reliability rows and runtime normalized vote family
  - no lane/global fallback row exists

### 2) `sampleConfidenceWeight`
- **Source data:** `sampleSize` from the matched reliability row.
- **Computation:** `clamp(sampleSize / sampleThreshold, 0, 1)` where `sampleThreshold` comes from `PICK_QUALITY_TUNING.sourceReliabilityMinSample`.
- **Neutral numeric value:** `1.0` (full confidence in reliability estimate).
- **Behavior:** this is a shrinkage factor; low sample shrinks reliability effect toward neutral in `reliabilityWeight = 1 + (raw - 1) * sampleConfidenceWeight`.
- **Can look neutral when:**
  - most matched rows are at or above threshold
  - most votes miss reliability rows entirely (then `sampleConfidenceWeight` is set to `1` in current code path)

### 3) `laneFitWeight`
- **Source data:** whether lane-specific or lane-wide reliability match is found.
- **Matching order in `computeWeightedVotes()`:**
  1. `sourceFamily::${laneTag}|${regimeSubset}` (exact lane+regime key)
  2. `sourceFamily::${laneTag}` (lane-wide key)
  3. `sourceFamily::global` (global fallback)
- **Computation:** `1.05` if lane-specific or lane-wide matched, else `1.0`.
- **Neutral numeric value:** `1.0`.
- **Can look neutral when:**
  - laneTag reliability rows are absent
  - lane tags in snapshot do not align with runtime lane tags
  - source family mismatch prevents lane lookup

### 4) `uncertaintyDiscount`
- **Source data:** `uncertaintyScore` already computed in pick-quality path.
- **Computation:** `clamp(1 - uncertaintyScore * 0.35, 0.65, 1)`.
- **Neutral numeric value:** `1.0` when uncertainty is near zero.
- **Usually non-neutral:** this was the main varying component in diagnostics.

## Where `PickSourceReliability` is loaded and used
- Loaded in `pickQuality.ts` via `loadSourceReliabilitySnapshot(windowKey)`.
- Converted to vote weighting map in `predictionEngine.ts` `buildVoteReliabilityMap(...)`.
- Consumed by `computeWeightedVotes(...)` in `voteWeighting.ts`.

## How lane-specific reliability is chosen vs fallback
- **Exact lane+regime key:** strongest match, usually unavailable unless explicitly materialized.
- **Lane key (`sourceFamily::laneTag`):** lane-specific boost path.
- **Global key (`sourceFamily::global`):** fallback match with no lane boost.
- **No match:** reliability defaults to neutral.

## Why weights can collapse to neutral
- Source family mismatch between reliability snapshot builder and runtime vote normalization.
- Sparse or missing lane-specific rows.
- Lookup keys present but with values close to neutral (hit rates close to 0.5).
- Shrinkage pulling non-neutral reliability toward 1 when sample size is low.

## Current diagnostic command
- `report:vote-weight-input-audit` prints:
  - reliability coverage (exact/fallback/neutral) by source family, lane, version, sample bucket
  - sample-shrinkage behavior (pre/post reliability weight)
  - raw source -> normalized family mapping frequencies
  - lane-fit activation rates
  - normalized support comparison (percentile rank within version)
