# Pick Quality Framework v1 (Insertion Plan)

This note documents the additive insertion points for improving pick quality without breaking current behavior.

## Current Pipeline Insertion Points

1. Candidate pick creation
- `packages/core/src/features/predictionEngine.ts` in `generateGamePredictions()`, `allPlayMap` + `allRankedPlays`.

2. Vote combination
- `packages/core/src/features/predictionEngine.ts` in `buildModelVotes()` and `aggregateVotes()`.

3. Confidence/meta scoring
- `packages/core/src/features/predictionEngine.ts` in `allRankedPlays` mapping.
- `packages/core/src/patterns/metaModelCore.ts` via `scoreMetaModel()`.

4. Actionability gates
- `packages/core/src/features/productionPickSelection.ts` in `evaluateSuggestedPlayQualityGate()`.
- `packages/core/src/features/predictionEngine.ts` where quality/bettable filtering is applied.

5. Canonical persistence
- `packages/core/src/features/predictGames.ts` in `persistCanonicalPredictions()`.

6. Grading/ledger joins
- `apps/dashboard/src/app/api/predictions/route.ts` in `upsertSuggestedPlayLedger()`.
- `apps/dashboard/src/app/api/simulator/route.ts` for canonical-latest + ledger joins.

## Additive v1 Design

- Keep legacy gating behavior as default.
- Add market-relative and calibration/uncertainty fields to in-memory pick objects and persist additively.
- Add calibration and source reliability artifact tables.
- Add lane/regime tagging for all picks.
- Add strict actionability path behind config (`enableStrictActionabilityGates`).

## New v1 Layers

1. Market-relative scoring
- Compute and persist:
  - `rawWinProbability`
  - `calibratedWinProbability`
  - `impliedMarketProbability`
  - `edgeVsMarket`
  - `expectedValueScore`
  - `marketType`, `marketSubType`, `selectionSide`
  - `lineSnapshot`, `priceSnapshot`

2. Calibration
- Load lane/family calibrator from `PredictionCalibration`.
- Fallback order: lane -> market family -> global -> raw passthrough.

3. Uncertainty
- Compute `uncertaintyScore`.
- Apply additive penalty to edge:
  - `uncertaintyPenaltyApplied`
  - `adjustedEdgeScore`

4. Source reliability
- Build historical snapshot table `PickSourceReliability`.
- Load latest snapshot for scoring metadata.
- First pass is reporting-first; optional gate weighting is additive.

5. Lane/regime tags
- Persist `laneTag` and `regimeTags` for analysis/reporting.

## Non-breaking contract policy

- Existing fields and API paths remain valid.
- New fields are additive and nullable where data is unavailable.
- Strict gating is config-controlled and disabled by default.
