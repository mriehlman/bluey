# Prediction Governance

Last updated: 2026-03-21

This document defines non-optional contracts for Bluey prediction workflows.
If these contracts are violated, model outputs are considered invalid for
decision-making.

## Data Pipeline Contract (Critical)

All prediction outputs MUST be reproducible from:

raw data -> normalized DB -> feature snapshot -> pattern evaluation -> prediction

Rules:
1. No step may depend on implicit state or previous runs.
2. All derived values must be traceable to stored inputs.
3. Feature generation must be deterministic for a given `game_id`.
4. Backtests must use frozen historical snapshots (no retroactive mutation).
5. Odds and stats must be timestamped and never overwritten in-place.

Violations of this contract invalidate model results.

## Prediction Output Schema (Canonical)

A prediction record must include:

- `game_id`
- `market` (spread, total, player_prop, moneyline, etc)
- `selection` (e.g. OVER, HOME_COVER)
- `model_votes` (array of model decisions)
- `confidence_score` (0-1)
- `edge_estimate` (ROI/probability delta)
- `supporting_patterns` (pattern IDs)
- `prediction_contract_version`
- `ranking_policy_version`
- `aggregation_policy_version`
- `model_bundle_version`
- `feature_schema_version`
- `feature_snapshot_id` (traceability)
- `feature_snapshot_payload` (full canonical feature payload for replay)
- `generated_at` (timestamp)

No app may invent an incompatible prediction shape.

Version tags are required so field semantics and model logic remain reproducible
as the system evolves.

## Pattern Validation Rules

A pattern is valid only if:

- minimum sample size is met (default: >= 50),
- win-rate threshold is met (default: >= 0.524 + 0.005 buffer),
- tested on out-of-sample data,
- no future-leakage in inputs.

Optional scoring layers:

- recency weighting,
- cross-season stability,
- variance/drawdown sensitivity.

Patterns failing base validity rules must not drive actionable predictions.

## Model Layer Architecture

Models are independent evaluators that emit:

- yes/no decision,
- confidence score.

Current/planned evaluators include:

- `pattern_match_model`,
- `regression_model`,
- `matchup_model`,
- `trend_model`.

Final prediction is an explicit aggregation over model outputs.

Rules:

- models must not depend on each other,
- models must be individually testable,
- voting/aggregation logic must be explicit and configurable.

Aggregation policy (`aggregation_policy_version`) defines abstain semantics:

- abstain is allowed but contributes confidence penalty,
- at least two non-abstain votes are required,
- high-confidence "no" can veto prediction generation.

## Feature Snapshot System

For each evaluated game:

- persist or deterministically derive the full feature vector used by prediction,
- generate a stable `feature_snapshot_id`,
- link every prediction to that snapshot ID,
- attach/store canonical snapshot payload for exact replay.

Snapshot payload includes source-time visibility fields where available:

- `odds_timestamp_used`
- `stats_snapshot_cutoff`
- `injury_lineup_cutoff`

This is required for:

- exact replay,
- post-hoc debugging,
- auditability of historical output.

## Workflow Authority

The CLI is the only authoritative interface for:

- ingestion,
- feature generation,
- pattern discovery,
- backtests,
- prediction generation jobs.

Dashboard endpoints may query and visualize outputs, but must not become an
independent pipeline executor.

## Known Risk Areas

High-risk zones to verify before trusting outputs:

- odds ingestion mismatches (market naming/coverage gaps),
- event-to-dashboard routing ambiguity,
- media/browser format differences in sharing flows,
- cross-season data coverage gaps (for example, some player prop coverage starts later).

Agents and operators must verify these areas explicitly before asserting
pipeline correctness.
