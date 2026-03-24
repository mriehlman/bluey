# Vote Weighting v1

## Current flow

- Votes are built in `packages/core/src/features/predictionEngine.ts` via `buildModelVotes()`.
- Current aggregation is in `aggregateVotes()` in the same file.
- Current logic:
  - remove abstains
  - hard veto on high-confidence `no`
  - average `yes` confidences
  - subtract abstain penalty
  - pass threshold at `0.55`

## Insertion point

- Keep `buildModelVotes()` unchanged.
- Replace the internals of `aggregateVotes()` conditionally.
- New integration call path:
  - `aggregateVotes(votes, weightingContext)`
  - if `PICK_QUALITY_TUNING.enableDynamicVoteWeighting` is false -> run legacy path exactly
  - if true -> call `computeWeightedVotes()` from `packages/core/src/features/voteWeighting.ts`

## What changes

- Add dynamic vote weighting based on:
  - normalized source family
  - source reliability (with sample-aware shrink)
  - uncertainty discount
  - lane fit bonus
  - family-level anti-domination aggregation
  - disagreement penalty

## What remains

- Vote object shape remains compatible (`model_id`, `decision`, `confidence`).
- Pass/fail threshold remains `0.55`.
- Legacy behavior preserved by default (flag OFF).
- New diagnostics are additive:
  - weighted support/opposition/consensus/disagreement
  - per-vote weight breakdown
