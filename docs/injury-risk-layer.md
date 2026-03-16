# Injury Risk Layer (Model Layer 3)

## Model Layer Stack

1. Core Layer: Pattern V2 discovery and matching.
2. Meta Layer: calibrated probability scoring and market-aware ranking.
3. Injury Risk Layer: adjust confidence using player/team availability risk.
4. Lineup Context Layer (future): deeper on/off and rotation continuity signals.

This document focuses on Layer 3.

## Purpose

The injury risk layer captures pregame availability uncertainty that is not fully represented by pattern outcomes or market odds alone. It should improve both:

- pick quality (better filtering of unstable spots), and
- calibration (probabilities that better reflect real lineup volatility).

## Data Source (Current)

- Raw snapshots under `data/raw/injuries/YYYY-MM-DD.json`
- Source: `nbainjuries` snapshots pulled from official team injury reports
- Fields available per row:
  - `Game Date`
  - `Game Time`
  - `Matchup`
  - `Team`
  - `Player Name`
  - `Current Status`
  - `Reason`

## Phase 1 Scope (Simple, Useful)

Build a lightweight injury signal set for each team/game:

- counts by status: `out_count`, `questionable_count`, `probable_count`
- weighted injury burden score (example):
  - Out = 1.0
  - Doubtful = 0.75
  - Questionable = 0.5
  - Probable = 0.2
  - Available = 0.0
- non-injury filters:
  - tag and optionally downweight reasons like `G League`, `NOT YET SUBMITTED`
- key-player impact proxy:
  - weight status by player importance (PPG/APG usage proxy from existing context)

## Candidate Output Features

For each game side (`home`, `away`):

- `injury_burden_score`
- `injury_out_weighted_score`
- `questionable_weighted_score`
- `key_player_out_flag`
- `key_player_questionable_flag`
- `injury_report_completeness_flag` (false when report not yet submitted)

For matchup level:

- `injury_burden_delta` (home - away)
- `injury_uncertainty_flag` (high questionable concentration)

## How Layer 3 Should Be Used

Short-term integration points:

1. Prediction gating:
   - suppress or down-rank bets with high uncertainty.
2. Meta input expansion:
   - include injury burden and uncertainty as additional model inputs.
3. Pattern tokenization:
   - add injury context tokens for future Pattern V2 discovery.

## Guardrails

- Separate injury from non-injury reasons where possible.
- Keep text parsing deterministic and versioned.
- Preserve raw reason text for auditability.
- Avoid overfitting to one-off status labels.

## Immediate Next Steps

1. Create normalized injury table(s) from raw snapshots.
2. Build status/reason normalizer and mapping dictionary.
3. Compute per-game injury features and persist them.
4. Add simple gating rule in predictions route using uncertainty + burden.
5. Evaluate pre/post impact on hit rate, EV, and calibration.
