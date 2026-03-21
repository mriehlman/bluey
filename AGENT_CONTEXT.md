# Bluey Agent Context (Rolling)

Last updated: 2026-03-21
Owner: Any agent that changes structure, workflows, or core behavior.

## Purpose

This file is a living handoff for all AI agents working in this repo.  
It explains the project vision, current architecture, and the rules for keeping this file accurate over time.

## Product Vision

Bluey is an NBA prediction and betting-intelligence platform that:
- ingests games, stats, odds, injuries, and lineup context,
- mines and validates predictive patterns,
- produces model-driven picks and confidence diagnostics,
- exposes results via dashboard APIs and CLI workflows.

Primary goal: reliable, reproducible, evidence-based prediction workflows.  
Secondary goal: fast iteration without breaking data quality or model lineage.

## Current Monorepo Structure

```text
apps/
  cli/          -> command router and operator workflows
  dashboard/    -> Next.js app + API routes
packages/
  core/         -> ingestion, features, patterns, backtests, reports
  db/           -> shared Prisma schema + Prisma client exports
config/         -> pattern ideas and other config files
data/           -> local raw + derived data artifacts
scripts/        -> operational scripts (python + helpers)
docs/           -> technical notes and migration docs
```

## Package Responsibilities

- `@bluey/db`
  - source of truth for schema: `packages/db/prisma/schema.prisma`
  - exports shared Prisma client from `packages/db/src/index.ts`

- `@bluey/core`
  - all domain logic: ingest, prediction features, pattern discovery, backtesting
  - no app-specific UI concerns

- `apps/cli`
  - orchestrates operational commands
  - imports core/db packages; should not duplicate business logic

- `apps/dashboard`
  - serves APIs/UI; may call core functions directly
  - must remain compatible with monorepo pathing (repo-root-aware file access)

## Operational Standards

- Package manager: Bun only.
- Use `bun install`, `bun run ...`, and `bunx ...`.
- Root `preinstall` enforces Bun usage.
- Prisma commands must use `packages/db/prisma/schema.prisma`.

## Architecture Invariants (Do Not Break)

1. Single Prisma schema location under `packages/db/prisma/`.
2. Shared DB client comes from `@bluey/db`, not app-local Prisma clients.
3. Cross-package imports use package aliases (`@bluey/core/...`, `@bluey/db`), not old root `src/` paths.
4. File-system paths used by shared code must be repo-root aware (avoid raw `process.cwd()` assumptions).
5. CLI remains the source for repeatable batch workflows.

## Prediction Governance (Critical)

Bluey prediction outputs follow a mandatory governance contract:

- Reproducible pipeline: raw data -> normalized DB -> feature snapshot -> pattern evaluation -> prediction.
- Canonical prediction shape is shared across producers/consumers.
- Pattern validity requires minimum samples, threshold hit-rate, out-of-sample checks, and leakage guards.
- Feature snapshots must be traceable from each generated prediction.
- Dashboard is a consumer/view layer; CLI remains workflow authority.

Detailed specification: `docs/prediction-governance.md`.

## Agent Update Protocol (Mandatory)

When an agent makes meaningful changes, it must update this file in the same task if any of the following changed:
- folder/package layout,
- command workflows,
- key data flow assumptions,
- core model/prediction pipeline behavior,
- operational standards or invariants.

Minimum update checklist:
1. Update `Last updated` date.
2. Edit any affected section(s) above.
3. Add one line to the rolling log below.
4. Keep entries factual; no secrets, keys, or personal data.

## Rolling Change Log

- 2026-03-21: Created rolling agent context file for monorepo vision, architecture, and maintenance protocol.
- 2026-03-21: Added dashboard user auth baseline (Google/Apple via NextAuth), top-nav profile entry, and persisted user settings API/page.
- 2026-03-21: Split dashboard landing (`/`) from gated predictions (`/predictions`) and added NextAuth middleware for page-level access control.
- 2026-03-21: Stabilized dashboard dev workflow by reverting to webpack dev (`next dev`) with disabled dev cache, and made auth providers conditional on configured env credentials.
- 2026-03-21: Added non-production dev auth bypass (`DEV_AUTH_BYPASS`) with a seeded DB-backed dev user and landing-page "Continue in Dev Mode" sign-in.
- 2026-03-21: Improved live pattern matching parity by extending pregame token generation with additional discovery-aligned features (streak, playmaking resilience, and role-dependency approximation).
- 2026-03-21: Added formal prediction governance contract doc and mirrored critical governance rules in rolling agent context.
- 2026-03-21: Hardened governance with contract/version metadata, snapshot payload replay linkage, stricter model vote semantics, leakage metadata checks, replay test, and rejected-pattern reason diagnostics.
- 2026-03-21: Promoted canonical predictions to DB system-of-record with deterministic prediction IDs, ranking/aggregation policy versions, persisted rejection diagnostics, and source-time snapshot metadata.
- 2026-03-21: Added dashboard governance lineage read path (API + UI) for canonical predictions and rejections, including per-prediction version/snapshot inspection with read-only constraints.
- 2026-03-21: Extended governance lineage with run grouping, compare/diff support, export actions, prominent version chips, and summary-vs-detail payload separation for dashboard performance.

## Quick Start Commands (Current)

```bash
bun install
bun run db:up
bun run prisma:generate
bun run dev
```

CLI pattern:

```bash
bun run cli <command> -- <flags>
```

