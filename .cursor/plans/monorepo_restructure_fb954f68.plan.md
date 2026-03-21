---
name: Monorepo restructure
overview: Restructure the project into a bun workspace monorepo with a shared core library, keeping the dashboard and CLI as separate apps that import from the same packages instead of duplicating logic or spawning child processes.
todos:
  - id: workspace-root
    content: "Set up bun workspace root: package.json with workspaces field, tsconfig.base.json with shared compiler options"
    status: completed
  - id: pkg-db
    content: Create packages/db/ - move prisma schema, create client singleton export, package.json for @bluey/db
    status: completed
  - id: pkg-core
    content: Create packages/core/ - move all src/ modules (except cli/db/tests), update all db/prisma imports to @bluey/db
    status: completed
  - id: app-cli
    content: Create apps/cli/ - move CLI index.ts, update all imports to @bluey/core/...
    status: completed
  - id: app-dashboard
    content: Move dashboard to apps/dashboard/ - delete its prisma schema, replace ../../../../../src/ imports with @bluey/core, replace child_process with direct imports, add transpilePackages
    status: completed
  - id: cleanup
    content: Remove old src/ and prisma/ directories, update root scripts, run bun install and verify everything builds
    status: completed
isProject: false
---

# Monorepo Restructure

## Target Structure

```
bluey/
├── package.json              (workspace root)
├── tsconfig.base.json        (shared compiler options)
├── docker-compose.yml
├── data/                     (raw data files, stays at root)
├── config/                   (pattern-ideas.json, stays at root)
├── docs/
│
├── packages/
│   ├── db/
│   │   ├── package.json      (@bluey/db)
│   │   ├── tsconfig.json
│   │   ├── prisma/
│   │   │   └── schema.prisma (THE single schema)
│   │   └── src/
│   │       └── index.ts      (prisma client singleton + re-export types)
│   │
│   └── core/
│       ├── package.json      (@bluey/core)
│       ├── tsconfig.json
│       └── src/
│           ├── api/          (external HTTP clients: balldontlie, odds, nba, injuries)
│           ├── backtest/     (backtesting, walk-forward, P&L simulation)
│           ├── config/       (tuning.ts defaults)
│           ├── features/     (prediction engine, game events, injury context, daily picks)
│           ├── ingest/       (all sync functions: games, stats, odds, props, injuries, lineups)
│           ├── patterns/     (discovery, meta model, model versions, scoring)
│           ├── profiles/     (player/team profiles)
│           ├── reports/      (coverage, probability quality, prediction accuracy)
│           └── stats/        (filters, rollups)
│
├── apps/
│   ├── cli/
│   │   ├── package.json      (@bluey/cli)
│   │   ├── tsconfig.json
│   │   └── src/
│   │       └── index.ts      (thin CLI: parse args, call @bluey/core)
│   │
│   └── dashboard/
│       ├── package.json      (bluey-dashboard)
│       ├── tsconfig.json
│       ├── next.config.ts
│       └── src/
│           ├── app/          (pages + API routes)
│           └── lib/          (formatters, team logos)
│
└── scripts/                  (standalone debug scripts)
```

## What Changes and Why

### 1. Single Prisma schema (`packages/db/`)

Currently there are two schemas that drift apart (`prisma/schema.prisma` and `dashboard/prisma/schema.prisma`). The `@bluey/db` package owns the single schema and generated client. Both apps depend on it.

- Move `prisma/schema.prisma` into `packages/db/prisma/`
- Delete `dashboard/prisma/` entirely
- `packages/db/src/index.ts` exports the singleton client and re-exports Prisma types
- Both `@bluey/core` and `apps/dashboard` depend on `@bluey/db`

### 2. Core library (`packages/core/`)

Move everything from current `src/` (except `cli/`, `db/`, `tests/`) into `packages/core/src/`. Internal imports stay the same (relative paths within the package). The package.json exports entry points:

```json
{
  "name": "@bluey/core",
  "exports": {
    "./ingest/*": "./src/ingest/*.ts",
    "./features/*": "./src/features/*.ts",
    "./patterns/*": "./src/patterns/*.ts",
    "./config/*": "./src/config/*.ts",
    "./api/*": "./src/api/*.ts",
    "./backtest/*": "./src/backtest/*.ts",
    "./stats/*": "./src/stats/*.ts",
    "./reports/*": "./src/reports/*.ts",
    "./profiles/*": "./src/profiles/*.ts"
  },
  "dependencies": {
    "@bluey/db": "workspace:*",
    "nba-api-client": "^1.1.2"
  }
}
```

All internal `import { prisma } from "../db/prisma.js"` becomes `import { prisma } from "@bluey/db"`.

### 3. CLI app (`apps/cli/`)

Move `src/cli/index.ts` to `apps/cli/src/index.ts`. It imports from `@bluey/core` instead of relative paths:

```typescript
// Before: import { syncOddsLive } from "../ingest/syncOdds.js"
// After:
import { syncOddsLive } from "@bluey/core/ingest/syncOdds";
```

Root `package.json` scripts change from `bun run src/cli/index.ts` to `bun run apps/cli/src/index.ts` (or a workspace script alias).

### 4. Dashboard app (`apps/dashboard/`)

Move `dashboard/` into `apps/dashboard/`. Key changes:

- **Remove `dashboard/prisma/`** -- depend on `@bluey/db` instead
- **Remove `dashboard/src/lib/prisma.ts`** -- import from `@bluey/db`
- **Replace all `../../../../../src/` imports** with `@bluey/core/...`:

```typescript
// Before:
import { generateGamePredictions } from "../../../../../src/features/predictionEngine";
import { LEDGER_TUNING } from "../../../../../src/config/tuning";

// After:
import { generateGamePredictions } from "@bluey/core/features/predictionEngine";
import { LEDGER_TUNING } from "@bluey/core/config/tuning";
```

- **Remove ALL child_process spawning** from `/api/sync` and `/api/predictions` routes. Import sync functions directly:

```typescript
import { syncUpcomingFromNba } from "@bluey/core/ingest/syncNbaStats";
import { syncOddsLive } from "@bluey/core/ingest/syncOdds";
```

- **Remove duplicate logic** from `/api/model-versions` -- import from `@bluey/core/patterns/modelVersion`

### 5. Workspace root (`package.json`)

```json
{
  "name": "bluey-workspace",
  "private": true,
  "workspaces": [
    "packages/*",
    "apps/*"
  ],
  "scripts": {
    "cli": "bun run apps/cli/src/index.ts",
    "dev": "bun run --filter bluey-dashboard dev",
    "db:up": "docker compose up -d",
    "db:down": "docker compose down",
    "prisma:migrate": "bunx prisma migrate dev --schema packages/db/prisma/schema.prisma",
    "prisma:generate": "bunx prisma generate --schema packages/db/prisma/schema.prisma"
  }
}
```

All the 40+ `sync:*`, `build:*`, `predict:*` scripts go away from root. You run them as:

```bash
bun run cli sync:odds
bun run cli predict:today --date 2026-03-21
```

### 6. Next.js config (ALL THREE required)

```typescript
import path from "path";
const nextConfig: NextConfig = {
  transpilePackages: ["@bluey/core", "@bluey/db"],
  outputFileTracingRoot: path.join(__dirname, "../../"),
  serverExternalPackages: ["@prisma/client"],
};
```

- `transpilePackages` — workspace packages are raw .ts, not pre-built
- `outputFileTracingRoot` — prevents "multiple lockfiles" warning, sets correct workspace root
- `serverExternalPackages` — prevents webpack from bundling Prisma (which breaks the native query engine DLL lookup)

### 7. Prisma schema generator output

```prisma
generator client {
  provider = "prisma-client-js"
  output   = "../../../node_modules/@prisma/client"
}
```

Without this, bun puts the generated client in `.bun/` cache subdirectories that Next.js cannot resolve at runtime, causing `PrismaClientInitializationError: could not locate Query Engine`.

### 8. @bluey/core must list @prisma/client as a direct dependency

Transitive deps through `@bluey/db` do NOT resolve in Next.js webpack `transpilePackages`. Any package that imports `@prisma/client` types needs it in its own `package.json`.

## Execution Order

**CRITICAL: See `docs/monorepo-migration-postmortem.md` for the full runbook with audit steps.**

The first attempt wasted ~$50 in iterative rebuild cycles because it skipped the audit. The correct order is:

### Pre-work (BEFORE creating any directories)
1. Stop all running processes (dev servers, watchers)
2. Full import audit: `.js` extensions, `@prisma/client` direct imports, `child_process`, `process.cwd()`, hardcoded paths
3. Full type check: `tsc --noEmit` to surface ALL errors
4. Dependency audit: list every non-relative import
5. Fix ALL of the above in the old structure first — strip `.js` extensions, fix type errors, centralize `getDataDir()`, replace `child_process`

### Migration
1. Set up workspace root config (package.json workspaces, tsconfig.base.json)
2. Create `packages/db/` — move prisma schema (with `output` set!), create client export
3. Create `packages/core/` — move all `src/` modules, update `db/prisma` imports to `@bluey/db`
4. Create `apps/cli/` — move CLI, update imports to `@bluey/core`
5. Move dashboard to `apps/dashboard/` — copy CONTENTS not directory (avoids nesting), delete prisma/, delete lib/prisma.ts, delete package-lock.json, update imports, set all 3 next.config.ts options
6. Clean up root — remove old `src/`, `prisma/`, `dashboard/`

### Verify (ONE round)
1. `bun install`
2. `bunx prisma generate --schema packages/db/prisma/schema.prisma`
3. Verify `node_modules/@prisma/client/query_engine-windows.dll.node` exists
4. `tsc --noEmit` on each package, capture ALL errors, fix ALL at once
5. `next build` on dashboard, capture ALL errors, fix ALL at once
6. `bun run cli --help`
7. `bun run dev` and hit `localhost:3000`

## What Does NOT Change

- All core logic stays exactly the same (no refactoring of business logic)
- The `data/`, `config/`, `docs/`, `scripts/` directories stay at root
- Long-running operations (discovery, backtest, training) remain CLI-only
- The dashboard UI (`page.tsx`, `simulator/page.tsx`) stays the same
- Docker/postgres setup unchanged

