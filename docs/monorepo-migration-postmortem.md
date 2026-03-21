# Bun Workspace Monorepo Migration — Runbook & Lessons Learned

> This document exists because the first attempt burned ~$50 in wasted rebuild cycles.
> If you ever need to do this again, follow Phase 1-4 exactly. Do NOT skip the audit.

---

## Phase 1: Audit (DO NOT MOVE ANY FILES)

Stop every running process first: dev servers, watchers, terminals. Verify with `Get-Process node` or equivalent.

Then run every one of these scans and capture the output. Do not proceed until you understand every result.

### 1A. Import pattern scan

**CRITICAL: Search the ENTIRE repo, not just `src/`.** The first attempt missed `scripts/`, dynamic `import()` calls, and `.gitignore` paths.

```bash
# .js extensions on relative imports — Next.js webpack won't resolve .js → .ts
# Search EVERYWHERE, not just src/
rg 'from ["'"'"']\./.*\.js|from ["'"'"']\.\./.*\.js' src/ scripts/ dashboard/src/ --count

# Dynamic imports too — these won't be caught by "from" grep
rg 'import\(["'"'"']\.\.' src/ scripts/ dashboard/src/ --count

# prisma imports — need to become @bluey/db
rg 'from.*db/prisma' src/ scripts/ dashboard/src/ --count

# @prisma/client type imports — package that uses these needs @prisma/client in its own deps
rg 'from "@prisma/client"' src/ scripts/ -l

# child_process — must be replaced with direct function imports
rg 'child_process' src/ dashboard/src/ -l

# require() calls — may break under ESM
rg 'require\(' src/ -l

# Duplicate function definitions that should be centralized
rg 'function getDataDir' src/ -l

# Any file referencing src/ or prisma/ or dashboard/ by path — catches .gitignore, CI, docker, etc.
rg 'src/|prisma/' .gitignore docker-compose.yml .github/ --count 2>/dev/null
```

### 1B. Runtime path scan

```bash
# Any function using cwd-relative paths will break when called from apps/dashboard
rg 'process\.cwd|__dirname|path\.join|path\.resolve|\./data|DATA_DIR' src/ --no-filename
```

Every hit needs to be changed to either use `process.env.DATA_DIR` or a shared `getDataDir()` that walks up to find the repo root.

### 1C. Full type check

```bash
# Bun is lenient. Next.js tsc --strict is not. Surface ALL errors now.
bunx tsc --noEmit 2>&1 > type_errors.txt
```

Read the ENTIRE output. Fix every error BEFORE moving files. Common pre-existing issues we hit:
- `nba-api-client` types missing fields (`box.status`, `stat.firstName`, `stat.familyName`) — cast with `as any`
- Prisma models that were removed but still referenced (`prisma.night`, `prisma.nightEvent`) — cast or guard
- Interface missing fields (`PlayerTarget` missing `id`) — add the field

### 1D. Dependency scan

```bash
# Every non-relative import in src/ = a dependency for @bluey/core's package.json
rg '^import.*from ["'"'"']@' src/ --no-filename | sort -u
rg '^import.*from ["'"'"'][a-z]' src/ --no-filename | sort -u  # node builtins + npm packages
```

The result tells you exactly what goes in `packages/core/package.json` dependencies. We missed `@prisma/client` — transitive deps through workspace packages do NOT resolve in Next.js webpack.

---

## Phase 2: Fix In-Place (STILL IN OLD STRUCTURE)

Do all of these before creating any new directories:

1. **Strip `.js` from all relative imports** — 62 occurrences in our case
2. **Fix all type errors** from Phase 1C — fix them ALL, not one at a time
3. **Centralize `getDataDir()`** — create `src/config/paths.ts`, update all 8 files that had local copies
4. **Replace `child_process` spawning** in dashboard routes with direct function imports
5. **Verify**: `bunx tsc --noEmit` should be clean (or only show pre-existing non-blocking warnings)

---

## Phase 3: Create Structure and Move

### 3A. Workspace root

```jsonc
// package.json
{
  "name": "bluey-workspace",
  "private": true,
  "workspaces": ["packages/*", "apps/*"],
  "scripts": {
    "cli": "bun run apps/cli/src/index.ts",
    "dev": "cd apps/dashboard && bun run dev",
    "prisma:generate": "bunx prisma generate --schema packages/db/prisma/schema.prisma",
    "prisma:push": "bunx prisma db push --schema packages/db/prisma/schema.prisma"
  }
}
```

Create `tsconfig.base.json` with shared compiler options.

### 3B. packages/db

- Copy `prisma/schema.prisma` → `packages/db/prisma/schema.prisma`
- **CRITICAL**: Set custom output in the generator block:

```prisma
generator client {
  provider = "prisma-client-js"
  output   = "../../../node_modules/@prisma/client"
}
```

Without this, bun puts the generated client in `.bun/` cache subdirectories that Next.js cannot find. This caused a runtime `PrismaClientInitializationError: could not locate Query Engine` crash that only appeared AFTER the build succeeded. This is the single most insidious issue — it looks like everything works until you actually load a page.

- Create `packages/db/src/index.ts` — prisma singleton + re-export types
- `package.json`: name `@bluey/db`, deps: `@prisma/client`

### 3C. packages/core

- Copy all `src/` subdirs (except `cli/`, `db/`, `tests/`) → `packages/core/src/`
- Replace all `from "../db/prisma"` → `from "@bluey/db"` (was 29 files)
- `package.json` deps MUST include both `@bluey/db: workspace:*` AND `@prisma/client` (for type imports)
- `package.json` exports: wildcard patterns for each subdirectory

### 3D. apps/cli

- Copy `src/cli/index.ts` → `apps/cli/src/index.ts`
- Replace all relative imports with `@bluey/core/...` (no `.js` extensions)
- Replace `from "../db/prisma.js"` → `from "@bluey/db"`

### 3E. apps/dashboard

- Copy dashboard contents (NOT the directory itself — avoids `apps/dashboard/dashboard/` nesting)
- **Delete**: `prisma/` dir, `src/lib/prisma.ts`, `package-lock.json`
- **Replace**: all `@/lib/prisma` imports → `@bluey/db`
- **Replace**: all `../../../../../src/` imports → `@bluey/core/...`
- **Replace**: all `child_process` spawn code → direct function imports
- **next.config.ts** — ALL THREE of these are required from the start:

```typescript
const nextConfig: NextConfig = {
  transpilePackages: ["@bluey/core", "@bluey/db"],
  outputFileTracingRoot: path.join(__dirname, "../../"),
  serverExternalPackages: ["@prisma/client"],
};
```

| Config | Why |
|--------|-----|
| `transpilePackages` | Workspace packages are raw TypeScript, not pre-built |
| `outputFileTracingRoot` | Without it, Next.js picks the wrong workspace root and warns about multiple lockfiles |
| `serverExternalPackages` | Without it, webpack bundles @prisma/client and can't find the native query engine DLL |

### 3F. Cleanup

- Delete old `src/`, `prisma/`, `dashboard/` from root
- Remove stale lockfiles (`package-lock.json` in app dirs)

---

## Phase 4: Verify (ONE round, not twelve)

```bash
bun install
bunx prisma generate --schema packages/db/prisma/schema.prisma

# Verify engine DLL exists at standard path
ls node_modules/@prisma/client/query_engine-windows.dll.node

# Type check ALL packages in one pass
bunx tsc --noEmit --project packages/core/tsconfig.json 2>&1 | tee core_errors.txt
bunx tsc --noEmit --project apps/cli/tsconfig.json 2>&1 | tee cli_errors.txt

# Build dashboard
cd apps/dashboard && bunx next build 2>&1 | tee build_output.txt

# Test CLI
bun run cli --help
```

If errors appear: read ALL of them from the output files, fix ALL of them, THEN rebuild ONCE.

---

## Specific Gotchas Reference

| Issue | Symptom | Root Cause | Fix |
|-------|---------|------------|-----|
| `.js` extensions | `Module not found: Can't resolve './utils.js'` | Next.js webpack doesn't resolve .js → .ts | Strip `.js` from all relative imports before moving |
| Missing `@prisma/client` dep | `Cannot find module '@prisma/client'` | Transitive deps don't resolve in webpack | Add `@prisma/client` to `@bluey/core` package.json |
| Prisma engine not found | `PrismaClientInitializationError: could not locate Query Engine` at RUNTIME | Bun puts generated client in `.bun/` cache, not `node_modules/@prisma/client` | Set `output = "../../../node_modules/@prisma/client"` in schema generator block |
| Wrong `cwd` paths | `ENOENT: no such file or directory './data/...'` | `process.cwd()` = `apps/dashboard`, not repo root | Centralize `getDataDir()` to walk up and find repo root |
| Pre-existing type errors | Various `Property does not exist` errors one at a time | Bun compiles loose, Next.js uses strict tsc | Run `tsc --noEmit` BEFORE moving, fix ALL errors at once |
| Locked directory | `Cannot remove item: being used by another process` | Dev server still running from old location | Kill all processes BEFORE starting migration |
| Wrong workspace root | Next.js warning about multiple lockfiles | Stale `package-lock.json` in subdirs | Delete them; set `outputFileTracingRoot` |
| Nested copy | `apps/dashboard/dashboard/src/...` | `Copy-Item dashboard apps/dashboard` copies the dir INTO the target | Copy the CONTENTS, not the directory |
| Stale dynamic import in CLI | `await import("../api/oddsApi.js")` at runtime | Dynamic `import()` not caught by static import rewrite pass | Grep for `import(` in addition to `from` — dynamic imports need the same rewrite |
| Duplicate `getDataDir()` survived | `buildGameContext.ts` kept its local copy | Only grepped `ingest/` for local `getDataDir()`, missed `features/` | Grep the ENTIRE `src/` tree for the function name, not just known directories |
| Scripts with old `../src/` imports | `parityCheck.ts`, `debug_*.ts`, `check_odds.ts` all broken | `scripts/` directory was not included in the import audit scope | Audit ALL `.ts` files in the repo, not just `src/` and `dashboard/` |
| `.gitignore` stale path | `prisma/migrations/*/migration_lock.toml` pointed at old root | `.gitignore` was not audited for hardcoded paths | Include `.gitignore`, `docker-compose.yml`, CI configs, and any file referencing `src/` or `prisma/` in the audit |
