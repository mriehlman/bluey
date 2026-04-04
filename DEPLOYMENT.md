# Deployment & production database

This document describes how local development, Neon (production Postgres), Vercel, and GitHub Actions fit together.

## Mental model

| Environment | Database | Purpose |
|-------------|----------|---------|
| **Local** | Docker Postgres on port `5432` (`DATABASE_URL`) | Heavy work: backfills, discovery, pattern validation, full history |
| **Production** | Neon (`PROD_DATABASE_URL` for promote; dashboard uses `DATABASE_URL` pointing at Neon) | Slim dataset: current-season games, deployed patterns, dashboard + cron sync |

The CLI and ingest commands use whatever `DATABASE_URL` is set to. For day-to-day research, keep that as local Docker. The **promote** script copies a curated snapshot from local → production.

## Environment variables

### Root `.env` (CLI / promote / prune)

| Variable | Typical value |
|----------|----------------|
| `DATABASE_URL` | `postgresql://bluey:bluey@localhost:5432/bluey` (local Docker) |
| `PROD_DATABASE_URL` | Neon connection string (same DB the dashboard uses in production) |
| `DATA_DIR` | `./data` |

### `apps/dashboard` (Next.js)

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | **Production:** Neon URL. **Local dev:** can point at Neon or local prod Docker (`5433`) |
| `NEXTAUTH_URL` | Public URL of the app (e.g. `https://your-app.vercel.app`) |
| `NEXTAUTH_SECRET` | Long random string |
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | **Recommended for production** — enables “Continue with Google” (create OAuth credentials in Google Cloud Console; add the Vercel URL to authorized redirect URIs) |
| `DEV_AUTH_BYPASS` | `true` enables “Continue in Dev Mode” (no OAuth). On Vercel, `NODE_ENV` is production, so also set `DEV_AUTH_ALLOW_PRODUCTION=true` if you really want Dev Mode there — **do not** on a public site |
| `ODDS_API_KEY` | Required for Odds API calls from API routes that sync odds |

GitHub Actions **daily sync** uses secrets `NEON_DATABASE_URL` and `ODDS_API_KEY` (see below)—those should match production.

## Docker: local + optional second DB

`docker-compose.yml` defines:

- **`db`** — port `5432`, database `bluey` (main local dev)
- **`db-prod`** — port `5433`, database `bluey_prod` (optional local stand-in for “slim prod” before Neon)

Commands:

```bash
bun run db:up      # start all services in compose
bun run db:down    # stop
```

## Schema on Neon

After changing Prisma schema, apply to Neon:

```bash
bun run prisma:push:prod
```

(`scripts/prismaPushProd.ts` uses `PROD_DATABASE_URL`.)

## Promote: local → production snapshot

Copies teams/players, deployed `PatternV2`, `ModelVersion`, `FeatureBin`, calibration tables, and **current-season** game-related rows (stats, odds, contexts, events, feature tokens). Does **not** copy heavy research-only tables (`PatternV2Hit`, backtest runs, full historical props, etc.).

```bash
bun run promote
```

Flags:

- `--season 2025` — season filter (default: inferred from today)
- `--include-prev-season` — include previous season as well
- `--skip-game-data` — only model artifacts + reference data

Re-run promote whenever you deploy new patterns or refresh prod data after local work.

## Prune production (Neon)

Reduces size over time: strips heavy JSON on old predictions, deletes stale props/rejections, truncates tables that should not live in prod.

```bash
bun run prune:prod
bun run prune:prod -- --dry-run
```

Uses `PROD_DATABASE_URL`.

## GitHub Actions

Workflows live under `.github/workflows/`.

### 1. Daily sync (`daily-sync.yml`)

- **Schedule:** twice daily (cron is UTC; times align with ~7:30 AM and ~5:00 PM Eastern during EDT; they shift by one hour when clocks change).
- **Steps:** `bun run cli sync:daily` then `bun run cli predict:today`.
- **Manual run:** Actions → “Daily Sync & Predict” → Run workflow → optional date override.

**Repository secrets** (Settings → Secrets and variables → Actions):

| Secret | Description |
|--------|-------------|
| `NEON_DATABASE_URL` | Neon connection string (passed to the job as `DATABASE_URL`) |
| `ODDS_API_KEY` | The Odds API key |

### 2. Deploy to Vercel (`deploy.yml`)

- **Trigger:** push to `master`.
- **Secrets:** `VERCEL_TOKEN`, `VERCEL_ORG_ID`, `VERCEL_PROJECT_ID`.

Create a token at [vercel.com/account/tokens](https://vercel.com/account/tokens). `VERCEL_ORG_ID` and `VERCEL_PROJECT_ID` come from `.vercel/project.json` after linking the project (see below).

If your default branch is not `master`, change the branch name in `deploy.yml` or merge to `master` for deploys.

## Vercel

### Linking the monorepo

1. Install CLI: `npm i -g vercel` then `vercel login`.
2. From the **repository root** (not `apps/dashboard` alone), run `vercel link` and connect to the existing project **or** create `bluey-dashboard`.
3. In the Vercel project **Settings → General → Build & Development Settings**, set **Root Directory** to `apps/dashboard`.  
   - Run `vercel --prod --archive=tgz` from the **repo root** so paths are not doubled.
4. **`apps/dashboard/vercel.json`** overrides install/build so Vercel runs `bun install` from the **monorepo root** (required for `workspace:*` packages `@bluey/core` and `@bluey/db`) and runs `prisma generate` before `next build`. Without this, the default `bun install` only in `apps/dashboard` cannot resolve workspaces and the build fails.
5. Add production env vars in the Vercel dashboard (`DATABASE_URL`, `NEXTAUTH_*`, `ODDS_API_KEY`, etc.) or via `vercel env add`.

### First production deploy (CLI)

From repo root (after link + root directory). Monorepos can exceed Vercel’s per-upload file count; use an archive:

```bash
vercel --prod --archive=tgz
```

If you still hit limits, ensure `.vercelignore` at the repo root excludes `node_modules`, `.git`, and `data/` (already committed in this repo).

### GitHub deploy workflow

The workflow runs `vercel pull`, `vercel build --prod`, and `vercel deploy --prebuilt --prod` from the checkout root. Your Vercel project must be configured with root directory `apps/dashboard` so the build matches what you use locally.

## Checklist: new machine or teammate

1. Clone repo, `bun install`, `bun run db:up`, copy `.env` with local `DATABASE_URL`.
2. `bun run prisma:migrate` or `prisma:push` against local DB.
3. For production: add `PROD_DATABASE_URL`, run `bun run prisma:push:prod`, then `bun run promote` (or rely on Neon already populated).
4. Dashboard: `apps/dashboard/.env.local` with `DATABASE_URL` (Neon or local), auth and API keys.
5. CI: add GitHub secrets for daily sync and Vercel deploy.

## Troubleshooting

- **`files` should NOT have more than 15000 items:** Use `vercel --prod --archive=tgz`. The repo includes `.vercelignore` to skip `node_modules`, `.git`, `data/`, and build output so uploads stay small.
- **Build fails on `bun run build` / `build:vercel`:** The Prisma schema includes `binaryTargets` for Linux (Vercel’s build machines). `apps/dashboard/vercel.json` runs `bun install` from the repo root (workspaces) and `bun run build:vercel` (Prisma generate + Next build). The `preinstall` script skips the Bun-only check when `VERCEL` is set so installs are not blocked if the user-agent is unusual.
- **`Module '@prisma/client' has no exported member 'Game'` (or similar) on Vercel:** Prisma is generated into the repo-root `node_modules` via a custom `output` path. Import model types from **`@bluey/db`** (re-exports generated types) instead of **`@prisma/client`** in shared packages and the dashboard.
- **Dev auth env vars set on Vercel but “no provider” on the site:** NextAuth was building the provider list at **module load** (during `next build`), when `DEV_AUTH_BYPASS` was often undefined. Auth is now built via **`getAuthOptions()`** at **request time** so Production env vars apply. Redeploy after pulling this change.
- **`vercel --prod` and path `.../apps/dashboard/apps/dashboard`:** Root Directory in Vercel is set to `apps/dashboard` while the CLI was invoked from `apps/dashboard`. Run `vercel --prod` from the **repository root** instead.
- **Cron vs Eastern time:** GitHub Actions uses UTC only; adjust cron twice a year or accept a one-hour shift around DST if you keep two UTC crons.
