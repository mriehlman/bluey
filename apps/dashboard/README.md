# Bluey Dashboard

Lightweight Next.js dashboard for browsing patterns, events, and managing the watchlist.  
Connects to the **same Postgres database** as the engine — read-only except for watchlist edits.

## Quick Start

```bash
cd ../..                     # repo root
bun install
cp apps/dashboard/.env.example apps/dashboard/.env
bun run dev                  # starts dashboard on http://localhost:3000
```

For auth-enabled local dev, also set:

- `NEXTAUTH_URL`
- `NEXTAUTH_SECRET`
- `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET`
- `APPLE_ID` / `APPLE_SECRET` (or Apple team/key/private-key variables)

Optional local bypass (non-production only):

- `DEV_AUTH_BYPASS=true`

When enabled, the landing page shows **Continue in Dev Mode**, which creates/uses a local dev user record (`dev.user@bluey.local`) in the DB.

## Pages

| Route | Description |
|---|---|
| `/` | Landing + login home screen |
| `/predictions` | Main predictions dashboard (requires sign-in) |
| `/patterns` | Ranked pattern feed with filters (legs, search, top N) |
| `/patterns/[id]` | Pattern detail: scores, hits, per-season data, watchlist management |
| `/events/YYYY-MM-DD` | Night summary + stored events for a date |
| `/settings` | User profile settings (requires sign-in) |

## API Routes

| Route | Method | Description |
|---|---|---|
| `/api/watchlist/add` | POST | Add pattern to watchlist |
| `/api/watchlist/update` | POST | Update watchlist entry (toggle enabled, edit notes) |
| `/api/watchlist/remove` | POST | Remove pattern from watchlist |
| `/api/events/explain` | POST | Returns stored events for a date (live explain TODO) |
| `/api/user-config` | GET/POST | Load and persist signed-in user preferences |
| `/api/predictions?governance=1&date=YYYY-MM-DD` | GET | List canonical predictions (read-only lineage view) |
| `/api/predictions/[predictionId]` | GET | Fetch canonical prediction detail by `predictionId` |
| `/api/predictions/rejections?date=YYYY-MM-DD&gameId=...` | GET | List persisted rejection diagnostics |
| `/api/predictions/runs?date=YYYY-MM-DD` | GET | List prediction runs for grouping/compare workflows |

## Notes

- Uses the shared workspace database package (`@bluey/db`) and schema at `packages/db/prisma/schema.prisma`.
- Run Prisma commands from repo root (`bun run prisma:generate`, `bun run prisma:migrate`).
- Prediction pipeline authority lives in CLI workflows only (`ingest`, `feature build`, `discovery`, `validation`, `prediction jobs`).
- Dashboard must stay non-authoritative for pipeline state: it may read/query and manage UI-facing metadata, but must not run or mutate core pipeline stages.
- The "Live Explain" feature currently returns stored events only. To enable live computation, add an HTTP endpoint to the engine and update `/api/events/explain` to call it.
