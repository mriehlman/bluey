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

## Pages

| Route | Description |
|---|---|
| `/patterns` | Ranked pattern feed with filters (legs, search, top N) |
| `/patterns/[id]` | Pattern detail: scores, hits, per-season data, watchlist management |
| `/events/YYYY-MM-DD` | Night summary + stored events for a date |

## API Routes

| Route | Method | Description |
|---|---|---|
| `/api/watchlist/add` | POST | Add pattern to watchlist |
| `/api/watchlist/update` | POST | Update watchlist entry (toggle enabled, edit notes) |
| `/api/watchlist/remove` | POST | Remove pattern from watchlist |
| `/api/events/explain` | POST | Returns stored events for a date (live explain TODO) |

## Notes

- Uses the shared workspace database package (`@bluey/db`) and schema at `packages/db/prisma/schema.prisma`.
- Run Prisma commands from repo root (`bun run prisma:generate`, `bun run prisma:migrate`).
- The "Live Explain" feature currently returns stored events only. To enable live computation, add an HTTP endpoint to the engine and update `/api/events/explain` to call it.
