# Bluey Dashboard

Lightweight Next.js dashboard for browsing patterns, events, and managing the watchlist.  
Connects to the **same Postgres database** as the engine — read-only except for watchlist edits.

## Quick Start

```bash
cd dashboard
cp .env.example .env        # set DATABASE_URL if different from default
npm install                  # or: bun install
npx prisma generate          # generate Prisma client from schema
npm run dev                  # starts on http://localhost:3000
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

- Uses the same `schema.prisma` as the engine (copied, not shared).
- If the engine schema changes, update `dashboard/prisma/schema.prisma` to match and re-run `npx prisma generate`.
- The "Live Explain" feature currently returns stored events only. To enable live computation, add an HTTP endpoint to the engine and update `/api/events/explain` to call it.
