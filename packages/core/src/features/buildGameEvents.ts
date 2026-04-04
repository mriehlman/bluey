import { prisma } from "@bluey/db";
import { GAME_EVENT_CATALOG } from "./gameEventCatalog";
import type { GameEventContext } from "./gameEventCatalog";

function getCurrentSeason(): number {
  const now = new Date();
  // NBA season starts in October, so Oct-Dec uses current year, Jan-Sep uses previous year
  return now.getMonth() >= 9 ? now.getFullYear() : now.getFullYear() - 1;
}

export async function buildGameEvents(args: string[] = []): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const defaultSeason = getCurrentSeason();
  const fromSeason = Number(flags["from-season"] || flags.season) || defaultSeason;
  const toSeason = Number(flags["to-season"] || flags.season || fromSeason);

  console.log(`Building game events for season(s) ${fromSeason}${toSeason !== fromSeason ? ` to ${toSeason}` : ""}`);

  for (let season = fromSeason; season <= toSeason; season++) {
    console.log(`\n=== Building GameEvents for season ${season} ===\n`);

    const deleted = await prisma.gameEvent.deleteMany({ where: { season } });
    if (deleted.count > 0) {
      console.log(`  Cleared ${deleted.count} existing GameEvent rows`);
    }

    const totalGames = await prisma.game.count({ where: { season } });
    const batchSize = 100;
    console.log(`  Found ${totalGames} games for season ${season}`);

    let eventCount = 0;
    let gamesWithContext = 0;

    for (let offset = 0; offset < totalGames; offset += batchSize) {
      const games = await prisma.game.findMany({
        where: { season },
        include: {
          homeTeam: true,
          awayTeam: true,
          context: true,
          playerContexts: true,
          playerStats: true,
          odds: true,
        },
        orderBy: [{ date: "asc" }, { id: "asc" }],
        skip: offset,
        take: batchSize,
      });
      for (const game of games) {

        if (!game.context) continue;
        gamesWithContext++;

      const consensusOdds = game.odds.find((o) => o.source === "consensus") ?? game.odds[0] ?? null;

      const ctx: GameEventContext = {
        game,
        context: game.context,
        playerContexts: game.playerContexts,
        stats: game.playerStats,
        odds: consensusOdds,
      };

      const eventBatch: {
        gameId: string;
        season: number;
        eventKey: string;
        type: string;
        side: string;
        meta: unknown;
      }[] = [];

      for (const def of GAME_EVENT_CATALOG) {
        for (const side of def.sides) {
          const result = def.compute(ctx, side);
          if (result.hit) {
            eventBatch.push({
              gameId: game.id,
              season,
              eventKey: def.key,
              type: def.type,
              side,
              meta: result.meta ?? null,
            });
          }
        }
      }

        if (eventBatch.length > 0) {
          const result = await prisma.gameEvent.createMany({
            data: eventBatch.map((e) => ({
              gameId: e.gameId,
              season: e.season,
              eventKey: e.eventKey,
              type: e.type,
              side: e.side,
              meta: e.meta as any,
            })),
            skipDuplicates: true,
          });
          eventCount += result.count;
        }
      }
      console.log(`  Processed ${Math.min(offset + games.length, totalGames)}/${totalGames} games (${eventCount} events)`);
    }

    console.log(`\n  Season ${season} complete: ${gamesWithContext} games with context, ${eventCount} GameEvent rows created`);
  }
}
