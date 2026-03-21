import { prisma } from "@bluey/db";
import { GAME_EVENT_CATALOG } from "./gameEventCatalog";
import type { GameEventContext } from "./gameEventCatalog";
import { outcomeFamily } from "../patterns/metaModelCore";

export async function resolvePredictionLogs(args: string[] = []): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const unresolved = await prisma.predictionLog.findMany({
    where: { actualResult: null },
    orderBy: { gameDate: "asc" },
  });

  if (unresolved.length === 0) {
    console.log("No unresolved prediction logs.");
    return;
  }

  const gameIds = [...new Set(unresolved.map((r) => r.gameId))];
  console.log(`Resolving ${unresolved.length} predictions across ${gameIds.length} games...`);

  const games = await prisma.game.findMany({
    where: { id: { in: gameIds } },
    include: {
      homeTeam: true,
      awayTeam: true,
      playerStats: true,
      odds: true,
      context: true,
    },
  });

  const gameMap = new Map(games.map((g) => [g.id, g]));

  const playerContextsForGames = await prisma.playerGameContext.findMany({
    where: { gameId: { in: gameIds } },
  });
  const playerCtxByGame = new Map<string, typeof playerContextsForGames>();
  for (const pc of playerContextsForGames) {
    const list = playerCtxByGame.get(pc.gameId) ?? [];
    list.push(pc);
    playerCtxByGame.set(pc.gameId, list);
  }

  let resolved = 0;
  let skipped = 0;

  for (const log of unresolved) {
    const game = gameMap.get(log.gameId);
    if (!game || !game.context) {
      skipped++;
      continue;
    }

    if (game.playerStats.length === 0) {
      skipped++;
      continue;
    }

    const odds = game.odds.find((o) => o.source === "consensus") ?? game.odds[0] ?? null;
    const pCtxs = playerCtxByGame.get(game.id) ?? [];

    const ctx: GameEventContext = {
      game: { ...game, homeTeam: game.homeTeam, awayTeam: game.awayTeam },
      context: game.context,
      playerContexts: pCtxs,
      stats: game.playerStats,
      odds,
    };

    const baseOutcome = log.outcomeType.replace(/:.*$/, "");
    const catalogEntry = GAME_EVENT_CATALOG.find((e) => e.type === "outcome" && e.key === baseOutcome);

    if (!catalogEntry) {
      skipped++;
      continue;
    }

    let hit = false;
    for (const side of catalogEntry.sides) {
      const result = catalogEntry.compute(ctx, side);
      const fullKey = `${baseOutcome}:${side}`;
      if (fullKey === log.outcomeType && result.hit) {
        hit = true;
        break;
      }
    }

    await prisma.predictionLog.update({
      where: { id: log.id },
      data: {
        actualResult: hit,
        resolvedAt: new Date(),
      },
    });
    resolved++;
  }

  console.log(`Resolved: ${resolved}, Skipped (no game data): ${skipped}`);
}

export async function reportPredictionAccuracy(args: string[] = []): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const where: Record<string, any> = { actualResult: { not: null } };
  if (flags.since) {
    where.gameDate = { gte: new Date(flags.since + "T00:00:00Z") };
  }
  if (flags.version) {
    where.modelVersionName = flags.version;
  }

  const logs = await prisma.predictionLog.findMany({
    where,
    orderBy: { gameDate: "asc" },
  });

  if (logs.length === 0) {
    console.log("No resolved prediction logs found.");
    return;
  }

  const total = logs.length;
  const correct = logs.filter((l) => l.actualResult === true).length;
  const accuracy = correct / total;

  console.log(`\n=== Prediction Accuracy Report ===\n`);
  console.log(`Total predictions: ${total}`);
  console.log(`Correct: ${correct}`);
  console.log(`Accuracy: ${(accuracy * 100).toFixed(1)}%\n`);

  // By probability bucket
  const buckets = [
    { label: "50-55%", min: 0.50, max: 0.55 },
    { label: "55-60%", min: 0.55, max: 0.60 },
    { label: "60-65%", min: 0.60, max: 0.65 },
    { label: "65-70%", min: 0.65, max: 0.70 },
    { label: "70%+", min: 0.70, max: 1.01 },
  ];

  console.log("By model probability bucket:");
  for (const bucket of buckets) {
    const inBucket = logs.filter((l) => l.modelProb >= bucket.min && l.modelProb < bucket.max);
    if (inBucket.length === 0) continue;
    const bucketCorrect = inBucket.filter((l) => l.actualResult === true).length;
    const bucketAcc = bucketCorrect / inBucket.length;
    console.log(`  ${bucket.label}: ${(bucketAcc * 100).toFixed(1)}% (${bucketCorrect}/${inBucket.length})`);
  }

  // By outcome family
  console.log("\nBy outcome family:");
  const familyMap = new Map<string, { total: number; correct: number }>();
  for (const log of logs) {
    const family = outcomeFamily(log.outcomeType);
    const entry = familyMap.get(family) ?? { total: 0, correct: 0 };
    entry.total++;
    if (log.actualResult === true) entry.correct++;
    familyMap.set(family, entry);
  }
  for (const [family, stats] of [...familyMap.entries()].sort((a, b) => b[1].total - a[1].total)) {
    const acc = stats.correct / stats.total;
    console.log(`  ${family}: ${(acc * 100).toFixed(1)}% (${stats.correct}/${stats.total})`);
  }

  // By agreement count
  console.log("\nBy agreement count:");
  const agreeMap = new Map<number, { total: number; correct: number }>();
  for (const log of logs) {
    const agree = log.agreementCount;
    const entry = agreeMap.get(agree) ?? { total: 0, correct: 0 };
    entry.total++;
    if (log.actualResult === true) entry.correct++;
    agreeMap.set(agree, entry);
  }
  for (const [agree, stats] of [...agreeMap.entries()].sort((a, b) => a[0] - b[0])) {
    const acc = stats.correct / stats.total;
    console.log(`  ${agree} patterns: ${(acc * 100).toFixed(1)}% (${stats.correct}/${stats.total})`);
  }

  // Market vs model-only
  console.log("\nMarket-backed vs model-only:");
  const withMarket = logs.filter((l) => l.hadMarketPick);
  const withoutMarket = logs.filter((l) => !l.hadMarketPick);
  if (withMarket.length > 0) {
    const acc = withMarket.filter((l) => l.actualResult).length / withMarket.length;
    console.log(`  With market: ${(acc * 100).toFixed(1)}% (${withMarket.filter((l) => l.actualResult).length}/${withMarket.length})`);
  }
  if (withoutMarket.length > 0) {
    const acc = withoutMarket.filter((l) => l.actualResult).length / withoutMarket.length;
    console.log(`  Model-only: ${(acc * 100).toFixed(1)}% (${withoutMarket.filter((l) => l.actualResult).length}/${withoutMarket.length})`);
  }

  // Calibration: average predicted prob vs actual hit rate
  console.log("\nCalibration (predicted vs actual):");
  for (const bucket of buckets) {
    const inBucket = logs.filter((l) => l.modelProb >= bucket.min && l.modelProb < bucket.max);
    if (inBucket.length === 0) continue;
    const avgPredicted = inBucket.reduce((s, l) => s + l.modelProb, 0) / inBucket.length;
    const actualRate = inBucket.filter((l) => l.actualResult === true).length / inBucket.length;
    const diff = actualRate - avgPredicted;
    console.log(`  ${bucket.label}: predicted ${(avgPredicted * 100).toFixed(1)}% vs actual ${(actualRate * 100).toFixed(1)}% (${diff >= 0 ? "+" : ""}${(diff * 100).toFixed(1)}%)`);
  }

  console.log("");
}
