/**
 * Promotes model artifacts + current-season data from the local dev DB
 * to the slim production DB (local Docker or Neon).
 *
 * Usage:
 *   bun run promote
 *   bun run promote -- --season 2025 --skip-game-data
 *   bun run promote -- --include-prev-season
 */
import { PrismaClient } from "../packages/db/src/index.ts";

function getSeasonForDate(date: Date): number {
  return date.getMonth() >= 9 ? date.getFullYear() : date.getFullYear() - 1;
}

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg?.startsWith("--")) {
      const key = arg.slice(2);
      const next = args[i + 1];
      if (next && !next.startsWith("--")) {
        flags[key] = next;
        i++;
      } else {
        flags[key] = "true";
      }
    }
  }
  return flags;
}

interface PromoteStats {
  table: string;
  rows: number;
  action: string;
}

async function promote() {
  const sourceUrl = process.env.DATABASE_URL;
  const targetUrl = process.env.PROD_DATABASE_URL;

  if (!sourceUrl) throw new Error("DATABASE_URL is not set (local dev DB)");
  if (!targetUrl) throw new Error("PROD_DATABASE_URL is not set (production DB)");
  if (sourceUrl === targetUrl) throw new Error("DATABASE_URL and PROD_DATABASE_URL must be different");

  const flags = parseFlags(process.argv.slice(2));
  const season = Number(flags.season ?? getSeasonForDate(new Date()));
  const skipGameData = flags["skip-game-data"] === "true";
  const includePrevSeason = flags["include-prev-season"] === "true";
  const minSeason = includePrevSeason ? season - 1 : season;

  console.log(`\n=== Promote to Production ===`);
  console.log(`Source: ${sourceUrl.replace(/:[^:@]+@/, ":***@")}`);
  console.log(`Target: ${targetUrl.replace(/:[^:@]+@/, ":***@")}`);
  console.log(`Season: ${season}${includePrevSeason ? ` (including ${season - 1})` : ""}`);
  console.log(`Skip game data: ${skipGameData}\n`);

  const source = new PrismaClient({ datasources: { db: { url: sourceUrl } } });
  const target = new PrismaClient({ datasources: { db: { url: targetUrl } } });

  const stats: PromoteStats[] = [];

  try {
    // ── 1. Reference data (small, full copy) ──────────────────────────

    console.log("── Reference data ──");

    const teams = await source.team.findMany();
    if (teams.length > 0) {
      for (const t of teams) {
        await target.team.upsert({
          where: { id: t.id },
          create: t,
          update: { name: t.name, code: t.code, city: t.city },
        });
      }
      stats.push({ table: "Team", rows: teams.length, action: "upsert" });
      console.log(`  Team: ${teams.length} rows`);
    }

    const players = await source.player.findMany();
    if (players.length > 0) {
      for (const p of players) {
        await target.player.upsert({
          where: { id: p.id },
          create: p,
          update: { firstname: p.firstname, lastname: p.lastname, jerseyNum: p.jerseyNum, slug: p.slug },
        });
      }
      stats.push({ table: "Player", rows: players.length, action: "upsert" });
      console.log(`  Player: ${players.length} rows`);
    }

    // ── 2. Model artifacts (deployed/active only) ─────────────────────

    console.log("\n── Model artifacts ──");

    const deployedPatterns = await source.patternV2.findMany({
      where: { status: "deployed" },
    });
    if (deployedPatterns.length > 0) {
      await target.patternV2.deleteMany({ where: { status: "deployed" } });
      for (const p of deployedPatterns) {
        await target.patternV2.upsert({
          where: { id: p.id },
          create: p,
          update: {
            status: p.status,
            trainStats: p.trainStats as any,
            valStats: p.valStats as any,
            forwardStats: p.forwardStats as any,
            score: p.score,
            edge: p.edge,
            updatedAt: p.updatedAt,
          },
        });
      }
      stats.push({ table: "PatternV2", rows: deployedPatterns.length, action: "replace deployed" });
      console.log(`  PatternV2 (deployed): ${deployedPatterns.length} rows`);
    }

    const modelVersions = await source.modelVersion.findMany();
    if (modelVersions.length > 0) {
      for (const mv of modelVersions) {
        await target.modelVersion.upsert({
          where: { id: mv.id },
          create: mv,
          update: {
            name: mv.name,
            description: mv.description,
            isActive: mv.isActive,
            deployedPatterns: mv.deployedPatterns as any,
            featureBins: mv.featureBins as any,
            metaModel: mv.metaModel as any,
            tuningConfig: mv.tuningConfig as any,
            stats: mv.stats as any,
          },
        });
      }
      stats.push({ table: "ModelVersion", rows: modelVersions.length, action: "upsert" });
      console.log(`  ModelVersion: ${modelVersions.length} rows`);
    }

    const bins = await source.featureBin.findMany({ orderBy: { createdAt: "desc" } });
    if (bins.length > 0) {
      await target.featureBin.deleteMany({});
      for (const b of bins) {
        await target.featureBin.create({ data: b });
      }
      stats.push({ table: "FeatureBin", rows: bins.length, action: "replace all" });
      console.log(`  FeatureBin: ${bins.length} rows`);
    }

    const calibrations = await source.predictionCalibration.findMany().catch(() => []);
    if (calibrations.length > 0) {
      await target.predictionCalibration.deleteMany({});
      for (const c of calibrations) {
        await target.predictionCalibration.create({ data: c });
      }
      stats.push({ table: "PredictionCalibration", rows: calibrations.length, action: "replace all" });
      console.log(`  PredictionCalibration: ${calibrations.length} rows`);
    }

    const reliability = await source.pickSourceReliability.findMany().catch(() => []);
    if (reliability.length > 0) {
      await target.pickSourceReliability.deleteMany({});
      for (const r of reliability) {
        await target.pickSourceReliability.create({ data: r });
      }
      stats.push({ table: "PickSourceReliability", rows: reliability.length, action: "replace all" });
      console.log(`  PickSourceReliability: ${reliability.length} rows`);
    }

    if (skipGameData) {
      console.log("\n── Skipping game data (--skip-game-data) ──");
      printSummary(stats);
      await source.$disconnect();
      await target.$disconnect();
      return;
    }

    // ── 3. Current-season game data ───────────────────────────────────

    console.log(`\n── Game data (season >= ${minSeason}) ──`);

    const games = await source.game.findMany({
      where: { season: { gte: minSeason } },
      include: { externalIds: true },
    });
    console.log(`  Games to sync: ${games.length}`);

    let gameCount = 0;
    for (const g of games) {
      const { externalIds, ...gameData } = g;
      await target.game.upsert({
        where: { id: g.id },
        create: gameData,
        update: {
          homeScore: g.homeScore,
          awayScore: g.awayScore,
          status: g.status,
          periods: g.periods,
          homeWinsPreGame: g.homeWinsPreGame,
          homeLossesPreGame: g.homeLossesPreGame,
          awayWinsPreGame: g.awayWinsPreGame,
          awayLossesPreGame: g.awayLossesPreGame,
        },
      });

      for (const ext of externalIds) {
        await target.gameExternalId.upsert({
          where: { gameId_source: { gameId: ext.gameId, source: ext.source } },
          create: ext,
          update: { sourceId: ext.sourceId },
        });
      }

      gameCount++;
      if (gameCount % 100 === 0) console.log(`    ...${gameCount}/${games.length} games`);
    }
    stats.push({ table: "Game + GameExternalId", rows: gameCount, action: "upsert" });
    console.log(`  Game: ${gameCount} rows`);

    const gameIds = games.map((g) => g.id);

    // Player stats (chunked to avoid query size limits)
    const CHUNK = 200;
    let playerStatCount = 0;
    for (let i = 0; i < gameIds.length; i += CHUNK) {
      const chunk = gameIds.slice(i, i + CHUNK);
      const playerStats = await source.playerGameStat.findMany({
        where: { gameId: { in: chunk } },
      });
      if (playerStats.length === 0) continue;

      await target.playerGameStat.deleteMany({ where: { gameId: { in: chunk } } });
      await target.playerGameStat.createMany({ data: playerStats });
      playerStatCount += playerStats.length;
    }
    stats.push({ table: "PlayerGameStat", rows: playerStatCount, action: "replace by game" });
    console.log(`  PlayerGameStat: ${playerStatCount} rows`);

    // Game odds
    let oddsCount = 0;
    for (let i = 0; i < gameIds.length; i += CHUNK) {
      const chunk = gameIds.slice(i, i + CHUNK);
      const odds = await source.gameOdds.findMany({ where: { gameId: { in: chunk } } });
      if (odds.length === 0) continue;
      await target.gameOdds.deleteMany({ where: { gameId: { in: chunk } } });
      await target.gameOdds.createMany({ data: odds });
      oddsCount += odds.length;
    }
    stats.push({ table: "GameOdds", rows: oddsCount, action: "replace by game" });
    console.log(`  GameOdds: ${oddsCount} rows`);

    // Player prop odds (only recent — last 30 days of the season)
    const propCutoff = new Date();
    propCutoff.setDate(propCutoff.getDate() - 30);
    let propCount = 0;
    for (let i = 0; i < gameIds.length; i += CHUNK) {
      const chunk = gameIds.slice(i, i + CHUNK);
      const props = await source.playerPropOdds.findMany({
        where: { gameId: { in: chunk }, fetchedAt: { gte: propCutoff } },
      });
      if (props.length === 0) continue;
      await target.playerPropOdds.deleteMany({ where: { gameId: { in: chunk } } });
      if (props.length > 0) await target.playerPropOdds.createMany({ data: props });
      propCount += props.length;
    }
    stats.push({ table: "PlayerPropOdds", rows: propCount, action: "replace (last 30d)" });
    console.log(`  PlayerPropOdds (last 30d): ${propCount} rows`);

    // Game context
    let ctxCount = 0;
    for (let i = 0; i < gameIds.length; i += CHUNK) {
      const chunk = gameIds.slice(i, i + CHUNK);
      const contexts = await source.gameContext.findMany({ where: { gameId: { in: chunk } } });
      if (contexts.length === 0) continue;
      await target.gameContext.deleteMany({ where: { gameId: { in: chunk } } });
      await target.gameContext.createMany({ data: contexts });
      ctxCount += contexts.length;
    }
    stats.push({ table: "GameContext", rows: ctxCount, action: "replace by game" });
    console.log(`  GameContext: ${ctxCount} rows`);

    // Player game context
    let pCtxCount = 0;
    for (let i = 0; i < gameIds.length; i += CHUNK) {
      const chunk = gameIds.slice(i, i + CHUNK);
      const pContexts = await source.playerGameContext.findMany({ where: { gameId: { in: chunk } } });
      if (pContexts.length === 0) continue;
      await target.playerGameContext.deleteMany({ where: { gameId: { in: chunk } } });
      await target.playerGameContext.createMany({ data: pContexts });
      pCtxCount += pContexts.length;
    }
    stats.push({ table: "PlayerGameContext", rows: pCtxCount, action: "replace by game" });
    console.log(`  PlayerGameContext: ${pCtxCount} rows`);

    // Game events (current season)
    let eventCount = 0;
    for (let i = 0; i < gameIds.length; i += CHUNK) {
      const chunk = gameIds.slice(i, i + CHUNK);
      const events = await source.gameEvent.findMany({ where: { gameId: { in: chunk } } });
      if (events.length === 0) continue;
      await target.gameEvent.deleteMany({ where: { gameId: { in: chunk } } });
      await target.gameEvent.createMany({ data: events });
      eventCount += events.length;
    }
    stats.push({ table: "GameEvent", rows: eventCount, action: "replace by game" });
    console.log(`  GameEvent: ${eventCount} rows`);

    // Game feature tokens (current season)
    let tokenCount = 0;
    for (let i = 0; i < gameIds.length; i += CHUNK) {
      const chunk = gameIds.slice(i, i + CHUNK);
      const tokens = await source.gameFeatureToken.findMany({ where: { gameId: { in: chunk } } });
      if (tokens.length === 0) continue;
      await target.gameFeatureToken.deleteMany({ where: { gameId: { in: chunk } } });
      await target.gameFeatureToken.createMany({ data: tokens });
      tokenCount += tokens.length;
    }
    stats.push({ table: "GameFeatureToken", rows: tokenCount, action: "replace by game" });
    console.log(`  GameFeatureToken: ${tokenCount} rows`);

    printSummary(stats);
  } finally {
    await source.$disconnect();
    await target.$disconnect();
  }
}

function printSummary(stats: PromoteStats[]) {
  console.log("\n── Summary ──");
  console.log("┌────────────────────────────┬────────┬──────────────────────┐");
  console.log("│ Table                      │  Rows  │ Action               │");
  console.log("├────────────────────────────┼────────┼──────────────────────┤");
  for (const s of stats) {
    console.log(
      `│ ${s.table.padEnd(26)} │ ${String(s.rows).padStart(6)} │ ${s.action.padEnd(20)} │`,
    );
  }
  console.log("└────────────────────────────┴────────┴──────────────────────┘");
  const totalRows = stats.reduce((sum, s) => sum + s.rows, 0);
  console.log(`\nTotal rows promoted: ${totalRows.toLocaleString()}`);
  console.log("\nDone! Production DB is ready.");
  console.log("Tables NOT promoted (stay local-only):");
  console.log("  - PatternV2Hit, GamePattern, GamePatternHit");
  console.log("  - BacktestRun, BacktestResult");
  console.log("  - OrphanPlayerStat, PredictionLog, IngestDay, ExternalIdMap\n");
}

promote().catch((err) => {
  console.error("Promote failed:", err);
  process.exit(1);
});
