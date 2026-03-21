import { prisma } from "@bluey/db";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { computeContextForDate } from "./buildGameContext";
import {
  loadEarlyInjuriesForDate,
  buildTeamAliasLookup,
  computeLineupSignalsFromCounts,
} from "./injuryContext";
import { GAME_EVENT_CATALOG } from "./gameEventCatalog";
import type { GameEventContext } from "./gameEventCatalog";
import type { GameContext, PlayerGameContext } from "@prisma/client";
import { LEDGER_TUNING } from "../config/tuning";
import {
  generateGamePredictions,
  loadLatestFeatureBins,
  loadMetaModel,
  type DeployedPatternV2,
  type GamePlayerContext,
  type PlayerPropRow,
} from "./predictionEngine";
import { assertPredictionRecord, type PredictionRecord } from "./predictionContract";
import { loadActiveModelVersion } from "../patterns/modelVersion";

function getSeasonForDate(date: Date): number {
  return date.getMonth() >= 9 ? date.getFullYear() : date.getFullYear() - 1;
}

function sqlEsc(value: string): string {
  return value.replaceAll("'", "''");
}

async function ensureCanonicalPredictionTables(): Promise<void> {
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS "CanonicalPrediction" (
      "predictionId" text PRIMARY KEY,
      "runId" text,
      "runStartedAt" timestamptz,
      "runContext" jsonb,
      "gameId" text NOT NULL,
      "market" text NOT NULL,
      "selection" text NOT NULL,
      "confidenceScore" double precision NOT NULL,
      "edgeEstimate" double precision NOT NULL,
      "predictionContractVersion" text NOT NULL,
      "rankingPolicyVersion" text NOT NULL,
      "aggregationPolicyVersion" text NOT NULL,
      "modelBundleVersion" text NOT NULL,
      "featureSchemaVersion" text NOT NULL,
      "featureSnapshotId" text NOT NULL,
      "featureSnapshotPayload" jsonb NOT NULL,
      "supportingPatterns" text[] NOT NULL,
      "modelVotes" jsonb NOT NULL,
      "generatedAt" timestamptz NOT NULL,
      "createdAt" timestamptz NOT NULL DEFAULT NOW(),
      "updatedAt" timestamptz NOT NULL DEFAULT NOW()
    );
  `);
  await prisma.$executeRawUnsafe(
    `ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "runId" text`,
  );
  await prisma.$executeRawUnsafe(
    `ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "runStartedAt" timestamptz`,
  );
  await prisma.$executeRawUnsafe(
    `ALTER TABLE "CanonicalPrediction" ADD COLUMN IF NOT EXISTS "runContext" jsonb`,
  );
  await prisma.$executeRawUnsafe(
    `CREATE INDEX IF NOT EXISTS "CanonicalPrediction_runId_generatedAt_idx"
     ON "CanonicalPrediction" ("runId","generatedAt")`,
  );
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS "CanonicalPredictionRejection" (
      "id" text PRIMARY KEY,
      "runId" text,
      "runDate" date NOT NULL,
      "gameId" text NOT NULL,
      "patternId" text NOT NULL,
      "reasons" text[] NOT NULL,
      "createdAt" timestamptz NOT NULL DEFAULT NOW()
    );
  `);
  await prisma.$executeRawUnsafe(
    `ALTER TABLE "CanonicalPredictionRejection" ADD COLUMN IF NOT EXISTS "runId" text`,
  );
  await prisma.$executeRawUnsafe(
    `CREATE INDEX IF NOT EXISTS "CanonicalPredictionRejection_runId_idx"
     ON "CanonicalPredictionRejection" ("runId")`,
  );
}

async function persistCanonicalPredictions(input: {
  records: PredictionRecord[];
  runId: string;
  runStartedAtIso: string;
  runContext: Record<string, unknown>;
}): Promise<void> {
  for (const record of input.records) {
    await prisma.$executeRawUnsafe(
      `INSERT INTO "CanonicalPrediction"
      ("predictionId","runId","runStartedAt","runContext","gameId","market","selection","confidenceScore","edgeEstimate",
       "predictionContractVersion","rankingPolicyVersion","aggregationPolicyVersion",
       "modelBundleVersion","featureSchemaVersion","featureSnapshotId","featureSnapshotPayload",
       "supportingPatterns","modelVotes","generatedAt","updatedAt")
      VALUES
      ('${sqlEsc(record.prediction_id)}','${sqlEsc(input.runId)}','${sqlEsc(input.runStartedAtIso)}'::timestamptz,'${sqlEsc(JSON.stringify(input.runContext))}'::jsonb,'${sqlEsc(record.game_id)}','${sqlEsc(record.market)}',
       '${sqlEsc(record.selection)}',${record.confidence_score},${record.edge_estimate},
       '${sqlEsc(record.prediction_contract_version)}','${sqlEsc(record.ranking_policy_version)}',
       '${sqlEsc(record.aggregation_policy_version)}','${sqlEsc(record.model_bundle_version)}',
       '${sqlEsc(record.feature_schema_version)}','${sqlEsc(record.feature_snapshot_id)}',
       '${sqlEsc(JSON.stringify(record.feature_snapshot_payload))}'::jsonb,
       '${sqlEsc(`{${record.supporting_patterns.map((p) => `"${p.replaceAll('"', '\\"')}"`).join(",")}}`)}'::text[],
       '${sqlEsc(JSON.stringify(record.model_votes))}'::jsonb,
       '${sqlEsc(record.generated_at)}'::timestamptz,NOW())
      ON CONFLICT ("predictionId") DO UPDATE SET
        "runId" = EXCLUDED."runId",
        "runStartedAt" = EXCLUDED."runStartedAt",
        "runContext" = EXCLUDED."runContext",
        "confidenceScore" = EXCLUDED."confidenceScore",
        "edgeEstimate" = EXCLUDED."edgeEstimate",
        "featureSnapshotPayload" = EXCLUDED."featureSnapshotPayload",
        "supportingPatterns" = EXCLUDED."supportingPatterns",
        "modelVotes" = EXCLUDED."modelVotes",
        "generatedAt" = EXCLUDED."generatedAt",
        "updatedAt" = NOW()`,
    );
  }
}

async function persistRejectionDiagnostics(input: {
  runId: string;
  runDate: string;
  gameId: string;
  rejectedPatternDiagnostics: Array<{ patternId: string; reasons: string[] }>;
}): Promise<void> {
  for (const row of input.rejectedPatternDiagnostics) {
    const id = crypto.randomUUID();
    const reasonsArrayLiteral = `{${row.reasons.map((r) => `"${r.replaceAll('"', '\\"')}"`).join(",")}}`;
    await prisma.$executeRawUnsafe(
      `INSERT INTO "CanonicalPredictionRejection"
       ("id","runId","runDate","gameId","patternId","reasons")
       VALUES
       ('${id}','${sqlEsc(input.runId)}','${sqlEsc(input.runDate)}'::date,'${sqlEsc(input.gameId)}','${sqlEsc(row.patternId)}',
        '${sqlEsc(reasonsArrayLiteral)}'::text[])`,
    );
  }
}

export async function predictGames(args: string[] = []): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const dateStr = flags.date;
  if (!dateStr) {
    console.error("Usage: --date YYYY-MM-DD");
    process.exit(1);
  }

  const targetDate = new Date(dateStr + "T00:00:00Z");
  const season = Number(flags.season) || getSeasonForDate(targetDate);
  const runId = crypto.randomUUID();
  const runStartedAtIso = new Date().toISOString();
  const runContext: Record<string, unknown> = {
    date: dateStr,
    season,
    flags,
  };
  const canonicalRecords: PredictionRecord[] = [];
  const rejectionArtifact: Array<{
    runId: string;
    gameId: string;
    patternId: string;
    reasons: string[];
  }> = [];
  await ensureCanonicalPredictionTables();

  console.log(`\n=== Predictions for ${dateStr} (season ${season}) ===\n`);

  // Load games on this date
  const games = await prisma.game.findMany({
    where: {
      date: targetDate,
    },
    include: {
      homeTeam: true,
      awayTeam: true,
      playerStats: true,
      odds: true,
    },
  });

  if (games.length === 0) {
    console.log("No games found for this date.");
    return;
  }

  console.log(`Found ${games.length} games\n`);

  // Compute live context
  const allTeamIds = [...new Set(games.flatMap((g) => [g.homeTeamId, g.awayTeamId]))];
  const { teamSnapshots, playerSnapshots } = await computeContextForDate(season, targetDate, allTeamIds);

  // Load all stored patterns
  const patterns = await prisma.gamePattern.findMany({
    orderBy: { confidenceScore: "desc" },
  });

  const activeVersion = await loadActiveModelVersion();
  let deployedV2: DeployedPatternV2[];
  let bins: Map<string, any>;
  let metaModel: Awaited<ReturnType<typeof loadMetaModel>>;

  if (activeVersion) {
    console.log(`Using model version snapshot (active version found)`);
    deployedV2 = activeVersion.deployedPatterns;
    bins = new Map(Object.entries(activeVersion.featureBins));
    metaModel = activeVersion.metaModel;
    runContext.modelVersionName = (activeVersion as { name?: string }).name ?? null;
  } else {
    deployedV2 = await prisma.patternV2.findMany({
      where: { status: "deployed" },
      select: {
        id: true,
        outcomeType: true,
        conditions: true,
        posteriorHitRate: true,
        edge: true,
        score: true,
        n: true,
      },
      orderBy: { score: "desc" },
    }) as DeployedPatternV2[];
    bins = await loadLatestFeatureBins(prisma);
    metaModel = await loadMetaModel();
    runContext.modelVersionName = null;
  }

  const tokenizedTodayCount = await prisma.gameFeatureToken.count({
    where: { gameId: { in: games.map((g) => g.id) } },
  });
  console.log(`Loaded ${patterns.length} legacy stored patterns`);
  console.log(`Loaded ${deployedV2.length} deployed PatternV2 rows${activeVersion ? " (from snapshot)" : ""}`);
  console.log(`Found ${tokenizedTodayCount}/${games.length} GameFeatureToken rows for target games\n`);
  if (deployedV2.length > 0 && bins.size === 0) {
    console.log(
      "  Note: no FeatureBin rows found; v2 pregame tokenization is disabled. Run build:feature-bins first.\n",
    );
  }

  // Load player data for player contexts
  const playerIds = [...new Set(games.flatMap((g) => g.playerStats.map((s) => s.playerId)))];
  const playerContextsFromDB = await prisma.playerGameContext.findMany({
    where: {
      playerId: { in: playerIds },
      game: { season, date: { lt: targetDate } },
    },
    orderBy: { game: { date: "desc" } },
    distinct: ["playerId"],
  });

  // Load player props and name map for shared engine
  const gameIds = games.map((g) => g.id);
  const playerPropsRaw = await prisma.playerPropOdds.findMany({
    where: { gameId: { in: gameIds } },
    select: {
      gameId: true,
      playerId: true,
      market: true,
      line: true,
      overPrice: true,
      underPrice: true,
      player: { select: { firstname: true, lastname: true } },
    },
  });
  const playerPropsByGame = new Map<string, PlayerPropRow[]>();
  for (const gid of gameIds) playerPropsByGame.set(gid, []);
  for (const r of playerPropsRaw) {
    playerPropsByGame.get(r.gameId)?.push(r);
  }

  const allPlayerIds = [...new Set([
    ...playerIds,
    ...playerPropsRaw.map((r) => r.playerId),
    ...[...playerSnapshots.keys()],
  ])];
  const playersForNames = await prisma.player.findMany({
    where: { id: { in: allPlayerIds } },
    select: { id: true, firstname: true, lastname: true },
  });
  const playerNameMap = new Map(playersForNames.map((p) => [p.id, p]));

  // Load injury data for live predictions
  const allTeams = await prisma.team.findMany({
    select: { id: true, city: true, name: true, code: true },
  });
  const teamAliasLookup = buildTeamAliasLookup(allTeams);
  const injuryCache = new Map();
  const injuriesByTeam = loadEarlyInjuriesForDate(dateStr, teamAliasLookup, injuryCache);
  if (injuriesByTeam.size > 0) {
    console.log(`Loaded injury data: ${[...injuriesByTeam.values()].reduce((s, t) => s + t.out + t.doubtful + t.questionable + t.probable, 0)} entries across ${injuriesByTeam.size} teams\n`);
  }

  for (const game of games) {
    const homeSnap = teamSnapshots.get(game.homeTeamId);
    const awaySnap = teamSnapshots.get(game.awayTeamId);

    if (!homeSnap || !awaySnap) {
      console.log(`${game.homeTeam.code ?? game.homeTeam.name} vs ${game.awayTeam.code ?? game.awayTeam.name}  |  Insufficient context (< 10 games)\n`);
      continue;
    }

    // Build virtual GameContext
    const virtualContext: GameContext = {
      id: "",
      gameId: game.id,
      homeWins: homeSnap.wins,
      homeLosses: homeSnap.losses,
      homePpg: homeSnap.ppg,
      homeOppg: homeSnap.oppg,
      homePace: homeSnap.pace,
      homeRebPg: homeSnap.rebPg,
      homeAstPg: homeSnap.astPg,
      homeFg3Pct: homeSnap.fg3Pct,
      homeFtPct: homeSnap.ftPct,
      homeRankOff: homeSnap.rankOff,
      homeRankDef: homeSnap.rankDef,
      homeRankPace: homeSnap.rankPace,
      homeStreak: homeSnap.streak,
      awayWins: awaySnap.wins,
      awayLosses: awaySnap.losses,
      awayPpg: awaySnap.ppg,
      awayOppg: awaySnap.oppg,
      awayPace: awaySnap.pace,
      awayRebPg: awaySnap.rebPg,
      awayAstPg: awaySnap.astPg,
      awayFg3Pct: awaySnap.fg3Pct,
      awayFtPct: awaySnap.ftPct,
      awayRankOff: awaySnap.rankOff,
      awayRankDef: awaySnap.rankDef,
      awayRankPace: awaySnap.rankPace,
      awayStreak: awaySnap.streak,
      homeRestDays: homeSnap.lastGameDate
        ? Math.floor((targetDate.getTime() - homeSnap.lastGameDate.getTime()) / 86_400_000) - 1
        : null,
      awayRestDays: awaySnap.lastGameDate
        ? Math.floor((targetDate.getTime() - awaySnap.lastGameDate.getTime()) / 86_400_000) - 1
        : null,
      homeIsB2b: false,
      awayIsB2b: false,
      homeInjuryOutCount: injuriesByTeam.get(game.homeTeamId)?.out ?? null,
      homeInjuryDoubtfulCount: injuriesByTeam.get(game.homeTeamId)?.doubtful ?? null,
      homeInjuryQuestionableCount: injuriesByTeam.get(game.homeTeamId)?.questionable ?? null,
      homeInjuryProbableCount: injuriesByTeam.get(game.homeTeamId)?.probable ?? null,
      awayInjuryOutCount: injuriesByTeam.get(game.awayTeamId)?.out ?? null,
      awayInjuryDoubtfulCount: injuriesByTeam.get(game.awayTeamId)?.doubtful ?? null,
      awayInjuryQuestionableCount: injuriesByTeam.get(game.awayTeamId)?.questionable ?? null,
      awayInjuryProbableCount: injuriesByTeam.get(game.awayTeamId)?.probable ?? null,
      homeLineupCertainty: computeLineupSignalsFromCounts(injuriesByTeam.get(game.homeTeamId)).certainty,
      awayLineupCertainty: computeLineupSignalsFromCounts(injuriesByTeam.get(game.awayTeamId)).certainty,
      homeLateScratchRisk: computeLineupSignalsFromCounts(injuriesByTeam.get(game.homeTeamId)).lateScratchRisk,
      awayLateScratchRisk: computeLineupSignalsFromCounts(injuriesByTeam.get(game.awayTeamId)).lateScratchRisk,
      h2hHomeWins: 0,
      h2hAwayWins: 0,
    };

    virtualContext.homeIsB2b = virtualContext.homeRestDays === 0;
    virtualContext.awayIsB2b = virtualContext.awayRestDays === 0;

    // Build player contexts for this game from snapshots
    const virtualPlayerContexts: PlayerGameContext[] = [];
    for (const stat of game.playerStats) {
      const pSnap = playerSnapshots.get(stat.playerId);
      if (!pSnap) continue;

      const oppTeamId = stat.teamId === game.homeTeamId ? game.awayTeamId : game.homeTeamId;
      const oppSnap = teamSnapshots.get(oppTeamId);

      virtualPlayerContexts.push({
        id: "",
        gameId: game.id,
        playerId: pSnap.playerId,
        teamId: pSnap.teamId,
        gamesPlayed: pSnap.gamesPlayed,
        ppg: pSnap.ppg,
        rpg: pSnap.rpg,
        apg: pSnap.apg,
        mpg: pSnap.mpg,
        fg3Pct: pSnap.fg3Pct,
        ftPct: pSnap.ftPct,
        last5Ppg: pSnap.last5Ppg,
        rankPpg: pSnap.rankPpg,
        rankRpg: pSnap.rankRpg,
        rankApg: pSnap.rankApg,
        oppRankDef: oppSnap?.rankDef ?? null,
      });
    }

    // If no player stats yet (future game), use most recent DB-stored player contexts
    if (virtualPlayerContexts.length === 0) {
      const relevantPCtx = playerContextsFromDB.filter(
        (p) => p.teamId === game.homeTeamId || p.teamId === game.awayTeamId,
      );
      virtualPlayerContexts.push(...relevantPCtx);
    }

    const consensusOdds = game.odds.find((o) => o.source === "consensus") ?? game.odds[0] ?? null;

    const ctx: GameEventContext = {
      game: { ...game, homeTeam: game.homeTeam, awayTeam: game.awayTeam },
      context: virtualContext,
      playerContexts: virtualPlayerContexts,
      stats: game.playerStats,
      odds: consensusOdds,
    };

    // Compute active conditions
    const activeConditions = new Set<string>();
    for (const def of GAME_EVENT_CATALOG) {
      if (def.type !== "condition") continue;
      for (const side of def.sides) {
        const result = def.compute(ctx, side);
        if (result.hit) {
          activeConditions.add(`${def.key}:${side}`);
        }
      }
    }

    // Match against stored patterns
    const matchingPredictions: {
      conditions: string[];
      outcome: string;
      hitRate: number;
      hitCount: number;
      sampleSize: number;
      seasons: number;
      confidenceScore: number;
      valueScore: number;
    }[] = [];

    for (const pattern of patterns) {
      const allConditionsMet = pattern.conditions.every((c) => activeConditions.has(c));
      if (allConditionsMet) {
        matchingPredictions.push({
          conditions: pattern.conditions,
          outcome: pattern.outcome,
          hitRate: pattern.hitRate,
          hitCount: pattern.hitCount,
          sampleSize: pattern.sampleSize,
          seasons: pattern.seasons,
          confidenceScore: pattern.confidenceScore ?? 0,
          valueScore: pattern.valueScore ?? 0,
        });
      }
    }

    matchingPredictions.sort((a, b) => b.confidenceScore - a.confidenceScore || b.valueScore - a.valueScore);

    // Print output
    const homeLabel = game.homeTeam.code ?? game.homeTeam.name ?? `Team ${game.homeTeamId}`;
    const awayLabel = game.awayTeam.code ?? game.awayTeam.name ?? `Team ${game.awayTeamId}`;
    const spreadStr = consensusOdds?.spreadHome != null ? `Spread: ${homeLabel} ${consensusOdds.spreadHome > 0 ? "+" : ""}${consensusOdds.spreadHome}` : "";
    const totalStr = consensusOdds?.totalOver != null ? `O/U: ${consensusOdds.totalOver}` : "";
    const lineInfo = [spreadStr, totalStr].filter(Boolean).join("  |  ");

    console.log(`${homeLabel} vs ${awayLabel}${lineInfo ? `  |  ${lineInfo}` : ""}`);
    console.log(`  Record: ${homeLabel} ${homeSnap.wins}-${homeSnap.losses} (Off #${homeSnap.rankOff}, Def #${homeSnap.rankDef})  |  ${awayLabel} ${awaySnap.wins}-${awaySnap.losses} (Off #${awaySnap.rankOff}, Def #${awaySnap.rankDef})`);
    console.log(`  Active conditions: ${activeConditions.size > 0 ? [...activeConditions].join(", ") : "(none)"}`);

    if (matchingPredictions.length === 0) {
      console.log("  No matching patterns found.\n");
    } else {
      console.log(`  ${matchingPredictions.length} matching predictions:\n`);
      const top = matchingPredictions.slice(0, 10);
      for (let i = 0; i < top.length; i++) {
        const p = top[i];
        const edge = ((p.hitRate - 0.524) * 100).toFixed(1);
        console.log(`  [${i + 1}] ${p.conditions.join(" + ")}`);
        console.log(`      -> ${p.outcome}  |  ${(p.hitRate * 100).toFixed(1)}% (${p.hitCount}/${p.sampleSize})  |  ${p.seasons} seasons  |  edge ${Number(edge) >= 0 ? "+" : ""}${edge}%`);
      }
      console.log("");
    }

    const propsForGame = playerPropsByGame.get(game.id) ?? [];

    // V2 prediction engine (shared with dashboard)
    if (deployedV2.length > 0 && bins.size > 0) {
      const homePlayerCtxs = virtualPlayerContexts.filter((c) => c.teamId === game.homeTeamId);
      const awayPlayerCtxs = virtualPlayerContexts.filter((c) => c.teamId === game.awayTeamId);
      const getTopPlayer = (list: typeof virtualPlayerContexts, stat: "ppg" | "rpg" | "apg") => {
        if (list.length === 0) return null;
        const sorted = [...list].sort((a, b) => b[stat] - a[stat]);
        const top = sorted[0];
        const pInfo = playerNameMap.get(top.playerId);
        const name = pInfo ? `${pInfo.firstname ?? ""} ${pInfo.lastname ?? ""}`.trim() : `Player ${top.playerId}`;
        return { id: top.playerId, name, stat: top[stat] };
      };
      const gamePlayerCtx: GamePlayerContext = {
        homeTopScorer: getTopPlayer(homePlayerCtxs, "ppg"),
        homeTopRebounder: getTopPlayer(homePlayerCtxs, "rpg"),
        homeTopPlaymaker: getTopPlayer(homePlayerCtxs, "apg"),
        awayTopScorer: getTopPlayer(awayPlayerCtxs, "ppg"),
        awayTopRebounder: getTopPlayer(awayPlayerCtxs, "rpg"),
        awayTopPlaymaker: getTopPlayer(awayPlayerCtxs, "apg"),
      };

      const engineOdds = consensusOdds
        ? {
            spreadHome: (consensusOdds as any).spreadHome ?? null,
            totalOver: (consensusOdds as any).totalOver ?? null,
            mlHome: (consensusOdds as any).mlHome ?? (consensusOdds as any).moneylineHome ?? null,
            mlAway: (consensusOdds as any).mlAway ?? (consensusOdds as any).moneylineAway ?? null,
          }
        : null;
      const modelVersionName = activeVersion ? (activeVersion as any).name ?? null : null;

      const engineOutput = generateGamePredictions({
        season,
        gameContext: virtualContext,
        odds: engineOdds,
        deployedV2Patterns: deployedV2,
        featureBins: bins,
        metaModel,
        gamePlayerContext: gamePlayerCtx,
        propsForGame: propsForGame,
        maxBetPicksPerGame: Math.max(1, LEDGER_TUNING.maxBetPicksPerGame),
        fallbackAmericanOdds: LEDGER_TUNING.fallbackAmericanOdds,
        modelBundleVersion: modelVersionName ?? undefined,
        sourceTimestamps: {
          oddsTimestampUsed: consensusOdds?.fetchedAt?.toISOString?.() ?? null,
          statsSnapshotCutoff: targetDate.toISOString(),
          injuryLineupCutoff: targetDate.toISOString(),
        },
      });
      for (const record of engineOutput.canonicalPredictions) {
        assertPredictionRecord(record);
        canonicalRecords.push(record);
      }
      console.log(
        `  Feature snapshot: ${engineOutput.featureSnapshotId} | canonical predictions: ${engineOutput.canonicalPredictions.length}`,
      );
      if (engineOutput.rejectedPatternDiagnostics.length > 0) {
        for (const row of engineOutput.rejectedPatternDiagnostics) {
          rejectionArtifact.push({
            runId,
            gameId: game.id,
            patternId: row.patternId,
            reasons: row.reasons,
          });
        }
        await persistRejectionDiagnostics({
          runId,
          runDate: dateStr,
          gameId: game.id,
          rejectedPatternDiagnostics: engineOutput.rejectedPatternDiagnostics,
        });
        const reasonCounts = new Map<string, number>();
        for (const row of engineOutput.rejectedPatternDiagnostics) {
          for (const reason of row.reasons) {
            reasonCounts.set(reason, (reasonCounts.get(reason) ?? 0) + 1);
          }
        }
        const topReasons = [...reasonCounts.entries()]
          .sort((a, b) => b[1] - a[1])
          .slice(0, 4)
          .map(([reason, n]) => `${reason}:${n}`)
          .join(", ");
        console.log(
          `  Rejected patterns: ${engineOutput.rejectedPatternDiagnostics.length}` +
            `${topReasons ? ` (${topReasons})` : ""}`,
        );
      }

      if (engineOutput.discoveryV2Matches.length > 0) {
        console.log(`  Discovery v2 matches (${engineOutput.discoveryV2Matches.length}):`);
        for (const m of engineOutput.discoveryV2Matches.slice(0, 5)) {
          const clamp = (x: number) => Math.max(0, Math.min(1, x));
          console.log(
            `    ${(m.conditions ?? []).join(" + ")} -> ${m.outcomeType} | posterior ${(clamp(m.posteriorHitRate) * 100).toFixed(1)}% | edge ${(m.edge * 100).toFixed(1)}% | n=${m.n}`,
          );
        }
        console.log("");
      }

      if (engineOutput.modelPicks.length > 0) {
        console.log(`  Model picks (${engineOutput.modelPicks.length}):`);
        for (const mp of engineOutput.modelPicks.slice(0, 8)) {
          const targetStr = mp.playerTarget ? ` [${mp.playerTarget.name}]` : "";
          const marketStr = mp.marketPick ? " (has market)" : " (model-only)";
          console.log(
            `    ${mp.displayLabel}${targetStr} | prob ${(mp.modelProbability * 100).toFixed(1)}%` +
              `${mp.metaScore != null ? ` | meta ${(mp.metaScore * 100).toFixed(1)}%` : ""}` +
              ` | agree ${mp.agreementCount}${marketStr}`,
          );
        }
        console.log("");
      }

      if (engineOutput.suggestedBetPicks.length > 0) {
        console.log(`  Suggested bet picks (${engineOutput.suggestedBetPicks.length}):`);
        for (const p of engineOutput.suggestedBetPicks) {
          const targetStr = p.playerTarget ? ` [${p.playerTarget.name}]` : "";
          console.log(
            `    ${p.displayLabel ?? p.outcomeType}${targetStr} | post ${(p.posteriorHitRate * 100).toFixed(1)}%` +
              `${p.metaScore != null ? ` | meta ${(p.metaScore * 100).toFixed(1)}%` : ""}` +
              `${p.marketPick ? ` | edge ${(p.marketPick.edge * 100).toFixed(1)}% | EV ${(p.marketPick.ev * 100).toFixed(1)}%` : ""}`,
          );
        }
        console.log("");
      }

      // Log model picks for accuracy tracking
      const logEntries = engineOutput.modelPicks.map((mp) => ({
        gameId: game.id,
        gameDate: targetDate,
        outcomeType: mp.outcomeType,
        modelProb: mp.modelProbability,
        agreementCount: mp.agreementCount,
        metaScore: mp.metaScore,
        posteriorHitRate: mp.posteriorHitRate,
        confidence: mp.confidence,
        hadMarketPick: mp.marketPick != null,
        modelVersionName,
      }));
      if (logEntries.length > 0) {
        await prisma.predictionLog.createMany({
          data: logEntries,
          skipDuplicates: true,
        });
      }
    }
  }

  if (canonicalRecords.length > 0) {
    await persistCanonicalPredictions({
      records: canonicalRecords,
      runId,
      runStartedAtIso,
      runContext,
    });
    const outDir = path.join(process.cwd(), "data", "predictions", "canonical");
    await mkdir(outDir, { recursive: true });
    const outFile = path.join(outDir, `${dateStr}.json`);
    await writeFile(
      outFile,
      JSON.stringify(
        {
          runId,
          runStartedAt: runStartedAtIso,
          runContext,
          predictions: canonicalRecords,
        },
        null,
        2,
      ),
      "utf8",
    );
    console.log(`Saved canonical prediction artifacts: ${outFile} (${canonicalRecords.length} records)`);
  }
  if (rejectionArtifact.length > 0) {
    const outDir = path.join(process.cwd(), "data", "predictions", "canonical");
    await mkdir(outDir, { recursive: true });
    const outFile = path.join(outDir, `${dateStr}.rejections.json`);
    await writeFile(
      outFile,
      JSON.stringify(
        {
          runId,
          runStartedAt: runStartedAtIso,
          runContext,
          rejections: rejectionArtifact,
        },
        null,
        2,
      ),
      "utf8",
    );
    console.log(`Saved rejection diagnostics: ${outFile} (${rejectionArtifact.length} rows)`);
  }
}

export async function predictPlayers(args: string[] = []): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const dateStr = flags.date;
  if (!dateStr) {
    console.error("Usage: predict:players --date YYYY-MM-DD");
    process.exit(1);
  }

  const targetDate = new Date(dateStr + "T00:00:00Z");
  const season = Number(flags.season) || getSeasonForDate(targetDate);

  console.log(`\n=== Player Predictions for ${dateStr} (season ${season}) ===\n`);

  const games = await prisma.game.findMany({
    where: { date: targetDate },
    include: {
      homeTeam: true,
      awayTeam: true,
      playerStats: true,
      odds: true,
    },
  });

  if (games.length === 0) {
    console.log("No games found for this date.");
    return;
  }

  const allTeamIds = [...new Set(games.flatMap((g) => [g.homeTeamId, g.awayTeamId]))];
  const { teamSnapshots, playerSnapshots } = await computeContextForDate(season, targetDate, allTeamIds);

  // Load player patterns (those with player-related outcomes)
  const playerOutcomeKeys = [
    "PLAYER_30_PLUS", "PLAYER_40_PLUS", "PLAYER_DOUBLE_DOUBLE",
    "PLAYER_TRIPLE_DOUBLE", "PLAYER_10_PLUS_ASSISTS",
    "PLAYER_10_PLUS_REBOUNDS", "PLAYER_5_PLUS_THREES",
  ].map((k) => `${k}:game`);

  const patterns = await prisma.gamePattern.findMany({
    where: { outcome: { in: playerOutcomeKeys } },
    orderBy: { confidenceScore: "desc" },
  });

  console.log(`Loaded ${patterns.length} player-outcome patterns\n`);

  // Load players
  const playerIds = [...playerSnapshots.keys()];
  const players = await prisma.player.findMany({
    where: { id: { in: playerIds } },
  });
  const playerMap = new Map(players.map((p) => [p.id, p]));

  const pfdTeams = await prisma.team.findMany({ select: { id: true, city: true, name: true, code: true } });
  const pfdTeamLookup = buildTeamAliasLookup(pfdTeams);
  const pfdInjuryCache = new Map();
  const pfdInjuries = loadEarlyInjuriesForDate(dateStr, pfdTeamLookup, pfdInjuryCache);

  for (const game of games) {
    const homeSnap = teamSnapshots.get(game.homeTeamId);
    const awaySnap = teamSnapshots.get(game.awayTeamId);
    if (!homeSnap || !awaySnap) continue;

    const homeLabel = game.homeTeam.code ?? game.homeTeam.name ?? `Team ${game.homeTeamId}`;
    const awayLabel = game.awayTeam.code ?? game.awayTeam.name ?? `Team ${game.awayTeamId}`;

    console.log(`${homeLabel} vs ${awayLabel}`);

    // Build virtual context and run condition events (same as predictGames)
    const virtualContext = buildVirtualContext(game, homeSnap, awaySnap, targetDate, pfdInjuries);
    const virtualPlayerContexts = buildVirtualPlayerContexts(game, playerSnapshots, teamSnapshots);
    const consensusOdds = game.odds.find((o) => o.source === "consensus") ?? game.odds[0] ?? null;

    const ctx: GameEventContext = {
      game: { ...game, homeTeam: game.homeTeam, awayTeam: game.awayTeam },
      context: virtualContext,
      playerContexts: virtualPlayerContexts,
      stats: game.playerStats,
      odds: consensusOdds,
    };

    const activeConditions = new Set<string>();
    for (const def of GAME_EVENT_CATALOG) {
      if (def.type !== "condition") continue;
      for (const side of def.sides) {
        if (def.compute(ctx, side).hit) {
          activeConditions.add(`${def.key}:${side}`);
        }
      }
    }

    const matchingPredictions: {
      conditions: string[];
      outcome: string;
      hitRate: number;
      hitCount: number;
      sampleSize: number;
      seasons: number;
      confidenceScore: number;
    }[] = [];

    for (const pattern of patterns) {
      if (pattern.conditions.every((c) => activeConditions.has(c))) {
        matchingPredictions.push({
          conditions: pattern.conditions,
          outcome: pattern.outcome,
          hitRate: pattern.hitRate,
          hitCount: pattern.hitCount,
          sampleSize: pattern.sampleSize,
          seasons: pattern.seasons,
          confidenceScore: pattern.confidenceScore ?? 0,
        });
      }
    }

    matchingPredictions.sort((a, b) => b.confidenceScore - a.confidenceScore);

    // Show notable players
    const teamPlayers = [...playerSnapshots.values()].filter(
      (p) => p.teamId === game.homeTeamId || p.teamId === game.awayTeamId,
    );

    const notablePlayers = teamPlayers.filter(
      (p) => (p.rankPpg != null && p.rankPpg <= 20) || (p.rankRpg != null && p.rankRpg <= 20) || (p.rankApg != null && p.rankApg <= 20),
    );

    if (notablePlayers.length > 0) {
      console.log("  Notable players:");
      for (const p of notablePlayers.sort((a, b) => a.ppg > b.ppg ? -1 : 1).slice(0, 6)) {
        const info = playerMap.get(p.playerId);
        const name = info ? `${info.firstname ?? ""} ${info.lastname ?? ""}`.trim() : `Player ${p.playerId}`;
        const side = p.teamId === game.homeTeamId ? homeLabel : awayLabel;
        const ranks = [
          p.rankPpg != null ? `PPG #${p.rankPpg}` : null,
          p.rankRpg != null ? `RPG #${p.rankRpg}` : null,
          p.rankApg != null ? `APG #${p.rankApg}` : null,
        ].filter(Boolean).join(", ");
        console.log(`    ${name} (${side}): ${p.ppg.toFixed(1)}/${p.rpg.toFixed(1)}/${p.apg.toFixed(1)}  [${ranks}]`);
      }
    }

    if (matchingPredictions.length > 0) {
      console.log(`  ${matchingPredictions.length} player-outcome predictions:`);
      for (const p of matchingPredictions.slice(0, 5)) {
        const edge = ((p.hitRate - 0.524) * 100).toFixed(1);
        console.log(`    ${p.conditions.join(" + ")}  ->  ${p.outcome}  |  ${(p.hitRate * 100).toFixed(1)}% (${p.hitCount}/${p.sampleSize})  |  edge ${Number(edge) >= 0 ? "+" : ""}${edge}%`);
      }
    } else {
      console.log("  No player-outcome patterns matched.");
    }
    console.log("");
  }
}

function buildVirtualContext(
  game: { id: string; homeTeamId: number; awayTeamId: number },
  homeSnap: { wins: number; losses: number; ppg: number; oppg: number; pace: number | null; rebPg: number | null; astPg: number | null; fg3Pct: number | null; ftPct: number | null; rankOff: number | null; rankDef: number | null; rankPace: number | null; streak: number; lastGameDate: Date | null },
  awaySnap: typeof homeSnap,
  targetDate: Date,
  injuriesByTeam?: Map<number, import("./injuryContext").TeamInjurySummary>,
): GameContext {
  const homeRestDays = homeSnap.lastGameDate
    ? Math.floor((targetDate.getTime() - homeSnap.lastGameDate.getTime()) / 86_400_000) - 1
    : null;
  const awayRestDays = awaySnap.lastGameDate
    ? Math.floor((targetDate.getTime() - awaySnap.lastGameDate.getTime()) / 86_400_000) - 1
    : null;

  return {
    id: "",
    gameId: game.id,
    homeWins: homeSnap.wins,
    homeLosses: homeSnap.losses,
    homePpg: homeSnap.ppg,
    homeOppg: homeSnap.oppg,
    homePace: homeSnap.pace,
    homeRebPg: homeSnap.rebPg,
    homeAstPg: homeSnap.astPg,
    homeFg3Pct: homeSnap.fg3Pct,
    homeFtPct: homeSnap.ftPct,
    homeRankOff: homeSnap.rankOff,
    homeRankDef: homeSnap.rankDef,
    homeRankPace: homeSnap.rankPace,
    homeStreak: homeSnap.streak,
    awayWins: awaySnap.wins,
    awayLosses: awaySnap.losses,
    awayPpg: awaySnap.ppg,
    awayOppg: awaySnap.oppg,
    awayPace: awaySnap.pace,
    awayRebPg: awaySnap.rebPg,
    awayAstPg: awaySnap.astPg,
    awayFg3Pct: awaySnap.fg3Pct,
    awayFtPct: awaySnap.ftPct,
    awayRankOff: awaySnap.rankOff,
    awayRankDef: awaySnap.rankDef,
    awayRankPace: awaySnap.rankPace,
    awayStreak: awaySnap.streak,
    homeRestDays,
    awayRestDays,
    homeIsB2b: homeRestDays === 0,
    awayIsB2b: awayRestDays === 0,
    homeInjuryOutCount: injuriesByTeam?.get(game.homeTeamId)?.out ?? null,
    homeInjuryDoubtfulCount: injuriesByTeam?.get(game.homeTeamId)?.doubtful ?? null,
    homeInjuryQuestionableCount: injuriesByTeam?.get(game.homeTeamId)?.questionable ?? null,
    homeInjuryProbableCount: injuriesByTeam?.get(game.homeTeamId)?.probable ?? null,
    awayInjuryOutCount: injuriesByTeam?.get(game.awayTeamId)?.out ?? null,
    awayInjuryDoubtfulCount: injuriesByTeam?.get(game.awayTeamId)?.doubtful ?? null,
    awayInjuryQuestionableCount: injuriesByTeam?.get(game.awayTeamId)?.questionable ?? null,
    awayInjuryProbableCount: injuriesByTeam?.get(game.awayTeamId)?.probable ?? null,
    homeLineupCertainty: computeLineupSignalsFromCounts(injuriesByTeam?.get(game.homeTeamId)).certainty,
    awayLineupCertainty: computeLineupSignalsFromCounts(injuriesByTeam?.get(game.awayTeamId)).certainty,
    homeLateScratchRisk: computeLineupSignalsFromCounts(injuriesByTeam?.get(game.homeTeamId)).lateScratchRisk,
    awayLateScratchRisk: computeLineupSignalsFromCounts(injuriesByTeam?.get(game.awayTeamId)).lateScratchRisk,
    h2hHomeWins: 0,
    h2hAwayWins: 0,
  };
}

function buildVirtualPlayerContexts(
  game: { id: string; homeTeamId: number; awayTeamId: number; playerStats: { playerId: number; teamId: number }[] },
  playerSnapshots: Map<number, { playerId: number; teamId: number; gamesPlayed: number; ppg: number; rpg: number; apg: number; mpg: number; fg3Pct: number | null; ftPct: number | null; last5Ppg: number | null; rankPpg: number | null; rankRpg: number | null; rankApg: number | null }>,
  teamSnapshots: Map<number, { rankDef: number | null }>,
): import("@prisma/client").PlayerGameContext[] {
  const result: import("@prisma/client").PlayerGameContext[] = [];

  for (const stat of game.playerStats) {
    const pSnap = playerSnapshots.get(stat.playerId);
    if (!pSnap) continue;

    const oppTeamId = stat.teamId === game.homeTeamId ? game.awayTeamId : game.homeTeamId;
    const oppSnap = teamSnapshots.get(oppTeamId);

    result.push({
      id: "",
      gameId: game.id,
      playerId: pSnap.playerId,
      teamId: pSnap.teamId,
      gamesPlayed: pSnap.gamesPlayed,
      ppg: pSnap.ppg,
      rpg: pSnap.rpg,
      apg: pSnap.apg,
      mpg: pSnap.mpg,
      fg3Pct: pSnap.fg3Pct,
      ftPct: pSnap.ftPct,
      last5Ppg: pSnap.last5Ppg,
      rankPpg: pSnap.rankPpg,
      rankRpg: pSnap.rankRpg,
      rankApg: pSnap.rankApg,
      oppRankDef: oppSnap?.rankDef ?? null,
    });
  }

  return result;
}
