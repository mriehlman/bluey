import { prisma } from "../db/prisma.js";
import { CATALOG } from "./eventCatalog.js";
import type { NightContext, TeamAgg, EventResult } from "./eventCatalog.js";

const EVENT_LOGIC_VERSION = process.env.EVENT_LOGIC_VERSION ?? "unknown";

export async function explainNight(args: string[]): Promise<void> {
  const flags: Record<string, string> = {};
  const boolFlags = new Set<string>();
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--fix") {
      boolFlags.add("fix");
    } else if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const dateStr = flags.date;
  if (!dateStr) {
    console.error("Usage: events:explain --date YYYY-MM-DD [--fix]");
    process.exit(1);
  }

  const shouldFix = boolFlags.has("fix");
  const date = new Date(dateStr);

  const games = await prisma.game.findMany({
    where: { date },
    include: { homeTeam: true, awayTeam: true },
    orderBy: { homeTeamId: "asc" },
  });

  if (games.length === 0) {
    console.log(`No games found for ${dateStr}`);
    return;
  }

  const season = games[0].season;
  const gameIds = games.map((g) => g.id);

  const stats = await prisma.playerGameStat.findMany({
    where: { gameId: { in: gameIds } },
    include: { player: true },
  });

  const aggRows = await prisma.nightTeamAggregate.findMany({ where: { date } });
  const teamAggregates: TeamAgg[] | undefined =
    aggRows.length > 0
      ? aggRows.map((r) => ({
          teamId: r.teamId,
          points: r.points,
          rebounds: r.rebounds,
          assists: r.assists,
          steals: r.steals,
          blocks: r.blocks,
          turnovers: r.turnovers,
          minutes: r.minutes,
        }))
      : undefined;

  const storedEvents = await prisma.nightEvent.findMany({
    where: { date },
    orderBy: { eventKey: "asc" },
  });

  const ctx: NightContext = { date: dateStr, season, games, stats, teamAggregates };

  const liveResults = new Map<string, EventResult>();
  for (const def of CATALOG) {
    liveResults.set(def.key, def.compute(ctx));
  }

  console.log(`\n=== Event Debug Card: ${dateStr} (Season ${season}) ===\n`);

  // ── Games ──
  console.log(`  Games (${games.length}):\n`);
  console.log("    " + "Away".padEnd(22) + "Score".padStart(5) + "  @  " + "Home".padEnd(22) + "Score".padStart(5) + "  Margin".padStart(7));
  console.log("    " + "-".repeat(70));
  for (const g of games) {
    const awayName = [g.awayTeam.city, g.awayTeam.name].filter(Boolean).join(" ") || `#${g.awayTeamId}`;
    const homeName = [g.homeTeam.city, g.homeTeam.name].filter(Boolean).join(" ") || `#${g.homeTeamId}`;
    const margin = Math.abs(g.homeScore - g.awayScore);
    const winner = g.homeScore > g.awayScore ? "H" : g.awayScore > g.homeScore ? "A" : "T";
    console.log(
      `    ${awayName.padEnd(22)}${String(g.awayScore).padStart(5)}  @  ${homeName.padEnd(22)}${String(g.homeScore).padStart(5)}  ${String(margin).padStart(5)}${winner}`
    );
  }

  // ── Team Aggregates ──
  const teamAggs = teamAggregates ?? computeTeamAggs(stats);
  if (teamAggs.length > 0) {
    console.log(`\n  Team Aggregates (${teamAggs.length} teams):\n`);
    console.log("    " + "TeamId".padEnd(8) + "Pts".padStart(5) + "  Reb".padStart(5) + "  Ast".padStart(5) + "  Stl".padStart(5) + "  Blk".padStart(5) + "  Tov".padStart(5));
    console.log("    " + "-".repeat(40));
    const sorted = [...teamAggs].sort((a, b) => b.points - a.points);
    for (const a of sorted) {
      console.log(
        `    ${String(a.teamId).padEnd(8)}${String(a.points).padStart(5)}  ${String(a.rebounds).padStart(5)}  ${String(a.assists).padStart(5)}  ${String(a.steals).padStart(5)}  ${String(a.blocks).padStart(5)}  ${String(a.turnovers).padStart(5)}`
      );
    }
  }

  // ── Top Players ──
  if (stats.length > 0) {
    const topScorers = [...stats].sort((a, b) => b.points - a.points).slice(0, 10);
    console.log(`\n  Top Scorers:\n`);
    console.log("    " + "Player".padEnd(28) + "Team".padStart(6) + "  Pts".padStart(5) + "  Ast".padStart(5) + "  Reb".padStart(5) + "  Min".padStart(5));
    console.log("    " + "-".repeat(58));
    for (const s of topScorers) {
      const name = [s.player.firstname, s.player.lastname].filter(Boolean).join(" ") || `#${s.playerId}`;
      const mins = Math.floor(s.minutes / 60);
      console.log(
        `    ${name.padEnd(28)}${String(s.teamId).padStart(6)}  ${String(s.points).padStart(5)}  ${String(s.assists).padStart(5)}  ${String(s.rebounds).padStart(5)}  ${String(mins).padStart(5)}`
      );
    }
  }

  // ── Events: stored vs live (catalog only) ──
  console.log(`\n  Events:\n`);
  console.log("    " + "Event".padEnd(40) + "Stored".padStart(7) + "  Live".padStart(6) + "  Meta");
  console.log("    " + "-".repeat(80));

  const storedMap = new Map<string, typeof storedEvents[0]>();
  for (const e of storedEvents) storedMap.set(e.eventKey, e);

  const mismatches: string[] = [];

  for (const def of CATALOG) {
    const live = liveResults.get(def.key)!;
    const stored = storedMap.get(def.key);
    const storedHit = stored != null;
    const hitMatch = storedHit === live.hit;

    if (!hitMatch) mismatches.push(def.key);

    let metaSummary = "";
    if (live.hit && live.meta) {
      const parts: string[] = [];
      for (const [k, v] of Object.entries(live.meta)) {
        if (Array.isArray(v)) {
          parts.push(`${k}=[${v.length}]`);
        } else if (typeof v === "number") {
          parts.push(`${k}=${v}`);
        }
      }
      metaSummary = parts.join(", ");
    }

    const marker = hitMatch ? " " : "!";
    console.log(
      `  ${marker} ${def.key.padEnd(40)}${(storedHit ? "HIT" : "-").padStart(7)}  ${(live.hit ? "HIT" : "-").padStart(6)}  ${metaSummary}`
    );
  }

  // Infra events (display only, not included in mismatch check)
  for (const infraKey of ["NIGHT_PROCESSED", "STATS_PRESENT"]) {
    const stored = storedMap.get(infraKey);
    if (stored) {
      const meta = stored.meta as Record<string, unknown> | null;
      let metaSummary = "";
      if (meta) {
        const parts: string[] = [];
        for (const [k, v] of Object.entries(meta)) {
          parts.push(`${k}=${JSON.stringify(v)}`);
        }
        metaSummary = parts.join(", ");
      }
      console.log(
        `    ${"* " + infraKey}`.padEnd(44) + `${"HIT".padStart(7)}  ${"--".padStart(6)}  ${metaSummary}`
      );
    }
  }

  if (mismatches.length > 0) {
    console.log(`\n  MISMATCHES (stored vs live): ${mismatches.join(", ")}`);
    console.log("    This may indicate events were recomputed with different logic.");
    if (!shouldFix) {
      console.log("    Run with --fix to rewrite events for this date.");
    }
  }

  // ── --fix: atomic rewrite of events for this date ──
  if (shouldFix) {
    console.log(`\n  Fixing events for ${dateStr} (version: ${EVENT_LOGIC_VERSION})...`);

    const catalogKeys = CATALOG.map((d) => d.key);
    const allKeysToDelete = [...catalogKeys, "NIGHT_PROCESSED", "STATS_PRESENT"];

    const newEvents: { date: Date; season: number; eventKey: string; meta: unknown }[] = [];

    for (const def of CATALOG) {
      const result = liveResults.get(def.key)!;
      if (result.hit) {
        newEvents.push({
          date,
          season,
          eventKey: def.key,
          meta: result.meta ?? null,
        });
      }
    }

    const catalogHitCount = newEvents.length;

    newEvents.push({
      date,
      season,
      eventKey: "NIGHT_PROCESSED",
      meta: {
        gameCount: games.length,
        statCount: stats.length,
        eventHits: catalogHitCount,
        rebuiltAt: new Date().toISOString(),
        eventLogicVersion: EVENT_LOGIC_VERSION,
      },
    });

    if (stats.length > 0) {
      newEvents.push({
        date,
        season,
        eventKey: "STATS_PRESENT",
        meta: { statCount: stats.length },
      });
    }

    const result = await prisma.$transaction(async (tx) => {
      await tx.nightEvent.deleteMany({
        where: { date, eventKey: { in: allKeysToDelete } },
      });

      return tx.nightEvent.createMany({
        data: newEvents.map((e) => ({
          date: e.date,
          season: e.season,
          eventKey: e.eventKey,
          value: true,
          meta: e.meta as any,
        })),
        skipDuplicates: true,
      });
    });

    console.log(`  Rewrote ${result.count} events for ${dateStr} (${catalogHitCount} catalog + infra)`);
  }

  console.log(`\n=== End Debug Card ===`);
}

function computeTeamAggs(stats: { teamId: number; points: number; rebounds: number; assists: number; steals: number; blocks: number; turnovers: number; minutes: number }[]): TeamAgg[] {
  const map = new Map<number, TeamAgg>();
  for (const s of stats) {
    let agg = map.get(s.teamId);
    if (!agg) {
      agg = { teamId: s.teamId, points: 0, rebounds: 0, assists: 0, steals: 0, blocks: 0, turnovers: 0, minutes: 0 };
      map.set(s.teamId, agg);
    }
    agg.points += s.points;
    agg.rebounds += s.rebounds;
    agg.assists += s.assists;
    agg.steals += s.steals;
    agg.blocks += s.blocks;
    agg.turnovers += s.turnovers;
    agg.minutes += s.minutes;
  }
  return [...map.values()];
}
