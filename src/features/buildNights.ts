import { prisma } from "../db/prisma.js";

const EVENT_LOGIC_VERSION = process.env.EVENT_LOGIC_VERSION ?? "unknown";

export async function buildNights(args: string[] = []): Promise<void> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }

  const season = flags.season ? Number(flags.season) : undefined;
  const isIncremental = season != null;

  if (isIncremental) {
    console.log(`Building Night rows for season ${season}...\n`);
  } else {
    console.log("Building Night rows (full rebuild)...\n");
  }

  const nightProcessedWhere: any = { eventKey: "NIGHT_PROCESSED" };
  if (season != null) nightProcessedWhere.season = season;

  const nightProcessedEvents = await prisma.nightEvent.findMany({
    where: nightProcessedWhere,
    orderBy: { date: "asc" },
  });

  console.log(`  Found ${nightProcessedEvents.length} NIGHT_PROCESSED events`);

  const catalogEvents = await prisma.nightEvent.findMany({
    where: {
      eventKey: { notIn: ["NIGHT_PROCESSED", "STATS_PRESENT"] },
      ...(season != null ? { season } : {}),
    },
    select: { date: true },
  });

  const eventHitsByDate = new Map<string, number>();
  for (const e of catalogEvents) {
    const key = e.date.toISOString().slice(0, 10);
    eventHitsByDate.set(key, (eventHitsByDate.get(key) ?? 0) + 1);
  }

  let upserted = 0;

  for (const np of nightProcessedEvents) {
    const meta = np.meta as Record<string, unknown> | null;
    const dateKey = np.date.toISOString().slice(0, 10);

    const gameCount = (meta?.gameCount as number) ?? 0;
    const statCount = (meta?.statCount as number) ?? 0;
    const eventHitCount = eventHitsByDate.get(dateKey) ?? (meta?.eventHits as number) ?? 0;
    const version = (meta?.eventLogicVersion as string) ?? EVENT_LOGIC_VERSION;

    await prisma.night.upsert({
      where: { date: np.date },
      update: {
        season: np.season,
        gameCount,
        statCount,
        eventHitCount,
        eventLogicVersion: version,
        processedAt: new Date(),
      },
      create: {
        date: np.date,
        season: np.season,
        gameCount,
        statCount,
        eventHitCount,
        eventLogicVersion: version,
      },
    });
    upserted++;
  }

  console.log(`\nDone. Upserted ${upserted} Night rows.`);
}
