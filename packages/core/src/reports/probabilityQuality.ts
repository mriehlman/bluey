import { prisma } from "@bluey/db";

type Family = "PLAYER" | "TOTAL" | "SPREAD" | "MONEYLINE" | "OTHER";
type SourceKey = "raw" | "calibrated" | "market" | "blended";

type LedgerRow = {
  outcomeType: string;
  settledResult: string | null;
  impliedProb: number | null;
  estimatedProb: number | null;
  posteriorHitRate: number | null;
  metaScore: number | null;
  priceAmerican: number | null;
};

type SourceStats = {
  n: number;
  logLoss: number;
  brier: number;
  bets: number;
  staked: number;
  pnl: number;
};

function parseFlags(args: string[]): Record<string, string> {
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length && !args[i + 1].startsWith("--")) {
      flags[args[i].slice(2)] = args[i + 1];
      i++;
    } else if (args[i].startsWith("--")) {
      flags[args[i].slice(2)] = "true";
    }
  }
  return flags;
}

function familyForOutcome(outcomeType: string): Family {
  const base = outcomeType.replace(/:.*$/, "");
  if (base.startsWith("PLAYER_") || base.includes("TOP_")) return "PLAYER";
  if (base.endsWith("_WIN") || base === "HOME_WIN" || base === "AWAY_WIN") return "MONEYLINE";
  if (base.includes("COVERED")) return "SPREAD";
  if (base.startsWith("TOTAL_") || base.startsWith("OVER_") || base.startsWith("UNDER_")) return "TOTAL";
  return "OTHER";
}

function clampProb(p: number | null | undefined): number | null {
  if (p == null || !Number.isFinite(p)) return null;
  return Math.min(1 - 1e-6, Math.max(1e-6, p));
}

function payoutFromAmerican(american: number): number {
  return american > 0 ? american / 100 : 100 / Math.abs(american);
}

function settledHit(row: LedgerRow): number | null {
  if (row.settledResult === "HIT") return 1;
  if (row.settledResult === "MISS") return 0;
  return null;
}

function emptySourceStats(): SourceStats {
  return { n: 0, logLoss: 0, brier: 0, bets: 0, staked: 0, pnl: 0 };
}

function strategyShouldBet(source: SourceKey, prob: number, implied: number): boolean {
  if (source === "market") return true;
  return prob > implied;
}

function formatPercent(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return "n/a";
  return `${(v * 100).toFixed(2)}%`;
}

function sourceProbForRow(row: LedgerRow, source: SourceKey): number | null {
  if (source === "raw") return clampProb(row.posteriorHitRate);
  if (source === "calibrated") return clampProb(row.metaScore ?? row.posteriorHitRate);
  if (source === "market") return clampProb(row.impliedProb);
  return clampProb(row.estimatedProb ?? row.metaScore ?? row.posteriorHitRate);
}

export async function reportProbabilityQuality(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const from = flags.from;
  const to = flags.to;
  const season = flags.season ? Number(flags.season) : null;
  const stake = Number(flags.stake ?? 10);
  const actionableOnly = (flags["actionable-only"] ?? "true") !== "false";

  const where: string[] = [`l."settledResult" IN ('HIT','MISS')`];
  if (actionableOnly) where.push(`l."isActionable" = TRUE`);
  if (from) where.push(`l."date" >= '${from.replaceAll("'", "''")}'`);
  if (to) where.push(`l."date" <= '${to.replaceAll("'", "''")}'`);
  if (season != null && Number.isFinite(season)) where.push(`l."season" = ${season}`);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const rows = await prisma.$queryRawUnsafe<LedgerRow[]>(
    `SELECT
      l."outcomeType" as "outcomeType",
      l."settledResult" as "settledResult",
      l."impliedProb" as "impliedProb",
      l."estimatedProb" as "estimatedProb",
      l."posteriorHitRate" as "posteriorHitRate",
      l."metaScore" as "metaScore",
      l."priceAmerican" as "priceAmerican"
     FROM "SuggestedPlayLedger" l
     ${whereSql}`,
  );

  console.log("\n=== Probability Quality Report ===\n");
  console.log(
    `Scope: rows=${rows.length}, actionableOnly=${actionableOnly}, stake=$${stake.toFixed(2)}, from=${from ?? "n/a"}, to=${to ?? "n/a"}, season=${season ?? "all"}`,
  );
  if (rows.length === 0) return;

  const sources: SourceKey[] = ["raw", "calibrated", "market", "blended"];
  const byFamily = new Map<Family | "ALL", Record<SourceKey, SourceStats>>();
  const families: Array<Family | "ALL"> = ["ALL", "PLAYER", "TOTAL", "SPREAD", "MONEYLINE", "OTHER"];
  for (const f of families) {
    byFamily.set(f, {
      raw: emptySourceStats(),
      calibrated: emptySourceStats(),
      market: emptySourceStats(),
      blended: emptySourceStats(),
    });
  }

  for (const row of rows) {
    const y = settledHit(row);
    const implied = clampProb(row.impliedProb);
    if (y == null || implied == null) continue;
    const family = familyForOutcome(row.outcomeType);
    const groups: Array<Family | "ALL"> = ["ALL", family];
    for (const source of sources) {
      const p = sourceProbForRow(row, source);
      if (p == null) continue;
      for (const g of groups) {
        const stats = byFamily.get(g)![source];
        stats.n += 1;
        stats.logLoss += -(y * Math.log(p) + (1 - y) * Math.log(1 - p));
        stats.brier += (p - y) ** 2;
        const shouldBet = strategyShouldBet(source, p, implied);
        if (!shouldBet) continue;
        const price = row.priceAmerican;
        if (price == null || !Number.isFinite(price) || price === 0) continue;
        stats.bets += 1;
        stats.staked += stake;
        stats.pnl += y ? stake * payoutFromAmerican(price) : -stake;
      }
    }
  }

  const printFamily = (family: Family | "ALL"): void => {
    const bucket = byFamily.get(family)!;
    console.log(`\n${family}`);
    for (const source of sources) {
      const s = bucket[source];
      const ll = s.n > 0 ? s.logLoss / s.n : null;
      const brier = s.n > 0 ? s.brier / s.n : null;
      const roi = s.staked > 0 ? s.pnl / s.staked : null;
      console.log(
        `  ${source.padEnd(10)} n=${String(s.n).padStart(4)} | logloss=${ll == null ? "n/a" : ll.toFixed(4)} | brier=${brier == null ? "n/a" : brier.toFixed(4)} | bets=${String(s.bets).padStart(4)} | roi=${formatPercent(roi)}`,
      );
    }
  };

  for (const family of families) {
    printFamily(family);
  }
}
