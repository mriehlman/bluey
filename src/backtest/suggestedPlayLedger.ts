import { prisma } from "../db/prisma.js";

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

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

function americanToDecimal(american: number): number {
  if (american > 0) return 1 + american / 100;
  if (american < 0) return 1 + 100 / Math.abs(american);
  return 2;
}

export async function analyzeSuggestedPlayLedger(args: string[] = []): Promise<void> {
  const flags = parseFlags(args);
  const startingBankroll = Number(flags.bankroll ?? 1000);
  const fallbackStake = Number(flags.stake ?? 10);
  const actionableOnly = (flags["actionable-only"] ?? "true") !== "false";
  const from = flags.from;
  const to = flags.to;
  const season = flags.season ? Number(flags.season) : null;

  const where: string[] = [`l."settledResult" IN ('HIT','MISS')`];
  if (actionableOnly) where.push(`l."isActionable" = TRUE`);
  if (from) where.push(`l."date" >= '${sqlEsc(from)}'`);
  if (to) where.push(`l."date" <= '${sqlEsc(to)}'`);
  if (season != null) where.push(`l."season" = ${season}`);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  let rows: Array<{
    date: Date;
    season: number;
    settledHit: boolean | null;
    settledResult: string | null;
    stake: number | null;
    priceAmerican: number | null;
    profit: number | null;
  }> = [];
  try {
    rows = await prisma.$queryRawUnsafe<
      Array<{
        date: Date;
        season: number;
        settledHit: boolean | null;
        settledResult: string | null;
        stake: number | null;
        priceAmerican: number | null;
        profit: number | null;
      }>
    >(
      `SELECT
        l."date" as "date",
        l."season" as "season",
        l."settledHit" as "settledHit",
        l."settledResult" as "settledResult",
        l."stake" as "stake",
        l."priceAmerican" as "priceAmerican",
        l."profit" as "profit"
       FROM "SuggestedPlayLedger" l
       ${whereSql}
       ORDER BY l."date" ASC, l."id" ASC`,
    );
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    if (message.includes("SuggestedPlayLedger")) {
      console.log(
        "SuggestedPlayLedger table not found yet. Run prisma migrations first, then fetch /predictions to capture picks.",
      );
      return;
    }
    throw err;
  }

  console.log("\n=== Suggested Play Ledger Bankroll ===\n");
  console.log(
    `Scope: actionableOnly=${actionableOnly}, from=${from ?? "n/a"}, to=${to ?? "n/a"}, season=${season ?? "all"}`,
  );
  console.log(`Rows: ${rows.length}`);
  if (rows.length === 0) return;

  let bankroll = startingBankroll;
  let peak = bankroll;
  let maxDrawdown = 0;
  let wins = 0;
  let losses = 0;
  let totalWagered = 0;

  const bySeason = new Map<number, { bets: number; wins: number; pnl: number }>();

  for (const row of rows) {
    const stake = Number.isFinite(row.stake ?? NaN) ? Number(row.stake) : fallbackStake;
    const hit = row.settledHit ?? row.settledResult === "HIT";
    let profit: number;
    if (row.profit != null && Number.isFinite(row.profit)) {
      profit = Number(row.profit);
    } else if (row.priceAmerican != null && Number.isFinite(row.priceAmerican)) {
      const decimal = americanToDecimal(Number(row.priceAmerican));
      profit = hit ? stake * (decimal - 1) : -stake;
    } else {
      continue;
    }

    bankroll += profit;
    totalWagered += stake;
    if (hit) wins++;
    else losses++;
    peak = Math.max(peak, bankroll);
    if (peak > 0) {
      maxDrawdown = Math.max(maxDrawdown, (peak - bankroll) / peak);
    }

    const seasonAgg = bySeason.get(row.season) ?? { bets: 0, wins: 0, pnl: 0 };
    seasonAgg.bets += 1;
    seasonAgg.wins += hit ? 1 : 0;
    seasonAgg.pnl += profit;
    bySeason.set(row.season, seasonAgg);
  }

  const bets = wins + losses;
  const hitRate = bets > 0 ? wins / bets : 0;
  const netPnL = bankroll - startingBankroll;
  const roi = totalWagered > 0 ? netPnL / totalWagered : 0;

  console.log(`Hit rate: ${(hitRate * 100).toFixed(2)}% (${wins}/${bets})`);
  console.log(`Net P&L: $${netPnL.toFixed(2)}`);
  console.log(`Final bankroll: $${bankroll.toFixed(2)} (start $${startingBankroll.toFixed(2)})`);
  console.log(`ROI on amount wagered: ${(roi * 100).toFixed(2)}%`);
  console.log(`Max drawdown: ${(maxDrawdown * 100).toFixed(2)}%`);

  const seasons = [...bySeason.keys()].sort((a, b) => a - b);
  if (seasons.length > 0) {
    console.log("\nBy season:");
    for (const s of seasons) {
      const agg = bySeason.get(s);
      if (!agg) continue;
      const sHitRate = agg.bets > 0 ? agg.wins / agg.bets : 0;
      const sRoi = agg.bets > 0 ? agg.pnl / (agg.bets * fallbackStake) : 0;
      console.log(
        `  ${s}: ${(sHitRate * 100).toFixed(2)}% (${agg.wins}/${agg.bets}), pnl=$${agg.pnl.toFixed(2)}, roi=${(sRoi * 100).toFixed(2)}%`,
      );
    }
  }
}
