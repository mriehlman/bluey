import { prisma } from "@bluey/db";

type Family = "PLAYER" | "TOTAL" | "MONEYLINE" | "OTHER";

type LedgerRow = {
  id: string;
  date: string;
  outcomeType: string;
  displayLabel: string | null;
  targetPlayerName: string | null;
  settledResult: string;
  settledHit: boolean;
  confidence: number | null;
  posteriorHitRate: number | null;
  edge: number | null;
  votes: number | null;
  metaScore: number | null;
  priceAmerican: number | null;
  impliedProb: number | null;
  estimatedProb: number | null;
  rawWinProbability: number | null;
  calibratedWinProbability: number | null;
  impliedMarketProbability: number | null;
  edgeVsMarket: number | null;
  expectedValueScore: number | null;
  laneTag: string | null;
  regimeTags: string[] | null;
  sourceReliabilityScore: number | null;
  uncertaintyScore: number | null;
  uncertaintyPenaltyApplied: number | null;
  adjustedEdgeScore: number | null;
  profit: number | null;
  stake: number | null;
  homeCode: string;
  awayCode: string;
  homeScore: number;
  awayScore: number;
  modelVersionName: string | null;
  gateMode: string | null;
  dedupKey: string | null;
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
  if (base.startsWith("TOTAL_") || base.startsWith("OVER_") || base.startsWith("UNDER_")) return "TOTAL";
  return "OTHER";
}

function band(value: number | null, thresholds: number[], labels: string[]): string {
  if (value == null || !Number.isFinite(value)) return "N/A";
  for (let i = 0; i < thresholds.length; i++) {
    if (value < thresholds[i]) return labels[i];
  }
  return labels[labels.length - 1];
}

function pct(n: number, d: number): string {
  if (d === 0) return "---";
  return `${((n / d) * 100).toFixed(1)}%`;
}

function pad(s: string, w: number): string {
  return s.padEnd(w);
}

function rpad(s: string, w: number): string {
  return s.padStart(w);
}

function hr(width = 80): string {
  return "─".repeat(width);
}

function section(title: string): string {
  return `\n${"═".repeat(80)}\n  ${title}\n${"═".repeat(80)}`;
}

type BucketStats = { hits: number; misses: number; total: number; pnl: number };

function bucketTable(
  buckets: Map<string, BucketStats>,
  labelHeader: string,
  sortBy: "hitRate" | "misses" | "pnl" = "misses",
): string {
  const rows = [...buckets.entries()]
    .map(([label, s]) => ({ label, ...s, hitRate: s.total > 0 ? s.hits / s.total : 0 }))
    .sort((a, b) =>
      sortBy === "misses" ? b.misses - a.misses :
      sortBy === "pnl" ? a.pnl - b.pnl :
      a.hitRate - b.hitRate,
    );

  const lines: string[] = [];
  lines.push(
    `  ${pad(labelHeader, 28)} ${rpad("Picks", 6)} ${rpad("Hits", 6)} ${rpad("Miss", 6)} ${rpad("Hit%", 8)} ${rpad("PnL", 10)}`,
  );
  lines.push(`  ${hr(66)}`);
  for (const r of rows) {
    lines.push(
      `  ${pad(r.label, 28)} ${rpad(String(r.total), 6)} ${rpad(String(r.hits), 6)} ${rpad(String(r.misses), 6)} ${rpad(pct(r.hits, r.total), 8)} ${rpad(r.pnl >= 0 ? `+$${r.pnl.toFixed(2)}` : `-$${Math.abs(r.pnl).toFixed(2)}`, 10)}`,
    );
  }
  return lines.join("\n");
}

function addToBucket(buckets: Map<string, BucketStats>, key: string, row: LedgerRow) {
  const existing = buckets.get(key) ?? { hits: 0, misses: 0, total: 0, pnl: 0 };
  existing.total += 1;
  if (row.settledHit) existing.hits += 1;
  else existing.misses += 1;
  existing.pnl += Number(row.profit ?? 0);
  buckets.set(key, existing);
}

export async function reportPickDiagnostics(args: string[]): Promise<void> {
  const flags = parseFlags(args);
  const modelVersion = flags["model"] || flags["model-version"];
  const gateMode = flags["gate-mode"] || "legacy";
  const from = flags["from"];
  const to = flags["to"];
  const season = flags["season"] ? Number(flags["season"]) : undefined;
  const topN = Number(flags["top"] || "15");

  const whereClauses: string[] = [
    `l."settledResult" IN ('HIT','MISS')`,
    `l."isActionable" = TRUE`,
  ];
  if (modelVersion) whereClauses.push(`COALESCE(l."modelVersionName",'live') = '${modelVersion}'`);
  if (gateMode) {
    whereClauses.push(
      `COALESCE(l."gateMode", CASE WHEN COALESCE(l."actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict' ELSE 'legacy' END) = '${gateMode}'`,
    );
  }
  if (from) whereClauses.push(`l."date" >= '${from}'`);
  if (to) whereClauses.push(`l."date" <= '${to}'`);
  if (season) whereClauses.push(`l."season" = ${season}`);

  const rows = await prisma.$queryRawUnsafe<LedgerRow[]>(`
    SELECT
      l."id", l."date"::text as "date", l."outcomeType", l."displayLabel",
      l."targetPlayerName", l."settledResult", l."settledHit",
      l."confidence", l."posteriorHitRate", l."modelEdge" as "edge", l."votes", l."metaScore",
      l."priceAmerican", l."impliedProb", l."estimatedProb",
      l."rawWinProbability", l."calibratedWinProbability",
      l."impliedMarketProbability", l."edgeVsMarket", l."expectedValueScore",
      l."laneTag", l."regimeTags",
      l."sourceReliabilityScore", l."uncertaintyScore",
      l."uncertaintyPenaltyApplied", l."adjustedEdgeScore",
      l."profit", l."stake",
      ht."code" as "homeCode", at2."code" as "awayCode",
      g."homeScore", g."awayScore",
      l."modelVersionName", l."gateMode", l."dedupKey"
    FROM "SuggestedPlayLedger" l
    JOIN "Game" g ON g."id" = l."gameId"
    LEFT JOIN "Team" ht ON ht."id" = g."homeTeamId"
    LEFT JOIN "Team" at2 ON at2."id" = g."awayTeamId"
    WHERE ${whereClauses.join(" AND ")}
    ORDER BY l."date" DESC, l."outcomeType"
  `);

  if (rows.length === 0) {
    console.log("No settled picks found for the given filters.");
    console.log("Filters:", { modelVersion, gateMode, from, to, season });
    return;
  }

  const hits = rows.filter((r) => r.settledHit);
  const misses = rows.filter((r) => !r.settledHit);
  const totalPnl = rows.reduce((sum, r) => sum + Number(r.profit ?? 0), 0);

  // ── HEADER ──
  console.log(section("PICK DIAGNOSTICS REPORT"));
  console.log(`  Model: ${modelVersion ?? "all"} | Gate: ${gateMode ?? "all"}`);
  if (from || to) console.log(`  Date range: ${from ?? "..."} → ${to ?? "..."}`);
  if (season) console.log(`  Season: ${season}`);
  console.log(`  Total picks: ${rows.length} | Hits: ${hits.length} | Misses: ${misses.length} | Hit rate: ${pct(hits.length, rows.length)}`);
  console.log(`  Net PnL: ${totalPnl >= 0 ? "+" : ""}$${totalPnl.toFixed(2)}`);

  // ── BY OUTCOME FAMILY ──
  console.log(section("BREAKDOWN BY OUTCOME FAMILY"));
  const byFamily = new Map<string, BucketStats>();
  for (const row of rows) addToBucket(byFamily, familyForOutcome(row.outcomeType), row);
  console.log(bucketTable(byFamily, "Family", "misses"));

  // ── BY SPECIFIC OUTCOME TYPE ──
  console.log(section("BREAKDOWN BY OUTCOME TYPE"));
  const byOutcome = new Map<string, BucketStats>();
  for (const row of rows) addToBucket(byOutcome, row.outcomeType.replace(/:.*$/, ""), row);
  console.log(bucketTable(byOutcome, "Outcome Type", "misses"));

  // ── BY LANE TAG ──
  console.log(section("BREAKDOWN BY LANE TAG"));
  const byLane = new Map<string, BucketStats>();
  for (const row of rows) addToBucket(byLane, row.laneTag ?? "untagged", row);
  console.log(bucketTable(byLane, "Lane", "misses"));

  // ── BY CONFIDENCE BAND ──
  console.log(section("BREAKDOWN BY CONFIDENCE BAND"));
  const byConf = new Map<string, BucketStats>();
  const confBands = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
  const confLabels = ["<0.30", "0.30-0.39", "0.40-0.49", "0.50-0.59", "0.60-0.69", "0.70-0.79", "0.80+"];
  for (const row of rows) addToBucket(byConf, band(row.confidence, confBands, confLabels), row);
  console.log(bucketTable(byConf, "Confidence", "hitRate"));

  // ── BY EDGE BAND ──
  console.log(section("BREAKDOWN BY EDGE BAND"));
  const byEdge = new Map<string, BucketStats>();
  const edgeBands = [0.02, 0.05, 0.08, 0.12, 0.20];
  const edgeLabels = ["<2%", "2-5%", "5-8%", "8-12%", "12-20%", "20%+"];
  for (const row of rows) addToBucket(byEdge, band(row.edge, edgeBands, edgeLabels), row);
  console.log(bucketTable(byEdge, "Edge", "hitRate"));

  // ── BY UNCERTAINTY BAND ──
  console.log(section("BREAKDOWN BY UNCERTAINTY"));
  const byUncert = new Map<string, BucketStats>();
  const uncertBands = [0.1, 0.2, 0.3, 0.5];
  const uncertLabels = ["<0.10 (very low)", "0.10-0.19 (low)", "0.20-0.29 (medium)", "0.30-0.49 (high)", "0.50+ (very high)"];
  for (const row of rows) addToBucket(byUncert, band(row.uncertaintyScore, uncertBands, uncertLabels), row);
  console.log(bucketTable(byUncert, "Uncertainty", "hitRate"));

  // ── BY VOTE COUNT ──
  console.log(section("BREAKDOWN BY VOTE COUNT"));
  const byVotes = new Map<string, BucketStats>();
  for (const row of rows) {
    const v = row.votes ?? 1;
    const vLabel = v >= 5 ? "5+" : String(v);
    addToBucket(byVotes, `${vLabel} votes`, row);
  }
  console.log(bucketTable(byVotes, "Votes", "hitRate"));

  // ── BY SOURCE RELIABILITY ──
  console.log(section("BREAKDOWN BY SOURCE RELIABILITY"));
  const byReliability = new Map<string, BucketStats>();
  const relBands = [0.3, 0.5, 0.7, 0.9];
  const relLabels = ["<0.30 (weak)", "0.30-0.49 (fair)", "0.50-0.69 (good)", "0.70-0.89 (strong)", "0.90+ (excellent)"];
  for (const row of rows) addToBucket(byReliability, band(row.sourceReliabilityScore, relBands, relLabels), row);
  console.log(bucketTable(byReliability, "Source Reliability", "hitRate"));

  // ── BY DATE (daily hit rate) ──
  console.log(section("DAILY HIT RATE (worst days first)"));
  const byDate = new Map<string, BucketStats>();
  for (const row of rows) addToBucket(byDate, row.date, row);
  const dateRows = [...byDate.entries()]
    .map(([date, s]) => ({ date, ...s, hitRate: s.total > 0 ? s.hits / s.total : 0 }))
    .sort((a, b) => a.hitRate - b.hitRate)
    .slice(0, topN);
  console.log(`  ${pad("Date", 14)} ${rpad("Picks", 6)} ${rpad("Hits", 6)} ${rpad("Miss", 6)} ${rpad("Hit%", 8)} ${rpad("PnL", 10)}`);
  console.log(`  ${hr(52)}`);
  for (const r of dateRows) {
    console.log(
      `  ${pad(r.date, 14)} ${rpad(String(r.total), 6)} ${rpad(String(r.hits), 6)} ${rpad(String(r.misses), 6)} ${rpad(pct(r.hits, r.total), 8)} ${rpad(r.pnl >= 0 ? `+$${r.pnl.toFixed(2)}` : `-$${Math.abs(r.pnl).toFixed(2)}`, 10)}`,
    );
  }

  // ── REGIME TAG ANALYSIS ──
  console.log(section("REGIME TAG HIT RATES"));
  const byRegime = new Map<string, BucketStats>();
  for (const row of rows) {
    const tags = row.regimeTags ?? [];
    if (tags.length === 0) {
      addToBucket(byRegime, "(no regime tags)", row);
    } else {
      for (const tag of tags) addToBucket(byRegime, tag, row);
    }
  }
  console.log(bucketTable(byRegime, "Regime Tag", "hitRate"));

  // ── OVERCONFIDENT MISSES (high confidence picks that missed) ──
  console.log(section(`TOP ${topN} OVERCONFIDENT MISSES (high confidence but missed)`));
  const overconfidentMisses = misses
    .filter((r) => r.confidence != null)
    .sort((a, b) => (b.confidence ?? 0) - (a.confidence ?? 0))
    .slice(0, topN);

  for (let i = 0; i < overconfidentMisses.length; i++) {
    const r = overconfidentMisses[i];
    const matchup = `${r.awayCode} @ ${r.homeCode}`;
    const score = `${r.awayScore}-${r.homeScore}`;
    console.log(`\n  ${i + 1}. ${r.date} | ${matchup} (${score})`);
    console.log(`     Pick: ${r.displayLabel ?? r.outcomeType}`);
    console.log(`     Confidence: ${(r.confidence ?? 0).toFixed(3)} | Posterior: ${(r.posteriorHitRate ?? 0).toFixed(3)} | Edge: ${((r.edge ?? 0) * 100).toFixed(1)}%`);
    console.log(`     Votes: ${r.votes ?? "?"} | Lane: ${r.laneTag ?? "?"} | Uncertainty: ${(r.uncertaintyScore ?? 0).toFixed(3)}`);
    if (r.priceAmerican) console.log(`     Odds: ${r.priceAmerican > 0 ? "+" : ""}${r.priceAmerican} | Implied: ${((r.impliedMarketProbability ?? r.impliedProb ?? 0) * 100).toFixed(1)}%`);
    if (r.regimeTags?.length) console.log(`     Regime: ${r.regimeTags.join(", ")}`);
    if (r.targetPlayerName) console.log(`     Player: ${r.targetPlayerName}`);
  }

  // ── BIGGEST PNL LOSERS ──
  console.log(section(`TOP ${topN} BIGGEST PNL LOSSES`));
  const bigLosers = misses
    .filter((r) => r.profit != null)
    .sort((a, b) => (a.profit ?? 0) - (b.profit ?? 0))
    .slice(0, topN);

  for (let i = 0; i < bigLosers.length; i++) {
    const r = bigLosers[i];
    const matchup = `${r.awayCode} @ ${r.homeCode}`;
    const score = `${r.awayScore}-${r.homeScore}`;
    console.log(`\n  ${i + 1}. ${r.date} | ${matchup} (${score}) | Loss: -$${Math.abs(r.profit ?? 0).toFixed(2)}`);
    console.log(`     Pick: ${r.displayLabel ?? r.outcomeType}`);
    console.log(`     Confidence: ${(r.confidence ?? 0).toFixed(3)} | Edge: ${((r.edge ?? 0) * 100).toFixed(1)}% | Odds: ${r.priceAmerican ?? "?"}`);
    if (r.targetPlayerName) console.log(`     Player: ${r.targetPlayerName}`);
  }

  // ── EDGE VS REALITY ──
  console.log(section("CALIBRATION CHECK: PREDICTED VS ACTUAL HIT RATES"));
  const calibBands = [0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80];
  const calibLabels = ["<45%", "45-49%", "50-54%", "55-59%", "60-64%", "65-69%", "70-74%", "75-79%", "80%+"];
  const calibBuckets = new Map<string, { predicted: number[]; actual: number[] }>();
  for (const row of rows) {
    const prob = row.posteriorHitRate ?? row.calibratedWinProbability ?? row.rawWinProbability;
    if (prob == null) continue;
    const label = band(prob, calibBands, calibLabels);
    const bucket = calibBuckets.get(label) ?? { predicted: [], actual: [] };
    bucket.predicted.push(prob);
    bucket.actual.push(row.settledHit ? 1 : 0);
    calibBuckets.set(label, bucket);
  }
  console.log(`  ${pad("Predicted Band", 16)} ${rpad("Picks", 6)} ${rpad("Avg Pred", 9)} ${rpad("Actual%", 9)} ${rpad("Gap", 9)}`);
  console.log(`  ${hr(51)}`);
  for (const label of calibLabels) {
    const bucket = calibBuckets.get(label);
    if (!bucket || bucket.predicted.length === 0) continue;
    const avgPred = bucket.predicted.reduce((a, b) => a + b, 0) / bucket.predicted.length;
    const avgActual = bucket.actual.reduce((a, b) => a + b, 0) / bucket.actual.length;
    const gap = avgActual - avgPred;
    const gapStr = gap >= 0 ? `+${(gap * 100).toFixed(1)}%` : `${(gap * 100).toFixed(1)}%`;
    console.log(
      `  ${pad(label, 16)} ${rpad(String(bucket.predicted.length), 6)} ${rpad(`${(avgPred * 100).toFixed(1)}%`, 9)} ${rpad(`${(avgActual * 100).toFixed(1)}%`, 9)} ${rpad(gapStr, 9)}`,
    );
  }

  // ── ACTIONABLE SUGGESTIONS ──
  console.log(section("ACTIONABLE SUGGESTIONS"));
  const suggestions: string[] = [];

  // Check if any outcome family is dragging performance
  for (const [family, stats] of byFamily) {
    const hitRate = stats.total > 0 ? stats.hits / stats.total : 0;
    if (stats.total >= 10 && hitRate < 0.40) {
      suggestions.push(
        `⚠ ${family} picks are hitting at only ${(hitRate * 100).toFixed(1)}% (${stats.hits}/${stats.total}). ` +
        `Consider raising the minimum confidence threshold or disabling this lane.`,
      );
    }
  }

  // Check if low-vote picks are underperforming
  const singleVote = byVotes.get("1 votes");
  const multiVote = [...byVotes.entries()].filter(([k]) => k !== "1 votes");
  if (singleVote && singleVote.total >= 10) {
    const singleHr = singleVote.hits / singleVote.total;
    const multiTotal = multiVote.reduce((s, [, v]) => s + v.total, 0);
    const multiHits = multiVote.reduce((s, [, v]) => s + v.hits, 0);
    const multiHr = multiTotal > 0 ? multiHits / multiTotal : 0;
    if (singleHr < multiHr - 0.05) {
      suggestions.push(
        `⚠ Single-vote picks hit at ${(singleHr * 100).toFixed(1)}% vs ${(multiHr * 100).toFixed(1)}% for multi-vote. ` +
        `Consider requiring a minimum of 2 votes for bet picks.`,
      );
    }
  }

  // Check calibration
  for (const [label, bucket] of calibBuckets) {
    if (bucket.predicted.length < 10) continue;
    const avgPred = bucket.predicted.reduce((a, b) => a + b, 0) / bucket.predicted.length;
    const avgActual = bucket.actual.reduce((a, b) => a + b, 0) / bucket.actual.length;
    const gap = avgPred - avgActual;
    if (gap > 0.08) {
      suggestions.push(
        `⚠ Overconfident in the ${label} band: predicted ${(avgPred * 100).toFixed(1)}% but actual ${(avgActual * 100).toFixed(1)}%. ` +
        `Consider dampening confidence for picks in this range.`,
      );
    }
  }

  // Check high-uncertainty misses
  const highUncertStats = byUncert.get("0.30-0.49 (high)") ?? byUncert.get("0.50+ (very high)");
  if (highUncertStats && highUncertStats.total >= 5) {
    const hr2 = highUncertStats.hits / highUncertStats.total;
    if (hr2 < 0.45) {
      suggestions.push(
        `⚠ High-uncertainty picks are hitting at only ${(hr2 * 100).toFixed(1)}%. ` +
        `Consider tightening the uncertainty cap in tuning.ts.`,
      );
    }
  }

  // Check weak-source picks
  const weakSource = byReliability.get("<0.30 (weak)");
  if (weakSource && weakSource.total >= 5) {
    const hr3 = weakSource.hits / weakSource.total;
    if (hr3 < 0.45) {
      suggestions.push(
        `⚠ Weak-source-reliability picks are hitting at only ${(hr3 * 100).toFixed(1)}%. ` +
        `Consider raising the minimum source reliability threshold.`,
      );
    }
  }

  // Check regime tags
  for (const [tag, stats] of byRegime) {
    if (tag === "(no regime tags)" || stats.total < 10) continue;
    const hr4 = stats.hits / stats.total;
    if (hr4 < 0.40) {
      suggestions.push(
        `⚠ Regime "${tag}" is underperforming: ${(hr4 * 100).toFixed(1)}% hit rate (${stats.hits}/${stats.total}). ` +
        `Consider reducing exposure or adding a confidence penalty for this regime.`,
      );
    }
  }

  if (suggestions.length === 0) {
    console.log("  No major red flags detected in the current data.");
  } else {
    for (const s of suggestions) {
      console.log(`  ${s}\n`);
    }
  }

  // ── OUTPUT FILE ──
  const outPath = flags["out"];
  if (outPath) {
    const fs = await import("fs");
    const lines: string[] = [
      "date,matchup,outcome_type,display_label,player,result,confidence,posterior,edge,votes,uncertainty,lane,regime_tags,odds,implied_prob,profit",
    ];
    for (const row of rows) {
      lines.push([
        row.date,
        `${row.awayCode}@${row.homeCode}`,
        row.outcomeType.replace(/:.*$/, ""),
        (row.displayLabel ?? "").replace(/,/g, ";"),
        (row.targetPlayerName ?? "").replace(/,/g, ";"),
        row.settledResult,
        (row.confidence ?? "").toString(),
        (row.posteriorHitRate ?? "").toString(),
        (row.edge ?? "").toString(),
        (row.votes ?? "").toString(),
        (row.uncertaintyScore ?? "").toString(),
        row.laneTag ?? "",
        (row.regimeTags ?? []).join(";"),
        (row.priceAmerican ?? "").toString(),
        (row.impliedProb ?? "").toString(),
        (row.profit ?? "").toString(),
      ].join(","));
    }
    fs.writeFileSync(outPath, lines.join("\n"));
    console.log(`\n  CSV exported to: ${outPath}`);
  }

  console.log(`\n${"═".repeat(80)}`);
  console.log("  Tip: use --out <file.csv> to export raw data for deeper analysis");
  console.log(`  Usage: bun run cli report:pick-diagnostics --model <version> --gate-mode legacy --from 2026-01-01`);
  console.log(`${"═".repeat(80)}\n`);

  await prisma.$disconnect();
}
