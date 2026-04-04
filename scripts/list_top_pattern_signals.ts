import { prisma } from "../packages/db/src";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

const METRIC_MEANINGS: Record<string, string> = {
  away_rest_days: "Away team rest days before game",
  home_rest_days: "Home team rest days before game",
  rest_advantage_delta: "Difference between team rest days",
  injury_environment: "Overall injury context across both teams",
  injury_noise: "How volatile/noisy injury inputs are",
  role_dependency_band: "How star-dependent the matchup is overall",
  lineup_certainty: "Overall lineup certainty across both teams",
  data_completeness: "Completeness/confidence of required data inputs",
  streak_delta: "Difference in momentum/streak context",
  season_progress: "Progress through season timeline",
  season_phase: "Broad season phase grouping",
  away_role_dependency: "Away team dependency on top usage players",
  home_role_dependency: "Home team dependency on top usage players",
  away_injury_questionable: "Away team questionable-injury burden",
  home_injury_questionable: "Home team questionable-injury burden",
  away_late_scratch_risk: "Away late-scratch risk estimate",
  home_late_scratch_risk: "Home late-scratch risk estimate",
  away_injury_out: "Away team out-injury burden",
  home_injury_out: "Home team out-injury burden",
  home_lineup_certainty: "Home lineup certainty estimate",
  away_lineup_certainty: "Away lineup certainty estimate",
  lineup_certainty_delta: "Difference in lineup certainty",
  late_scratch_risk_delta: "Difference in late-scratch risk",
  injury_questionable_delta: "Difference in questionable-injury burden",
  injury_out_delta: "Difference in out-injury burden",
  away_rank_def: "Away defensive rank context",
  home_rank_def: "Home defensive rank context",
  home_rank_off: "Home offensive rank context",
  away_net_rating: "Away net rating context",
  home_net_rating: "Home net rating context",
  away_fg3_rate: "Away three-point attempt/make rate context",
  home_fg3_rate: "Home three-point attempt/make rate context",
  away_top3_last5ppg: "Away top-3 scorers recent points trend",
  home_top3_last5ppg: "Home top-3 scorers recent points trend",
  away_creation_burden: "Away offensive creation burden",
  home_creation_burden: "Home offensive creation burden",
  home_playmaking_resilience: "Home playmaking resilience under absences",
  away_playmaking_resilience: "Away playmaking resilience under absences",
  away_streak: "Away team streak bucket",
  home_streak: "Home team streak bucket",
  away_oppg: "Away opponent-points-allowed profile",
  home_oppg: "Home opponent-points-allowed profile",
  team_form_volatility: "Stability/volatility of recent team form",
};

function bucketMeaning(value: string): string {
  if (/^Q[1-5]$/.test(value)) {
    const rank = Number(value.slice(1));
    if (rank === 1) return "lowest quintile bucket";
    if (rank === 3) return "middle quintile bucket";
    if (rank === 5) return "highest quintile bucket";
    return rank < 3 ? "lower quintile bucket" : "upper quintile bucket";
  }
  if (value === "0") return "exactly zero";
  if (value === "EVEN") return "roughly balanced/no edge";
  if (value === "LOW") return "low bucket";
  if (value === "MID") return "mid bucket";
  if (value === "LATE") return "late-season bucket";
  if (value === "HIGH") return "high bucket";
  if (value === "CHAOTIC") return "high-chaos/high-uncertainty bucket";
  if (value === "STAR_LED") return "star-led dependency bucket";
  if (value === "WINNING_SIDE") return "leans to currently stronger streak side";
  if (value === "LOSING_SIDE") return "leans to currently weaker streak side";
  if (value.endsWith("_PLUS")) return `${value.replaceAll("_", " ").toLowerCase()} threshold`;
  return value.replaceAll("_", " ").toLowerCase();
}

function describeSignal(raw: string): string {
  const negated = raw.startsWith("!");
  const signal = negated ? raw.slice(1) : raw;
  const [metric, value] = signal.split(":");
  const metricText = METRIC_MEANINGS[metric] ?? metric.replaceAll("_", " ");
  if (!value) return negated ? `NOT ${metricText}` : metricText;
  const bucketText = bucketMeaning(value);
  return negated
    ? `NOT ${metricText} (${bucketText})`
    : `${metricText} (${bucketText})`;
}

async function main() {
  const requestedModel = process.argv[2] ?? null;
  const limit = Number(process.argv[3] ?? 200);
  const outputCsv = process.argv[4] ?? null;

  const active = requestedModel
    ? { name: requestedModel }
    : await prisma.modelVersion.findFirst({
        where: { isActive: true },
        select: { name: true },
      });

  if (!active?.name) {
    console.log(JSON.stringify({ error: "No active model found" }, null, 2));
    return;
  }

  const model = active.name;
  const safeModel = model.replaceAll("'", "''");
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 200;

  const rows = await prisma.$queryRawUnsafe<
    Array<{
      signal: string;
      uses: number;
      scoreAbsSum: number | null;
      avgPosterior: number | null;
    }>
  >(
    `SELECT
      cond AS signal,
      COUNT(*)::int AS uses,
      SUM(ABS(COALESCE((pat->>'score')::float8,0)))::float8 AS "scoreAbsSum",
      AVG(COALESCE((pat->>'posteriorHitRate')::float8,NULL))::float8 AS "avgPosterior"
    FROM "ModelVersion" mv
    CROSS JOIN LATERAL jsonb_array_elements(mv."deployedPatterns") pat
    CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(pat->'conditions','[]'::jsonb)) cond
    WHERE mv."name" = '${safeModel}'
    GROUP BY cond
    ORDER BY "scoreAbsSum" DESC, uses DESC, signal ASC
    LIMIT ${safeLimit}`,
  );

  if (outputCsv) {
    const header = "rank,signal,meaning,uses,scoreAbsSum,avgPosterior";
    const lines = rows.map((r, idx) => {
      const signal = `"${String(r.signal).replaceAll('"', '""')}"`;
      const meaning = `"${describeSignal(String(r.signal)).replaceAll('"', '""')}"`;
      const uses = Number(r.uses ?? 0);
      const scoreAbsSum = r.scoreAbsSum == null ? "" : String(r.scoreAbsSum);
      const avgPosterior = r.avgPosterior == null ? "" : String(r.avgPosterior);
      return `${idx + 1},${signal},${meaning},${uses},${scoreAbsSum},${avgPosterior}`;
    });
    const csv = [header, ...lines].join("\n");
    const outPath = path.isAbsolute(outputCsv)
      ? outputCsv
      : path.resolve(process.cwd(), outputCsv);
    await mkdir(path.dirname(outPath), { recursive: true });
    await writeFile(outPath, csv, "utf8");
    console.log(JSON.stringify({ model, limit: safeLimit, rows: rows.length, csvPath: outPath }, null, 2));
    return;
  }

  console.log(JSON.stringify({ model, limit: safeLimit, rows }, null, 2));
}

main()
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
