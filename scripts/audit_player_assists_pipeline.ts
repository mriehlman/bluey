import { prisma } from "../packages/db/src";

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

type PredictionLogRow = {
  outcomeType: string;
  hadMarketPick: boolean;
  rows: number;
};

type LedgerRow = {
  outcomeType: string;
  gate: string;
  rows: number;
};

type ModelCountRow = {
  modelVersionName: string;
  rows: number;
};

async function main() {
  const model = process.argv[2] ?? "v1.8-hybrid-with-rebounds-2026-03-29";
  const from = process.argv[3] ?? "2025-10-01";
  const to = process.argv[4] ?? "2026-03-28";

  const assistsWhere = `(
    "outcomeType" LIKE '%ASSIST%'
    OR "outcomeType" LIKE '%PLAYMAKER%'
    OR "outcomeType" LIKE 'PLAYER_10_PLUS_ASSISTS:%'
  )`;

  const predictionLogRows = await prisma.$queryRawUnsafe<PredictionLogRow[]>(
    `SELECT
      "outcomeType" AS "outcomeType",
      COALESCE("hadMarketPick", FALSE) AS "hadMarketPick",
      COUNT(*)::int AS rows
    FROM "PredictionLog"
    WHERE COALESCE("modelVersionName",'') = '${sqlEsc(model)}'
      AND "gameDate" >= '${sqlEsc(from)}'::date
      AND "gameDate" <= '${sqlEsc(to)}'::date
      AND ${assistsWhere}
    GROUP BY 1,2
    ORDER BY 1 ASC, 2 ASC`,
  );

  const ledgerRows = await prisma.$queryRawUnsafe<LedgerRow[]>(
    `SELECT
      "outcomeType" AS "outcomeType",
      COALESCE("gateMode", CASE WHEN COALESCE("actionabilityVersion",'legacy_v1') LIKE 'strict%' THEN 'strict' ELSE 'legacy' END) AS gate,
      COUNT(*)::int AS rows
    FROM "SuggestedPlayLedger"
    WHERE COALESCE("modelVersionName",'live') = '${sqlEsc(model)}'
      AND "date" >= '${sqlEsc(from)}'::date
      AND "date" <= '${sqlEsc(to)}'::date
      AND ${assistsWhere}
    GROUP BY 1,2
    ORDER BY 1 ASC, 2 ASC`,
  );

  const predictionLogByModel = await prisma.$queryRawUnsafe<ModelCountRow[]>(
    `SELECT
      COALESCE("modelVersionName",'(null)') AS "modelVersionName",
      COUNT(*)::int AS rows
    FROM "PredictionLog"
    WHERE "gameDate" >= '${sqlEsc(from)}'::date
      AND "gameDate" <= '${sqlEsc(to)}'::date
      AND ${assistsWhere}
    GROUP BY 1
    ORDER BY rows DESC, 1 ASC`,
  );

  const ledgerByModel = await prisma.$queryRawUnsafe<ModelCountRow[]>(
    `SELECT
      COALESCE("modelVersionName",'(null)') AS "modelVersionName",
      COUNT(*)::int AS rows
    FROM "SuggestedPlayLedger"
    WHERE "date" >= '${sqlEsc(from)}'::date
      AND "date" <= '${sqlEsc(to)}'::date
      AND ${assistsWhere}
    GROUP BY 1
    ORDER BY rows DESC, 1 ASC`,
  );

  const generatedByMarketFlag = predictionLogRows.reduce(
    (acc, row) => {
      if (row.hadMarketPick) acc.withMarket += row.rows;
      else acc.withoutMarket += row.rows;
      return acc;
    },
    { withMarket: 0, withoutMarket: 0 },
  );

  const ledgerByGate = new Map<string, number>();
  for (const row of ledgerRows) {
    ledgerByGate.set(row.gate, (ledgerByGate.get(row.gate) ?? 0) + row.rows);
  }

  console.log(
    JSON.stringify(
      {
        model,
        from,
        to,
        generated: {
          totalRows: generatedByMarketFlag.withMarket + generatedByMarketFlag.withoutMarket,
          withMarketPick: generatedByMarketFlag.withMarket,
          withoutMarketPick: generatedByMarketFlag.withoutMarket,
          byOutcomeAndMarketPick: predictionLogRows,
        },
        ledger: {
          totalRows: ledgerRows.reduce((sum, r) => sum + r.rows, 0),
          byGate: [...ledgerByGate.entries()].map(([gate, rows]) => ({ gate, rows })),
          byOutcomeAndGate: ledgerRows,
        },
        assistsRowsByModelInWindow: {
          predictionLog: predictionLogByModel,
          suggestedPlayLedger: ledgerByModel,
        },
      },
      null,
      2,
    ),
  );
}

main()
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
