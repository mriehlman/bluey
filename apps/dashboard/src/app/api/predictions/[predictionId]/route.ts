import { NextResponse } from "next/server";
import { prisma } from "@bluey/db";

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

type CanonicalPredictionDetailRow = {
  predictionId: string;
  runId: string | null;
  runStartedAt: Date | string | null;
  runContext: unknown;
  gameId: string;
  market: string;
  selection: string;
  confidenceScore: number;
  edgeEstimate: number;
  generatedAt: Date | string;
  predictionContractVersion: string;
  modelBundleVersion: string;
  featureSchemaVersion: string;
  rankingPolicyVersion: string;
  aggregationPolicyVersion: string;
  featureSnapshotId: string;
  modelVotes: unknown;
  supportingPatterns: string[] | null;
  featureSnapshotPayload: unknown;
};

export const dynamic = "force-dynamic";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ predictionId: string }> },
) {
  const { predictionId } = await params;
  try {
    const rows = await prisma.$queryRawUnsafe<CanonicalPredictionDetailRow[]>(
      `SELECT
      cp."predictionId",
      cp."runId",
      cp."runStartedAt",
      cp."runContext",
      cp."gameId",
      cp."market",
      cp."selection",
      cp."confidenceScore",
      cp."edgeEstimate",
      cp."generatedAt",
      cp."predictionContractVersion",
      cp."modelBundleVersion",
      cp."featureSchemaVersion",
      cp."rankingPolicyVersion",
      cp."aggregationPolicyVersion",
      cp."featureSnapshotId",
      cp."modelVotes",
      cp."supportingPatterns",
      cp."featureSnapshotPayload"
    FROM "CanonicalPrediction" cp
    WHERE cp."predictionId" = '${sqlEsc(predictionId)}'
      LIMIT 1`,
    );

    const row = rows[0];
    if (!row) {
      return NextResponse.json(
        { message: "Prediction not found." },
        { status: 404 },
      );
    }

    return NextResponse.json({
      prediction: {
        predictionId: row.predictionId,
        runId: row.runId,
        runStartedAt: row.runStartedAt,
        runContext: row.runContext,
        gameId: row.gameId,
        market: row.market,
        selection: row.selection,
        confidenceScore: row.confidenceScore,
        edgeEstimate: row.edgeEstimate,
        generatedAt: row.generatedAt,
        predictionContractVersion: row.predictionContractVersion,
        modelBundleVersion: row.modelBundleVersion,
        featureSchemaVersion: row.featureSchemaVersion,
        rankingPolicyVersion: row.rankingPolicyVersion,
        aggregationPolicyVersion: row.aggregationPolicyVersion,
        featureSnapshotId: row.featureSnapshotId,
        modelVotes: row.modelVotes,
        supportingPatterns: row.supportingPatterns ?? [],
        featureSnapshotPayload: row.featureSnapshotPayload,
      },
    });
  } catch (error) {
    return NextResponse.json(
      {
        message:
          error instanceof Error
            ? `Prediction detail unavailable: ${error.message}`
            : "Prediction detail unavailable.",
      },
      { status: 503 },
    );
  }
}
