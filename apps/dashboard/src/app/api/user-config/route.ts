import { NextResponse } from "next/server";
import { prisma } from "@bluey/db";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";

function normalizeString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

export async function GET() {
  const session = await getServerSession(authOptions);
  const userId = session?.user?.id;

  if (!userId) {
    return NextResponse.json({ message: "Unauthorized" }, { status: 401 });
  }

  const db = prisma as any;
  const config = await db.userConfig.findUnique({
    where: { userId },
    select: {
      favoriteTeamCode: true,
      defaultStake: true,
      timezone: true,
      notes: true,
      updatedAt: true,
    },
  });

  return NextResponse.json({
    config: config ?? {
      favoriteTeamCode: null,
      defaultStake: null,
      timezone: null,
      notes: null,
      updatedAt: null,
    },
  });
}

export async function POST(req: Request) {
  const session = await getServerSession(authOptions);
  const userId = session?.user?.id;

  if (!userId) {
    return NextResponse.json({ message: "Unauthorized" }, { status: 401 });
  }

  const body = (await req.json()) as {
    favoriteTeamCode?: unknown;
    defaultStake?: unknown;
    timezone?: unknown;
    notes?: unknown;
  };

  const favoriteTeamCode = normalizeString(body.favoriteTeamCode);
  const timezone = normalizeString(body.timezone);
  const notes = normalizeString(body.notes);
  const defaultStake =
    typeof body.defaultStake === "number" && Number.isFinite(body.defaultStake)
      ? body.defaultStake
      : null;

  const db = prisma as any;
  const saved = await db.userConfig.upsert({
    where: { userId },
    create: {
      userId,
      favoriteTeamCode,
      timezone,
      notes,
      defaultStake,
    },
    update: {
      favoriteTeamCode,
      timezone,
      notes,
      defaultStake,
    },
    select: {
      favoriteTeamCode: true,
      defaultStake: true,
      timezone: true,
      notes: true,
      updatedAt: true,
    },
  });

  return NextResponse.json({ ok: true, config: saved });
}
