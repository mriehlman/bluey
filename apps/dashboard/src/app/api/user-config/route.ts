import { NextResponse } from "next/server";
import { prisma } from "@bluey/db";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";

function normalizeString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function sqlEsc(input: string): string {
  return input.replaceAll("'", "''");
}

function sqlStr(v: string | null): string {
  return v == null ? "NULL" : `'${sqlEsc(v)}'`;
}

function sqlNum(v: number | null): string {
  return v == null || !Number.isFinite(v) ? "NULL" : String(v);
}

type UserConfigRow = {
  favoriteTeamCode: string | null;
  defaultStake: number | null;
  timezone: string | null;
  notes: string | null;
  updatedAt: Date | string | null;
};

async function readUserConfig(userId: string): Promise<UserConfigRow | null> {
  const db = prisma as any;
  if (db.userConfig?.findUnique) {
    return (await db.userConfig.findUnique({
      where: { userId },
      select: {
        favoriteTeamCode: true,
        defaultStake: true,
        timezone: true,
        notes: true,
        updatedAt: true,
      },
    })) as UserConfigRow | null;
  }

  const rows = await prisma.$queryRawUnsafe<UserConfigRow[]>(
    `SELECT "favoriteTeamCode","defaultStake","timezone","notes","updatedAt"
     FROM "UserConfig"
     WHERE "userId" = '${sqlEsc(userId)}'
     LIMIT 1`,
  );
  return rows[0] ?? null;
}

async function writeUserConfig(args: {
  userId: string;
  favoriteTeamCode: string | null;
  defaultStake: number | null;
  timezone: string | null;
  notes: string | null;
}): Promise<UserConfigRow> {
  const db = prisma as any;
  if (db.userConfig?.upsert) {
    return (await db.userConfig.upsert({
      where: { userId: args.userId },
      create: {
        userId: args.userId,
        favoriteTeamCode: args.favoriteTeamCode,
        timezone: args.timezone,
        notes: args.notes,
        defaultStake: args.defaultStake,
      },
      update: {
        favoriteTeamCode: args.favoriteTeamCode,
        timezone: args.timezone,
        notes: args.notes,
        defaultStake: args.defaultStake,
      },
      select: {
        favoriteTeamCode: true,
        defaultStake: true,
        timezone: true,
        notes: true,
        updatedAt: true,
      },
    })) as UserConfigRow;
  }

  const rows = await prisma.$queryRawUnsafe<UserConfigRow[]>(
    `INSERT INTO "UserConfig" ("id","userId","favoriteTeamCode","defaultStake","timezone","notes","createdAt","updatedAt")
     VALUES ('${crypto.randomUUID()}','${sqlEsc(args.userId)}',${sqlStr(args.favoriteTeamCode)},${sqlNum(args.defaultStake)},${sqlStr(args.timezone)},${sqlStr(args.notes)},NOW(),NOW())
     ON CONFLICT ("userId") DO UPDATE SET
       "favoriteTeamCode" = EXCLUDED."favoriteTeamCode",
       "defaultStake" = EXCLUDED."defaultStake",
       "timezone" = EXCLUDED."timezone",
       "notes" = EXCLUDED."notes",
       "updatedAt" = NOW()
     RETURNING "favoriteTeamCode","defaultStake","timezone","notes","updatedAt"`,
  );

  return rows[0];
}

export async function GET() {
  const session = await getServerSession(authOptions);
  const userId = session?.user?.id;

  if (!userId) {
    return NextResponse.json({ message: "Unauthorized" }, { status: 401 });
  }

  let config: UserConfigRow | null = null;
  try {
    config = await readUserConfig(userId);
  } catch {
    config = null;
  }

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

  let saved: UserConfigRow;
  try {
    saved = await writeUserConfig({
      userId,
      favoriteTeamCode,
      defaultStake,
      timezone,
      notes,
    });
  } catch (err) {
    return NextResponse.json(
      {
        ok: false,
        message:
          err instanceof Error
            ? err.message
            : "Failed to save user settings. Ensure UserConfig table exists.",
      },
      { status: 500 },
    );
  }

  return NextResponse.json({ ok: true, config: saved });
}
