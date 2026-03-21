import { NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";

export const dynamic = "force-dynamic";

export async function GET() {
  const versions = await prisma.modelVersion.findMany({
    select: {
      id: true,
      name: true,
      description: true,
      isActive: true,
      stats: true,
      createdAt: true,
    },
    orderBy: { createdAt: "desc" },
  });

  return NextResponse.json({ versions });
}

export async function POST(req: Request) {
  const body = await req.json();
  const { action, name } = body as { action: string; name?: string };

  if (action === "activate" && name) {
    const version = await prisma.modelVersion.findUnique({ where: { name } });
    if (!version) {
      return NextResponse.json({ error: `Version "${name}" not found` }, { status: 404 });
    }
    await prisma.modelVersion.updateMany({
      where: { isActive: true },
      data: { isActive: false },
    });
    await prisma.modelVersion.update({
      where: { id: version.id },
      data: { isActive: true },
    });
    return NextResponse.json({ ok: true, message: `Version "${name}" activated` });
  }

  if (action === "deactivate") {
    await prisma.modelVersion.updateMany({
      where: { isActive: true },
      data: { isActive: false },
    });
    return NextResponse.json({ ok: true, message: "All versions deactivated. Using live data." });
  }

  return NextResponse.json({ error: "Invalid action. Use activate or deactivate." }, { status: 400 });
}
