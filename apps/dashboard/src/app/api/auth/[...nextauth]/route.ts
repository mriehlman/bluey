import NextAuth from "next-auth";
import type { NextRequest } from "next/server";
import { getAuthOptions } from "@/lib/auth";

type RouteContext = { params: Promise<{ nextauth: string[] }> };

export async function GET(req: NextRequest, context: RouteContext) {
  return NextAuth(req, context, getAuthOptions());
}

export async function POST(req: NextRequest, context: RouteContext) {
  return NextAuth(req, context, getAuthOptions());
}
