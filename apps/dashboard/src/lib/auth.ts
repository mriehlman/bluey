import { prisma } from "@bluey/db";
import { PrismaAdapter } from "@next-auth/prisma-adapter";
import type { NextAuthOptions } from "next-auth";
import AppleProvider from "next-auth/providers/apple";
import CredentialsProvider from "next-auth/providers/credentials";
import GoogleProvider from "next-auth/providers/google";

/** Read env at runtime (avoid Next bundling a stale value from build time). */
function envIsTrue(name: string): boolean {
  const v = process.env[name]?.trim().toLowerCase();
  return v === "true" || v === "1" || v === "yes";
}

async function getOrCreateDevUser() {
  const fallbackUser = {
    id: "dev-user-local",
    name: "Bluey Dev User",
    email: "dev.user@bluey.local",
    image: "https://api.dicebear.com/9.x/thumbs/svg?seed=Bluey%20Dev%20User",
  };

  try {
    const db = prisma as any;
    if (db.user?.upsert) {
      return await db.user.upsert({
        where: { email: fallbackUser.email },
        create: fallbackUser,
        update: {
          name: fallbackUser.name,
          image: fallbackUser.image,
        },
        select: {
          id: true,
          name: true,
          email: true,
          image: true,
        },
      });
    }
  } catch {
    // Fall through to SQL fallback.
  }

  try {
    const rows = await prisma.$queryRawUnsafe<Array<{
      id: string;
      name: string | null;
      email: string | null;
      image: string | null;
    }>>(
      `INSERT INTO "User" ("id","name","email","image")
       VALUES ('${fallbackUser.id}','${fallbackUser.name}','${fallbackUser.email}','${fallbackUser.image}')
       ON CONFLICT ("email") DO UPDATE
       SET "name" = EXCLUDED."name",
           "image" = EXCLUDED."image"
       RETURNING "id","name","email","image"`,
    );
    if (rows[0]?.id) {
      return rows[0];
    }
  } catch {
    // Final fallback: allow dev login even when DB user tables are not ready.
  }

  return fallbackUser;
}

/**
 * Build NextAuth options when called (not at module load) so Vercel/runtime env
 * like DEV_AUTH_BYPASS is visible to the provider list.
 */
export function getAuthOptions(): NextAuthOptions & { trustHost?: boolean } {
  const providers: NextAuthOptions["providers"] = [];

  if (process.env.GOOGLE_CLIENT_ID && process.env.GOOGLE_CLIENT_SECRET) {
    providers.push(
      GoogleProvider({
        clientId: process.env.GOOGLE_CLIENT_ID,
        clientSecret: process.env.GOOGLE_CLIENT_SECRET,
      }),
    );
  }

  if (process.env.APPLE_ID && process.env.APPLE_SECRET) {
    providers.push(
      AppleProvider({
        clientId: process.env.APPLE_ID,
        clientSecret: process.env.APPLE_SECRET,
      }),
    );
  }

  const devAuthEnabled =
    envIsTrue("DEV_AUTH_BYPASS") &&
    (process.env.NODE_ENV !== "production" || envIsTrue("DEV_AUTH_ALLOW_PRODUCTION"));

  if (devAuthEnabled) {
    providers.push(
      CredentialsProvider({
        id: "dev-login",
        name: "Dev Login",
        credentials: {
          devKey: { label: "Dev Key", type: "text" },
        },
        async authorize() {
          return await getOrCreateDevUser();
        },
      }),
    );
  }

  return {
    adapter: PrismaAdapter(prisma as any) as any,
    secret: process.env.NEXTAUTH_SECRET,
    trustHost: true,
    session: {
      strategy: "jwt",
    },
    providers,
    callbacks: {
      jwt: ({ token, user }) => {
        if (user?.id) {
          token.sub = user.id;
        }
        return token;
      },
      session: ({ session, token }) => {
        if (session.user) {
          session.user.id = token.sub ?? "";
        }
        return session;
      },
    },
    pages: {
      signIn: "/",
    },
  };
}
