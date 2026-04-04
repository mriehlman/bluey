/**
 * Pushes the Prisma schema to the production DB.
 * Wraps `prisma db push` with PROD_DATABASE_URL.
 */
import { $ } from "bun";

const prodUrl = process.env.PROD_DATABASE_URL;
if (!prodUrl) {
  console.error("PROD_DATABASE_URL is not set in .env");
  process.exit(1);
}

console.log(`Pushing schema to prod: ${prodUrl.replace(/:[^:@]+@/, ":***@")}`);

await $`bunx prisma db push --schema packages/db/prisma/schema.prisma`
  .env({ ...process.env, DATABASE_URL: prodUrl });
