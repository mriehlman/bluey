// Copies Prisma query-engine binaries from the generated output into
// apps/dashboard/.prisma/client/ so Vercel's file tracer includes them
// and Prisma can locate them at runtime.

const path = require("path");
const fs = require("fs");

const src = path.join(__dirname, "..", "packages", "db", "src", "generated", "prisma");
const dst = path.join(__dirname, "..", "apps", "dashboard", ".prisma", "client");

if (!fs.existsSync(src)) {
  console.error("[copy-prisma-engines] generated prisma dir not found:", src);
  process.exit(1);
}

fs.mkdirSync(dst, { recursive: true });

let copied = 0;
for (const file of fs.readdirSync(src)) {
  if (file.endsWith(".node") || file === "schema.prisma") {
    fs.copyFileSync(path.join(src, file), path.join(dst, file));
    copied++;
  }
}

console.log(`[copy-prisma-engines] copied ${copied} files to ${dst}`);
