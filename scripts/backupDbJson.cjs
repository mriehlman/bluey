const fs = require("fs");
const path = require("path");
const { PrismaClient } = require("@prisma/client");

function loadEnvFile(root) {
  const envPath = path.join(root, ".env");
  if (!fs.existsSync(envPath)) return;
  for (const line of fs.readFileSync(envPath, "utf8").split(/\r?\n/)) {
    if (!line || line.trim().startsWith("#") || !line.includes("=")) continue;
    const i = line.indexOf("=");
    const key = line.slice(0, i).trim();
    let val = line.slice(i + 1).trim();
    if (val.startsWith("\"") && val.endsWith("\"")) val = val.slice(1, -1);
    if (!process.env[key]) process.env[key] = val;
  }
}

function nowStamp() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}-${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

async function backupTable(prisma, table, outPath, batchSize = 5000) {
  const countRows = await prisma.$queryRawUnsafe(`SELECT COUNT(*)::bigint AS count FROM "${table}"`);
  const total = Number(countRows[0]?.count ?? 0);

  const ws = fs.createWriteStream(outPath, { encoding: "utf8" });
  ws.write("[\n");

  let written = 0;
  let offset = 0;
  while (offset < total) {
    const rows = await prisma.$queryRawUnsafe(
      `SELECT row_to_json(t) AS row FROM (SELECT * FROM "${table}" OFFSET ${offset} LIMIT ${batchSize}) t`,
    );
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i]?.row ?? null;
      const prefix = written === 0 ? "  " : ",\n  ";
      ws.write(prefix + JSON.stringify(row));
      written++;
    }
    offset += rows.length;
    if (rows.length === 0) break;
  }

  ws.write("\n]\n");
  await new Promise((resolve) => ws.end(resolve));
  return total;
}

async function main() {
  const root = process.cwd();
  loadEnvFile(root);

  const backupRoot = path.join(root, "backups");
  fs.mkdirSync(backupRoot, { recursive: true });

  const runDir = path.join(backupRoot, `db-${nowStamp()}`);
  fs.mkdirSync(runDir, { recursive: true });

  const prisma = new PrismaClient();
  try {
    const tables = await prisma.$queryRawUnsafe(
      "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name",
    );

    const manifest = {
      createdAt: new Date().toISOString(),
      databaseUrlHost: (() => {
        try {
          return new URL(process.env.DATABASE_URL).host;
        } catch {
          return "unknown";
        }
      })(),
      tableCount: tables.length,
      tables: [],
    };

    for (const t of tables) {
      const name = t.table_name;
      const file = `${name}.json`;
      const outPath = path.join(runDir, file);
      const rows = await backupTable(prisma, name, outPath);
      manifest.tables.push({ table: name, rows, file });
      console.log(`${name}: ${rows} rows`);
    }

    fs.writeFileSync(path.join(runDir, "_manifest.json"), JSON.stringify(manifest, null, 2));
    console.log(`\nBackup complete: ${runDir}`);
  } finally {
    await prisma.$disconnect();
  }
}

main().catch((err) => {
  console.error("Backup failed:", err);
  process.exit(1);
});
