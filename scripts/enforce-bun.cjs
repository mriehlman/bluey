const ua = process.env.npm_config_user_agent || "";
const execPath = (process.env.npm_execpath || "").toLowerCase();

const usingBun = ua.includes("bun/") || execPath.includes("bun");

if (!usingBun) {
  console.error("");
  console.error("This workspace is Bun-only.");
  console.error("Use `bun install` instead of npm/yarn/pnpm.");
  console.error("");
  process.exit(1);
}
