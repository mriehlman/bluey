import type { NextConfig } from "next";
import path from "path";

const nextConfig: NextConfig = {
  transpilePackages: ["@bluey/core", "@bluey/db"],
  outputFileTracingRoot: path.join(__dirname, "../../"),
  serverExternalPackages: ["@prisma/client"],
};

export default nextConfig;
