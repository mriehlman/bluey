import type { NextConfig } from "next";
import path from "path";

const nextConfig: NextConfig = {
  transpilePackages: ["@bluey/core", "@bluey/db"],
  outputFileTracingRoot: path.join(__dirname, "../../"),
  outputFileTracingIncludes: {
    "/*": ["./.prisma/client/*.node", "./.prisma/client/schema.prisma"],
  },
  webpack: (config, { dev }) => {
    if (dev) {
      config.cache = { type: "filesystem" as const };
    }
    return config;
  },
};

export default nextConfig;
