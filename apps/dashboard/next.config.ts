import type { NextConfig } from "next";
import path from "path";

const nextConfig: NextConfig = {
  transpilePackages: ["@bluey/core", "@bluey/db"],
  outputFileTracingRoot: path.join(__dirname, "../../"),
  webpack: (config, { dev }) => {
    if (dev) {
      // Avoid intermittent missing-module/missing-chunk errors in dev on Windows.
      config.cache = false;
    }
    return config;
  },
};

export default nextConfig;
