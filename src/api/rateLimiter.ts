/**
 * Sliding-window rate limiter.
 * Tracks request timestamps and blocks when the window limit is reached.
 */
export class RateLimiter {
  private readonly windowMs: number;
  private readonly maxRequests: number;
  private timestamps: number[] = [];

  constructor(requestsPerMinute: number) {
    this.windowMs = 60_000;
    this.maxRequests = requestsPerMinute;
  }

  async acquire(): Promise<void> {
    while (true) {
      const now = Date.now();
      this.timestamps = this.timestamps.filter((t) => now - t < this.windowMs);

      if (this.timestamps.length < this.maxRequests) {
        this.timestamps.push(now);
        return;
      }

      const oldestInWindow = this.timestamps[0];
      const waitMs = this.windowMs - (now - oldestInWindow) + 50;
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }
}
