"use client";

import { useState } from "react";

export default function LiveExplainButton({ date }: { date: string }) {
  const [result, setResult] = useState<Record<string, unknown> | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function explain() {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch("/api/events/explain", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ date }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error ?? "Request failed");
      setResult(data);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      <button onClick={explain} disabled={loading}>
        {loading ? "Loading…" : "Fetch Stored Events"}
      </button>
      {error && <p style={{ color: "var(--error)" }}>{error}</p>}
      {result && (
        <pre style={{ marginTop: "0.5rem" }}>
          {JSON.stringify(result, null, 2)}
        </pre>
      )}
    </div>
  );
}
