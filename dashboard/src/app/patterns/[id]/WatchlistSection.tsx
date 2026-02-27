"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

interface Props {
  patternId: string;
  watchlist: { enabled: boolean; notes: string | null } | null;
}

export default function WatchlistSection({ patternId, watchlist }: Props) {
  const router = useRouter();
  const [loading, setLoading] = useState(false);
  const [notes, setNotes] = useState(watchlist?.notes ?? "");

  async function post(url: string, body: Record<string, unknown>) {
    setLoading(true);
    try {
      await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      router.refresh();
    } finally {
      setLoading(false);
    }
  }

  if (!watchlist) {
    return (
      <form
        onSubmit={(e) => {
          e.preventDefault();
          post("/api/watchlist/add", { patternId, notes: notes || undefined });
        }}
      >
        <textarea
          value={notes}
          onChange={(e) => setNotes(e.target.value)}
          placeholder="Notes (optional)…"
          rows={2}
          style={{ width: 300 }}
        />
        <button type="submit" disabled={loading}>
          {loading ? "Adding…" : "Add to Watchlist"}
        </button>
      </form>
    );
  }

  return (
    <div>
      <p>
        Status:{" "}
        <span className={`badge ${watchlist.enabled ? "badge-green" : "badge-red"}`}>
          {watchlist.enabled ? "Enabled" : "Disabled"}
        </span>
      </p>

      <form
        onSubmit={(e) => {
          e.preventDefault();
          post("/api/watchlist/update", { patternId, notes, enabled: watchlist.enabled });
        }}
        style={{ marginBottom: "0.5rem" }}
      >
        <textarea
          value={notes}
          onChange={(e) => setNotes(e.target.value)}
          placeholder="Notes…"
          rows={2}
          style={{ width: 300 }}
        />
        <button type="submit" disabled={loading}>
          Save Notes
        </button>
      </form>

      <div style={{ display: "flex", gap: "0.5rem" }}>
        <button
          disabled={loading}
          onClick={() =>
            post("/api/watchlist/update", {
              patternId,
              enabled: !watchlist.enabled,
            })
          }
        >
          {watchlist.enabled ? "Disable" : "Enable"}
        </button>
        <button
          className="danger"
          disabled={loading}
          onClick={() => post("/api/watchlist/remove", { patternId })}
        >
          Remove
        </button>
      </div>
    </div>
  );
}
