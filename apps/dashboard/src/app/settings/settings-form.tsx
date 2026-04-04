"use client";

import { useEffect, useState } from "react";

type ConfigPayload = {
  favoriteTeamCode: string | null;
  defaultStake: number | null;
  timezone: string | null;
  notes: string | null;
  updatedAt: string | null;
};

export default function SettingsForm() {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const [config, setConfig] = useState({
    favoriteTeamCode: "",
    defaultStake: "",
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "America/New_York",
    notes: "",
  });
  const [updatedAt, setUpdatedAt] = useState<string | null>(null);

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const res = await fetch("/api/user-config");
        if (!res.ok) {
          throw new Error(`Failed to load settings (${res.status})`);
        }
        const json = (await res.json()) as { config: ConfigPayload };
        if (!mounted) return;

        setConfig({
          favoriteTeamCode: json.config.favoriteTeamCode ?? "",
          defaultStake:
            json.config.defaultStake != null
              ? String(json.config.defaultStake)
              : "",
          timezone:
            json.config.timezone ??
            Intl.DateTimeFormat().resolvedOptions().timeZone ??
            "America/New_York",
          notes: json.config.notes ?? "",
        });
        setUpdatedAt(json.config.updatedAt);
      } catch (err) {
        if (mounted) {
          setStatus(err instanceof Error ? err.message : "Failed to load settings");
        }
      } finally {
        if (mounted) {
          setLoading(false);
        }
      }
    })();

    return () => {
      mounted = false;
    };
  }, []);

  async function onSave(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setSaving(true);
    setStatus(null);

    const parsedStake =
      config.defaultStake.trim() === "" ? null : Number(config.defaultStake);
    if (parsedStake != null && !Number.isFinite(parsedStake)) {
      setStatus("Default stake must be a valid number.");
      setSaving(false);
      return;
    }

    try {
      const res = await fetch("/api/user-config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          favoriteTeamCode: config.favoriteTeamCode,
          defaultStake: parsedStake,
          timezone: config.timezone,
          notes: config.notes,
        }),
      });
      const json = (await res.json()) as {
        ok?: boolean;
        message?: string;
        config?: ConfigPayload;
      };
      if (!res.ok || !json.ok) {
        throw new Error(json.message ?? `Failed to save settings (${res.status})`);
      }

      setUpdatedAt(json.config?.updatedAt ?? null);
      setStatus("Settings saved.");
    } catch (err) {
      setStatus(err instanceof Error ? err.message : "Failed to save settings");
    } finally {
      setSaving(false);
    }
  }

  if (loading) {
    return <p className="muted">Loading your settings...</p>;
  }

  return (
    <form onSubmit={onSave} style={{ display: "grid", gap: "0.75rem", maxWidth: 560 }}>
      <label>
        Favorite team code
        <input
          type="text"
          placeholder="LAL"
          value={config.favoriteTeamCode}
          onChange={(e) =>
            setConfig((prev) => ({
              ...prev,
              favoriteTeamCode: e.target.value.toUpperCase(),
            }))
          }
        />
      </label>

      <label>
        Default stake
        <input
          type="number"
          step="0.01"
          min="0"
          placeholder="10"
          value={config.defaultStake}
          onChange={(e) =>
            setConfig((prev) => ({
              ...prev,
              defaultStake: e.target.value,
            }))
          }
        />
      </label>

      <label>
        Timezone
        <input
          type="text"
          placeholder="America/New_York"
          value={config.timezone}
          onChange={(e) =>
            setConfig((prev) => ({
              ...prev,
              timezone: e.target.value,
            }))
          }
        />
      </label>

      <label>
        Notes
        <textarea
          rows={4}
          placeholder="Saved user context..."
          value={config.notes}
          onChange={(e) =>
            setConfig((prev) => ({
              ...prev,
              notes: e.target.value,
            }))
          }
        />
      </label>

      <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
        <button type="submit" disabled={saving}>
          {saving ? "Saving..." : "Save settings"}
        </button>
        {updatedAt && (
          <span className="muted" style={{ fontSize: "0.82rem" }}>
            Last saved: {new Date(updatedAt).toLocaleString()}
          </span>
        )}
      </div>

      {status && (
        <p
          style={{
            margin: 0,
            color: status === "Settings saved." ? "var(--success)" : "var(--error)",
          }}
        >
          {status}
        </p>
      )}
    </form>
  );
}
