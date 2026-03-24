"use client";

import { useEffect, useState } from "react";

interface Pick {
  gameLabel: string;
  label: string;
  outcomeType?: string;
  odds: number;
  hit: boolean;
  mlInvolved?: boolean;
  laneTag?: string | null;
  meta: number | null;
  posterior: number;
}

function isPlayerPick(outcomeType: string): boolean {
  const base = (outcomeType ?? "").replace(/:.*$/, "");
  return base.startsWith("PLAYER_") || base.startsWith("HOME_TOP_") || base.startsWith("AWAY_TOP_");
}

function filterPicksByType<T extends { outcomeType?: string }>(picks: T[], pickType: "game" | "player" | "all"): T[] {
  if (pickType === "all") return picks;
  return picks.filter((p) => {
    const ot = p.outcomeType ?? "";
    return pickType === "player" ? isPlayerPick(ot) : !isPlayerPick(ot);
  });
}

function filterPicksByMl<T extends { mlInvolved?: boolean }>(
  picks: T[],
  mlFilter: "all" | "ml_only" | "no_ml",
): T[] {
  if (mlFilter === "all") return picks;
  return picks.filter((p) => (mlFilter === "ml_only" ? !!p.mlInvolved : !p.mlInvolved));
}

function inferLaneTag(outcomeType: string): string {
  const base = (outcomeType ?? "").replace(/:.*$/, "");
  if (base.includes("WIN")) return "moneyline";
  if (base.includes("COVERED")) return "spread";
  if (base.includes("OVER") || base.includes("UNDER") || base.includes("TOTAL_")) return "total";
  if (base === "PLAYER_10_PLUS_REBOUNDS" || base.includes("REBOUNDER")) return "player_rebounds";
  if (base === "PLAYER_10_PLUS_ASSISTS" || base.includes("ASSIST") || base.includes("PLAYMAKER")) return "player_assists";
  if (base === "PLAYER_30_PLUS" || base === "PLAYER_40_PLUS" || base.includes("SCORER")) return "player_points";
  if (base.startsWith("PLAYER_") || base.startsWith("HOME_TOP_") || base.startsWith("AWAY_TOP_")) return "other_prop";
  return "other";
}

function filterPicksByLane<T extends { outcomeType?: string; laneTag?: string | null }>(
  picks: T[],
  lane: "all" | "moneyline" | "spread" | "total" | "player_points" | "player_rebounds" | "player_assists" | "other_prop" | "other",
): T[] {
  if (lane === "all") return picks;
  return picks.filter((p) => (p.laneTag ?? inferLaneTag(p.outcomeType ?? "")) === lane);
}

interface Day {
  date: string;
  season: number;
  picks: Pick[];
}

interface ModelVersionOption {
  name: string;
  isActive: boolean;
  coverage?: {
    gradedDates: number;
    gradedPicks: number;
  };
}

function americanToDecimal(american: number): number {
  return american > 0 ? 1 + american / 100 : 1 + 100 / Math.abs(american);
}

function getCurrentSeason(): number {
  const d = new Date();
  return d.getMonth() >= 9 ? d.getFullYear() : d.getFullYear() - 1;
}

function flatBetPayout(odds: number, stake: number, hit: boolean): number {
  if (!hit) return -stake;
  const decimal = americanToDecimal(odds);
  return stake * (decimal - 1);
}

export default function SimulatorPage() {
  const [data, setData] = useState<{ days: Day[]; seasons: number[] } | null>(null);
  const [modelVersions, setModelVersions] = useState<ModelVersionOption[]>([]);
  const [selectedModelVersion, setSelectedModelVersion] = useState<string>("all");
  const [liveCoverage, setLiveCoverage] = useState<{ gradedDates: number; gradedPicks: number }>({
    gradedDates: 0,
    gradedPicks: 0,
  });
  const [loading, setLoading] = useState(true);
  const [stake, setStake] = useState("10");
  const [selectedSeason, setSelectedSeason] = useState<string>(() => String(getCurrentSeason()));
  const [parlaySort, setParlaySort] = useState<{ col: string; asc: boolean }>({ col: "date", asc: true });
  const [pickType, setPickType] = useState<"game" | "player" | "all">("game");
  const [mlFilter, setMlFilter] = useState<"all" | "ml_only" | "no_ml">("all");
  const [oddsMode, setOddsMode] = useState<"all" | "full" | "require" | "ignore">("all");
  const [gateMode, setGateMode] = useState<"all" | "legacy" | "strict">("all");
  const [laneFilter, setLaneFilter] = useState<"all" | "moneyline" | "spread" | "total" | "player_points" | "player_rebounds" | "player_assists" | "other_prop" | "other">("all");
  const [minParlayLegs, setMinParlayLegs] = useState("1");
  const [runningSeason, setRunningSeason] = useState(false);
  const [runSeasonMsg, setRunSeasonMsg] = useState<string>("");

  useEffect(() => {
    fetch("/api/model-versions")
      .then((r) => r.json())
      .then((json: {
        versions?: Array<{
          name: string;
          isActive: boolean;
          coverage?: { gradedDates: number; gradedPicks: number };
        }>;
        liveCoverage?: { gradedDates: number; gradedPicks: number };
      }) => {
        const versions = (json.versions ?? []).map((v) => ({
          name: v.name,
          isActive: v.isActive,
          coverage: v.coverage,
        }));
        setModelVersions(versions);
        setLiveCoverage(json.liveCoverage ?? { gradedDates: 0, gradedPicks: 0 });
        const active = versions.find((v) => v.isActive);
        const firstUsable = versions.find((v) => (v.coverage?.gradedPicks ?? 0) > 0);
        if (active && (active.coverage?.gradedPicks ?? 0) > 0) {
          setSelectedModelVersion(active.name);
        } else if (firstUsable) {
          setSelectedModelVersion(firstUsable.name);
        } else if ((json.liveCoverage?.gradedPicks ?? 0) > 0) {
          setSelectedModelVersion("live");
        }
      })
      .catch(() => setModelVersions([]));
  }, []);

  useEffect(() => {
    setLoading(true);
    const qp = new URLSearchParams();
    if (selectedModelVersion !== "all") qp.set("modelVersion", selectedModelVersion);
    if (oddsMode !== "all") qp.set("oddsMode", oddsMode);
    if (gateMode !== "all") qp.set("gateMode", gateMode);
    if (laneFilter !== "all") qp.set("lane", laneFilter);
    const qs = qp.toString() ? `?${qp.toString()}` : "";
    fetch(`/api/simulator${qs}`)
      .then((r) => r.json())
      .then((d) => {
        setData(d);
        setSelectedSeason((prev) => {
          const seasons = d?.seasons ?? [];
          const current = getCurrentSeason();
          if (seasons.includes(current)) return String(current);
          if (seasons.length > 0 && !seasons.includes(Number(prev))) return String(seasons.at(-1));
          return prev;
        });
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [selectedModelVersion, oddsMode, gateMode, laneFilter]);

  const runSeasonWithFilters = async () => {
    if (!selectedSeason || selectedSeason === "all") {
      setRunSeasonMsg("Select a specific season first.");
      return;
    }
    if (selectedModelVersion === "all") {
      setRunSeasonMsg("Select a specific model version (or live) first.");
      return;
    }
    const resolvedOddsMode = oddsMode === "all" ? "full" : oddsMode;
    setRunningSeason(true);
    setRunSeasonMsg("Running season picks. This may take a while...");
    try {
      const res = await fetch("/api/simulator/run-season", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          season: Number(selectedSeason),
          modelVersion: selectedModelVersion,
          oddsMode: resolvedOddsMode,
          strictGates: gateMode === "strict",
        }),
      });
      const json = await res.json();
      if (!res.ok) {
        setRunSeasonMsg(json?.error ?? "Failed to run season picks.");
        return;
      }
      setRunSeasonMsg(
        `Run complete: ${json.ok}/${json.dates} dates succeeded` +
          (json.failed ? ` (${json.failed} failed)` : ""),
      );
      const qp = new URLSearchParams();
      if (selectedModelVersion !== "all") qp.set("modelVersion", selectedModelVersion);
      if (oddsMode !== "all") qp.set("oddsMode", oddsMode);
      if (gateMode !== "all") qp.set("gateMode", gateMode);
      if (laneFilter !== "all") qp.set("lane", laneFilter);
      const qs = qp.toString() ? `?${qp.toString()}` : "";
      const refreshed = await fetch(`/api/simulator${qs}`).then((r) => r.json());
      setData(refreshed);
    } catch {
      setRunSeasonMsg("Failed to run season picks.");
    } finally {
      setRunningSeason(false);
    }
  };

  const stakeNum = parseFloat(stake) || 10;
  const seasons = data?.seasons ?? [];
  const currentSeason = getCurrentSeason();
  const seasonOptions = Array.from(new Set<number>([...seasons, currentSeason])).sort((a, b) => a - b);

  if (loading) {
    return (
      <div className="card" style={{ opacity: 0.8 }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
          <div className="spinner" />
          <span className="muted">Loading historical data...</span>
        </div>
      </div>
    );
  }

  const allDays = data?.days ?? [];
  const seasonFilteredDays = selectedSeason === "all"
    ? allDays
    : allDays.filter((d) => d.season === parseInt(selectedSeason));

  const filteredDays = seasonFilteredDays.map((d) => ({
    ...d,
    picks: filterPicksByLane(filterPicksByMl(filterPicksByType(d.picks, pickType), mlFilter), laneFilter),
  }));

  const allPicks = filteredDays.flatMap((d) => d.picks);
  const totalPicks = allPicks.length;
  const totalHits = allPicks.filter((p) => p.hit).length;

  // Flat bet simulation
  let flatRunning = 0;
  let flatStaked = 0;
  const flatByDay = filteredDays.map((day) => {
    let dayPnl = 0;
    for (const pick of day.picks) {
      dayPnl += flatBetPayout(pick.odds, stakeNum, pick.hit);
      flatStaked += stakeNum;
    }
    flatRunning += dayPnl;
    return { date: day.date, picks: day.picks.length, hits: day.picks.filter((p) => p.hit).length, pnl: dayPnl, cumulative: flatRunning };
  });
  const flatTotalPnl = flatRunning;
  const flatRoi = flatStaked > 0 ? (flatTotalPnl / flatStaked) * 100 : 0;

  // Parlay simulation
  let parlayRunning = 0;
  const parlayByDay = filteredDays.map((day) => {
    if (day.picks.length === 0) return { date: day.date, legs: 0, hit: false, odds: 1, pnl: 0, cumulative: parlayRunning };
    const combinedDecimal = day.picks.reduce((acc, p) => acc * americanToDecimal(p.odds), 1);
    const allHit = day.picks.every((p) => p.hit);
    const pnl = allHit ? stakeNum * (combinedDecimal - 1) : -stakeNum;
    parlayRunning += pnl;
    return { date: day.date, legs: day.picks.length, hit: allHit, odds: combinedDecimal, pnl, cumulative: parlayRunning };
  });
  const minParlayLegsNum = Math.max(1, Number.parseInt(minParlayLegs, 10) || 1);
  const parlayRows = parlayByDay.filter((d) => d.legs >= minParlayLegsNum);
  const filteredParlayWins = parlayRows.filter((d) => d.hit).length;
  const filteredParlayStaked = parlayRows.length * stakeNum;
  const filteredParlayTotalPnl = parlayRows.reduce((sum, d) => sum + d.pnl, 0);
  const filteredParlayRoi = filteredParlayStaked > 0 ? (filteredParlayTotalPnl / filteredParlayStaked) * 100 : 0;

  const sortedParlayRows = [...parlayRows].sort((a, b) => {
    const mul = parlaySort.asc ? 1 : -1;
    switch (parlaySort.col) {
      case "date":
        return mul * (a.date.localeCompare(b.date));
      case "legs":
        return mul * (a.legs - b.legs);
      case "odds":
        return mul * (a.odds - b.odds);
      case "result":
        return mul * (a.pnl - b.pnl);
      default:
        return 0;
    }
  });

  const sortParlay = (col: string) => {
    setParlaySort((prev) => ({ col, asc: prev.col === col ? !prev.asc : true }));
  };

  const SortIcon = ({ col }: { col: string }) =>
    parlaySort.col === col ? (parlaySort.asc ? " ▲" : " ▼") : "";

  return (
    <>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "1rem", flexWrap: "wrap", gap: "0.5rem" }}>
        <h1 style={{ margin: 0, fontSize: "1.4rem", fontWeight: 800 }}>Bet Simulator</h1>
        <div style={{ display: "flex", alignItems: "center", gap: "0.65rem", flexWrap: "wrap", justifyContent: "flex-end" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
            <label className="muted" style={{ fontSize: "0.85rem" }}>Model (season coverage)</label>
            <select
              value={selectedModelVersion}
              onChange={(e) => setSelectedModelVersion(e.target.value)}
              style={{ padding: "0.35rem 0.45rem", fontSize: "0.85rem" }}
            >
              <option value="all">all canonical runs</option>
              <option value="live">
                live (no snapshot) [season: {liveCoverage.gradedDates}d/{liveCoverage.gradedPicks}p]
              </option>
              {modelVersions.map((v) => (
                <option
                  key={v.name}
                  value={v.name}
                  disabled={(v.coverage?.gradedPicks ?? 0) === 0}
                >
                  {v.name}
                  {v.isActive ? " (active)" : ""}
                  {` [season: ${v.coverage?.gradedDates ?? 0}d/${v.coverage?.gradedPicks ?? 0}p]`}
                </option>
              ))}
            </select>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
            <label className="muted" style={{ fontSize: "0.85rem" }}>Season</label>
            <select
              value={selectedSeason}
              onChange={(e) => setSelectedSeason(e.target.value)}
              style={{ padding: "0.35rem 0.45rem", fontSize: "0.85rem" }}
            >
              <option value="all">All Seasons</option>
              {seasonOptions.map((s) => (
                <option key={s} value={String(s)}>{s}-{String(s + 1).slice(2)}</option>
              ))}
            </select>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
            <label className="muted" style={{ fontSize: "0.85rem" }}>Stake $</label>
            <input
              type="number"
              value={stake}
              onChange={(e) => setStake(e.target.value)}
              style={{ width: 72, padding: "0.35rem 0.45rem", fontSize: "0.85rem" }}
              min="1"
              step="5"
            />
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
            <label className="muted" style={{ fontSize: "0.85rem" }}>Min legs</label>
            <input
              type="number"
              value={minParlayLegs}
              onChange={(e) => setMinParlayLegs(e.target.value)}
              style={{ width: 72, padding: "0.35rem 0.45rem", fontSize: "0.85rem" }}
              min="1"
              step="1"
            />
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.35rem" }}>
            <label className="muted" style={{ fontSize: "0.85rem" }}>Picks</label>
            <select
              value={pickType}
              onChange={(e) => setPickType(e.target.value as "game" | "player" | "all")}
              style={{ padding: "0.35rem 0.45rem", fontSize: "0.85rem" }}
            >
              <option value="game">Game</option>
              <option value="player">Player</option>
              <option value="all">All</option>
            </select>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.35rem" }}>
            <label className="muted" style={{ fontSize: "0.85rem" }}>ML vote</label>
            <select
              value={mlFilter}
              onChange={(e) => setMlFilter(e.target.value as "all" | "ml_only" | "no_ml")}
              style={{ padding: "0.35rem 0.45rem", fontSize: "0.85rem" }}
            >
              <option value="all">All</option>
              <option value="ml_only">ML only</option>
              <option value="no_ml">No ML</option>
            </select>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.35rem" }}>
            <label className="muted" style={{ fontSize: "0.85rem" }}>Odds mode</label>
            <select
              value={oddsMode}
              onChange={(e) => setOddsMode(e.target.value as "all" | "full" | "require" | "ignore")}
              style={{ padding: "0.35rem 0.45rem", fontSize: "0.85rem" }}
            >
              <option value="all">All runs</option>
              <option value="full">full</option>
              <option value="require">require</option>
              <option value="ignore">ignore</option>
            </select>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.35rem" }}>
            <label className="muted" style={{ fontSize: "0.85rem" }}>Gates</label>
            <select
              value={gateMode}
              onChange={(e) => setGateMode(e.target.value as "all" | "legacy" | "strict")}
              style={{ padding: "0.35rem 0.45rem", fontSize: "0.85rem" }}
            >
              <option value="all">All runs</option>
              <option value="legacy">legacy</option>
              <option value="strict">strict</option>
            </select>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.35rem" }}>
            <label className="muted" style={{ fontSize: "0.85rem" }}>Lane</label>
            <select
              value={laneFilter}
              onChange={(e) => setLaneFilter(e.target.value as typeof laneFilter)}
              style={{ padding: "0.35rem 0.45rem", fontSize: "0.85rem" }}
            >
              <option value="all">all lanes</option>
              <option value="moneyline">moneyline</option>
              <option value="spread">spread</option>
              <option value="total">total</option>
              <option value="player_points">player_points</option>
              <option value="player_rebounds">player_rebounds</option>
              <option value="player_assists">player_assists</option>
              <option value="other_prop">other_prop</option>
              <option value="other">other</option>
            </select>
          </div>
          <button
            type="button"
            onClick={runSeasonWithFilters}
            disabled={runningSeason || selectedSeason === "all" || selectedModelVersion === "all"}
            style={{
              padding: "0.4rem 0.65rem",
              fontSize: "0.85rem",
              border: "1px solid var(--border)",
              borderRadius: 4,
              background: "var(--accent-muted)",
              cursor:
                runningSeason || selectedSeason === "all" || selectedModelVersion === "all"
                  ? "not-allowed"
                  : "pointer",
              opacity:
                runningSeason || selectedSeason === "all" || selectedModelVersion === "all" ? 0.6 : 1,
            }}
          >
            {runningSeason ? "Running..." : "Run season picks"}
          </button>
          {mlFilter !== "all" && pickType === "game" && (
            <span className="muted" style={{ fontSize: "0.75rem" }}>
              ML vote applies to player-point picks; use Player or All.
            </span>
          )}
        </div>
      </div>

      <div className="muted" style={{ marginBottom: "1rem", fontSize: "0.85rem" }}>
        {filteredDays.length} days, {totalPicks} graded picks, {totalHits} hits ({totalPicks > 0 ? (totalHits / totalPicks * 100).toFixed(1) : "0.0"}%)
        {selectedSeason !== "all" && <span style={{ marginLeft: "0.5rem", opacity: 0.7 }}>(out of {allDays.length} total days)</span>}
        {pickType !== "all" && <span style={{ marginLeft: "0.5rem", opacity: 0.7 }}>• {pickType} picks only</span>}
        {mlFilter !== "all" && <span style={{ marginLeft: "0.5rem", opacity: 0.7 }}>• {mlFilter === "ml_only" ? "ML vote involved" : "ML vote not involved"}</span>}
        {selectedModelVersion !== "all" && <span style={{ marginLeft: "0.5rem", opacity: 0.7 }}>• model {selectedModelVersion}</span>}
        {oddsMode !== "all" && <span style={{ marginLeft: "0.5rem", opacity: 0.7 }}>• oddsMode {oddsMode}</span>}
        {gateMode !== "all" && <span style={{ marginLeft: "0.5rem", opacity: 0.7 }}>• gates {gateMode}</span>}
        {laneFilter !== "all" && <span style={{ marginLeft: "0.5rem", opacity: 0.7 }}>• lane {laneFilter}</span>}
        <span style={{ marginLeft: "0.5rem", opacity: 0.7 }}>• canonical latest run</span>
      </div>
      {runSeasonMsg && (
        <div className="muted" style={{ marginBottom: "0.8rem", fontSize: "0.85rem" }}>
          {runSeasonMsg}
        </div>
      )}
      {allDays.length === 0 && (
        <div className="card" style={{ marginBottom: "0.8rem", padding: "0.75rem" }}>
          <p style={{ marginBottom: "0.35rem" }}>
            No graded pick data found for model `{selectedModelVersion}` with current filters.
          </p>
          <p className="muted" style={{ margin: 0, fontSize: "0.85rem" }}>
            You can still run season picks above; results will populate here after completion.
          </p>
        </div>
      )}

      {/* Summary cards */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem", marginBottom: "1.5rem" }}>
        {/* Flat bet summary */}
        <div className="card" style={{ padding: "1rem" }}>
          <h3 style={{ margin: "0 0 0.75rem", fontSize: "1rem" }}>Flat Bet (each pick)</h3>
          <div style={{ display: "flex", flexDirection: "column", gap: "0.3rem", fontSize: "0.9rem" }}>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span className="muted">Total staked</span>
              <span>${flatStaked.toFixed(2)}</span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span className="muted">Total P&L</span>
              <span style={{ fontWeight: 800, fontSize: "1.1rem", color: flatTotalPnl >= 0 ? "var(--success)" : "var(--error)" }}>
                {flatTotalPnl >= 0 ? "+" : ""}${flatTotalPnl.toFixed(2)}
              </span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span className="muted">ROI</span>
              <span style={{ fontWeight: 700, color: flatRoi >= 0 ? "var(--success)" : "var(--error)" }}>
                {flatRoi >= 0 ? "+" : ""}{flatRoi.toFixed(1)}%
              </span>
            </div>
          </div>
        </div>

        {/* Parlay summary */}
        <div className="card" style={{ padding: "1rem" }}>
          <h3 style={{ margin: "0 0 0.75rem", fontSize: "1rem" }}>
            Daily Parlay (all picks, min legs {minParlayLegsNum})
          </h3>
          <div style={{ display: "flex", flexDirection: "column", gap: "0.3rem", fontSize: "0.9rem" }}>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span className="muted">Days bet</span>
              <span>{parlayRows.length} ({filteredParlayWins} won)</span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span className="muted">Total staked</span>
              <span>${filteredParlayStaked.toFixed(2)}</span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span className="muted">Total P&L</span>
              <span style={{ fontWeight: 800, fontSize: "1.1rem", color: filteredParlayTotalPnl >= 0 ? "var(--success)" : "var(--error)" }}>
                {filteredParlayTotalPnl >= 0 ? "+" : ""}${filteredParlayTotalPnl.toFixed(2)}
              </span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span className="muted">ROI</span>
              <span style={{ fontWeight: 700, color: filteredParlayRoi >= 0 ? "var(--success)" : "var(--error)" }}>
                {filteredParlayRoi >= 0 ? "+" : ""}{filteredParlayRoi.toFixed(1)}%
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Day-by-day table */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem" }}>
        {/* Flat bet daily */}
        <div>
          <h3 style={{ margin: "0 0 0.5rem" }}>Flat Bet Daily</h3>
          <div style={{ maxHeight: 500, overflowY: "auto" }}>
            <table>
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Picks</th>
                  <th>Hits</th>
                  <th>P&L</th>
                  <th>Cumulative</th>
                </tr>
              </thead>
              <tbody>
                {flatByDay.map((d) => (
                  <tr key={d.date}>
                    <td style={{ fontSize: "0.82rem" }}>{d.date}</td>
                    <td>{d.picks}</td>
                    <td>{d.hits}/{d.picks}</td>
                    <td style={{ color: d.pnl >= 0 ? "var(--success)" : "var(--error)", fontWeight: 600 }}>
                      {d.pnl >= 0 ? "+" : ""}{d.pnl.toFixed(2)}
                    </td>
                    <td style={{ color: d.cumulative >= 0 ? "var(--success)" : "var(--error)", fontWeight: 600 }}>
                      {d.cumulative >= 0 ? "+" : ""}{d.cumulative.toFixed(2)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Parlay daily */}
        <div>
          <h3 style={{ margin: "0 0 0.5rem" }}>Daily Parlay</h3>
          <div style={{ maxHeight: 500, overflowY: "auto" }}>
            <table>
              <thead>
                <tr>
                  <th style={{ cursor: "pointer", userSelect: "none" }} onClick={() => sortParlay("date")}>Date<SortIcon col="date" /></th>
                  <th style={{ cursor: "pointer", userSelect: "none" }} onClick={() => sortParlay("legs")}>Legs<SortIcon col="legs" /></th>
                  <th style={{ cursor: "pointer", userSelect: "none" }} onClick={() => sortParlay("odds")}>Odds<SortIcon col="odds" /></th>
                  <th style={{ cursor: "pointer", userSelect: "none" }} onClick={() => sortParlay("result")}>Result<SortIcon col="result" /></th>
                </tr>
              </thead>
              <tbody>
                {sortedParlayRows.map((d) => (
                  <tr key={d.date}>
                    <td style={{ fontSize: "0.82rem" }}>{d.date}</td>
                    <td>{d.legs}</td>
                    <td>{d.odds.toFixed(2)}x</td>
                    <td>
                      {d.hit ? (
                        <span style={{ color: "var(--success)", fontWeight: 700 }}>+${d.pnl.toFixed(2)}</span>
                      ) : (
                        <span style={{ color: "var(--error)" }}>-${stakeNum.toFixed(2)}</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </>
  );
}
