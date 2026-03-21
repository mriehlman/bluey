"use client";

import { useEffect, useState } from "react";

interface Pick {
  gameLabel: string;
  label: string;
  outcomeType?: string;
  odds: number;
  hit: boolean;
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

interface Day {
  date: string;
  season: number;
  picks: Pick[];
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
  const [loading, setLoading] = useState(true);
  const [stake, setStake] = useState("10");
  const [selectedSeason, setSelectedSeason] = useState<string>(() => String(getCurrentSeason()));
  const [parlaySort, setParlaySort] = useState<{ col: string; asc: boolean }>({ col: "date", asc: true });
  const [pickType, setPickType] = useState<"game" | "player" | "all">("game");

  useEffect(() => {
    fetch("/api/simulator")
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
  }, []);

  const stakeNum = parseFloat(stake) || 10;
  const seasons = data?.seasons ?? [];

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

  if (!data || data.days.length === 0) {
    return <div className="card"><p>No graded pick data found.</p></div>;
  }

  const seasonFilteredDays = selectedSeason === "all"
    ? data.days
    : data.days.filter((d) => d.season === parseInt(selectedSeason));

  const filteredDays = seasonFilteredDays.map((d) => ({
    ...d,
    picks: filterPicksByType(d.picks, pickType),
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
  let parlayStaked = 0;
  let parlayWins = 0;
  const parlayByDay = filteredDays.map((day) => {
    if (day.picks.length === 0) return { date: day.date, legs: 0, hit: false, odds: 1, pnl: 0, cumulative: parlayRunning };
    const combinedDecimal = day.picks.reduce((acc, p) => acc * americanToDecimal(p.odds), 1);
    const allHit = day.picks.every((p) => p.hit);
    parlayStaked += stakeNum;
    const pnl = allHit ? stakeNum * (combinedDecimal - 1) : -stakeNum;
    if (allHit) parlayWins++;
    parlayRunning += pnl;
    return { date: day.date, legs: day.picks.length, hit: allHit, odds: combinedDecimal, pnl, cumulative: parlayRunning };
  });
  const parlayTotalPnl = parlayRunning;
  const parlayRoi = parlayStaked > 0 ? (parlayTotalPnl / parlayStaked) * 100 : 0;

  const parlayRows = parlayByDay.filter((d) => d.legs > 0);
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
        <div style={{ display: "flex", alignItems: "center", gap: "1rem" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
            <label className="muted" style={{ fontSize: "0.85rem" }}>Season</label>
            <select
              value={selectedSeason}
              onChange={(e) => setSelectedSeason(e.target.value)}
              style={{ padding: "0.4rem 0.5rem", fontSize: "0.9rem" }}
            >
              <option value="all">All Seasons</option>
              {seasons.map((s) => (
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
              style={{ width: 80, padding: "0.4rem 0.5rem", fontSize: "0.9rem" }}
              min="1"
              step="5"
            />
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.3rem" }}>
            <span className="muted" style={{ fontSize: "0.85rem" }}>Picks:</span>
            {(["game", "player", "all"] as const).map((t) => (
              <button
                key={t}
                type="button"
                onClick={() => setPickType(t)}
                style={{
                  padding: "0.35rem 0.5rem",
                  fontSize: "0.85rem",
                  border: "1px solid var(--border)",
                  borderRadius: 4,
                  background: pickType === t ? "var(--accent-muted)" : "transparent",
                  cursor: "pointer",
                }}
              >
                {t === "game" ? "Game" : t === "player" ? "Player" : "All"}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="muted" style={{ marginBottom: "1rem", fontSize: "0.85rem" }}>
        {filteredDays.length} days, {totalPicks} graded picks, {totalHits} hits ({totalPicks > 0 ? (totalHits / totalPicks * 100).toFixed(1) : "0.0"}%)
        {selectedSeason !== "all" && <span style={{ marginLeft: "0.5rem", opacity: 0.7 }}>(out of {data.days.length} total days)</span>}
        {pickType !== "all" && <span style={{ marginLeft: "0.5rem", opacity: 0.7 }}>• {pickType} picks only</span>}
      </div>

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
          <h3 style={{ margin: "0 0 0.75rem", fontSize: "1rem" }}>Daily Parlay (all picks)</h3>
          <div style={{ display: "flex", flexDirection: "column", gap: "0.3rem", fontSize: "0.9rem" }}>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span className="muted">Days bet</span>
              <span>{parlayByDay.filter((d) => d.legs > 0).length} ({parlayWins} won)</span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span className="muted">Total staked</span>
              <span>${parlayStaked.toFixed(2)}</span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span className="muted">Total P&L</span>
              <span style={{ fontWeight: 800, fontSize: "1.1rem", color: parlayTotalPnl >= 0 ? "var(--success)" : "var(--error)" }}>
                {parlayTotalPnl >= 0 ? "+" : ""}${parlayTotalPnl.toFixed(2)}
              </span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span className="muted">ROI</span>
              <span style={{ fontWeight: 700, color: parlayRoi >= 0 ? "var(--success)" : "var(--error)" }}>
                {parlayRoi >= 0 ? "+" : ""}{parlayRoi.toFixed(1)}%
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
