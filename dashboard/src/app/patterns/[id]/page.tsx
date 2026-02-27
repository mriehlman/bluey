import { prisma } from "@/lib/prisma";
import { fmtDate, fmtFloat, jsonPretty } from "@/lib/format";
import { notFound } from "next/navigation";
import WatchlistSection from "./WatchlistSection";

export const dynamic = "force-dynamic";

interface Props {
  params: Promise<{ id: string }>;
}

export default async function PatternDetailPage({ params }: Props) {
  const { id } = await params;

  const pattern = await prisma.pattern.findUnique({
    where: { id },
    include: { watchlist: true },
  });
  if (!pattern) return notFound();

  const hits = await prisma.patternHit.findMany({
    where: { patternId: id },
    orderBy: { date: "desc" },
    take: 50,
  });

  return (
    <>
      <h1 className="mono">{pattern.patternKey}</h1>

      <div className="card">
        <h2>Scores</h2>
        <table>
          <thead>
            <tr>
              <th>Overall</th>
              <th>Stability</th>
              <th>Balance</th>
              <th>Rarity</th>
              <th>Recency</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><strong>{fmtFloat(pattern.overallScore)}</strong></td>
              <td>{fmtFloat(pattern.stabilityScore)}</td>
              <td>{fmtFloat(pattern.balanceScore)}</td>
              <td>{fmtFloat(pattern.rarityScore)}</td>
              <td>{fmtFloat(pattern.recencyScore)}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div className="card">
        <h2>Metrics</h2>
        <table>
          <tbody>
            <tr><td><strong>Legs</strong></td><td>{pattern.legs}</td></tr>
            <tr><td><strong>Occurrences</strong></td><td>{pattern.occurrences}</td></tr>
            <tr><td><strong>Seasons</strong></td><td>{pattern.seasons}</td></tr>
            <tr><td><strong>Last Hit</strong></td><td className="mono">{fmtDate(pattern.lastHitDate)}</td></tr>
            <tr><td><strong>Longest Gap (days)</strong></td><td>{pattern.longestGapDays ?? "—"}</td></tr>
            <tr><td><strong>Event Keys</strong></td><td className="mono">{pattern.eventKeys.join(", ")}</td></tr>
          </tbody>
        </table>
      </div>

      <div className="card">
        <h2>Per-Season Breakdown</h2>
        <pre>{jsonPretty(pattern.perSeason)}</pre>
      </div>

      <div className="card">
        <h2>Watchlist</h2>
        <WatchlistSection
          patternId={pattern.id}
          watchlist={
            pattern.watchlist
              ? {
                  enabled: pattern.watchlist.enabled,
                  notes: pattern.watchlist.notes,
                }
              : null
          }
        />
      </div>

      <div className="card">
        <h2>Recent Hits ({hits.length})</h2>
        {hits.length > 0 ? (
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Season</th>
                <th>Meta</th>
              </tr>
            </thead>
            <tbody>
              {hits.map((h) => (
                <tr key={h.id}>
                  <td className="mono">
                    <a href={`/events/${fmtDate(h.date)}`}>{fmtDate(h.date)}</a>
                  </td>
                  <td>{h.season}</td>
                  <td className="mono" style={{ whiteSpace: "pre-wrap", maxWidth: 400 }}>
                    {h.meta ? jsonPretty(h.meta) : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p className="muted">No hits recorded.</p>
        )}
      </div>
    </>
  );
}
