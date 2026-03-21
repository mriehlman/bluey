import { prisma } from "@bluey/db";
import { fmtDate, jsonPretty, parseDateParam } from "@/lib/format";
import LiveExplainButton from "./LiveExplainButton";

export const dynamic = "force-dynamic";

interface Props {
  params: Promise<{ date: string }>;
}

export default async function EventDatePage({ params }: Props) {
  const { date: dateStr } = await params;
  let dateVal: Date;
  try {
    dateVal = parseDateParam(dateStr);
  } catch {
    return <p>Invalid date format. Use YYYY-MM-DD.</p>;
  }

  const night = await (prisma as any).night?.findUnique({ where: { date: dateVal } }).catch(() => null) ?? null;

  const events = await (prisma as any).nightEvent?.findMany({
    where: { date: dateVal },
    orderBy: { eventKey: "asc" },
  }).catch(() => []) ?? [];

  const prevDate = new Date(dateVal);
  prevDate.setUTCDate(prevDate.getUTCDate() - 1);
  const nextDate = new Date(dateVal);
  nextDate.setUTCDate(nextDate.getUTCDate() + 1);

  return (
    <>
      <h1>Events &mdash; {dateStr}</h1>

      <div style={{ display: "flex", gap: "1rem", marginBottom: "1rem" }}>
        <a href={`/events/${fmtDate(prevDate)}`}>&larr; {fmtDate(prevDate)}</a>
        <a href={`/events/${fmtDate(nextDate)}`}>{fmtDate(nextDate)} &rarr;</a>
      </div>

      <div className="card">
        <h2>Night Summary</h2>
        {night ? (
          <table>
            <tbody>
              <tr><td><strong>Games</strong></td><td>{night.gameCount}</td></tr>
              <tr><td><strong>Stats</strong></td><td>{night.statCount}</td></tr>
              <tr><td><strong>Event Hits</strong></td><td>{night.eventHitCount}</td></tr>
              <tr><td><strong>Logic Version</strong></td><td className="mono">{night.eventLogicVersion ?? "—"}</td></tr>
              <tr><td><strong>Processed At</strong></td><td className="mono">{night.processedAt.toISOString()}</td></tr>
            </tbody>
          </table>
        ) : (
          <p className="muted">No Night record for this date.</p>
        )}
      </div>

      <div className="card">
        <h2>Stored Events ({events.length})</h2>
        {events.length > 0 ? (
          <table>
            <thead>
              <tr>
                <th>Event Key</th>
                <th>Season</th>
                <th>Value</th>
                <th>Meta</th>
              </tr>
            </thead>
            <tbody>
              {events.map((ev: any) => (
                <tr key={ev.id}>
                  <td className="mono">{ev.eventKey}</td>
                  <td>{ev.season}</td>
                  <td>
                    <span className={`badge ${ev.value ? "badge-green" : "badge-red"}`}>
                      {ev.value ? "true" : "false"}
                    </span>
                  </td>
                  <td className="mono" style={{ whiteSpace: "pre-wrap", maxWidth: 400 }}>
                    {ev.meta ? jsonPretty(ev.meta) : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p className="muted">No events stored for this date.</p>
        )}
      </div>

      <div className="card">
        <h2>Live Explain</h2>
        <LiveExplainButton date={dateStr} />
      </div>
    </>
  );
}
