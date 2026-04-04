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

  const dayStart = new Date(dateVal);
  const dayEnd = new Date(dateVal);
  dayEnd.setUTCDate(dayEnd.getUTCDate() + 1);

  const games = await prisma.game.findMany({
    where: {
      date: {
        gte: dayStart,
        lt: dayEnd,
      },
    },
    select: {
      id: true,
      homeTeam: { select: { code: true, name: true } },
      awayTeam: { select: { code: true, name: true } },
    },
  });

  const gameIds = games.map((g) => g.id);
  const gameLabelById = new Map(
    games.map((g) => {
      const home = g.homeTeam.code ?? g.homeTeam.name ?? "HOME";
      const away = g.awayTeam.code ?? g.awayTeam.name ?? "AWAY";
      return [g.id, `${away} @ ${home}`];
    }),
  );

  const events = gameIds.length
    ? await prisma.gameEvent.findMany({
        where: { gameId: { in: gameIds } },
        orderBy: [{ eventKey: "asc" }, { side: "asc" }],
      })
    : [];
  const outcomeCount = events.filter((e) => e.type === "outcome").length;
  const conditionCount = events.length - outcomeCount;

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
        <h2>Event Summary</h2>
        <table>
          <tbody>
            <tr><td><strong>Games</strong></td><td>{games.length}</td></tr>
            <tr><td><strong>Total Events</strong></td><td>{events.length}</td></tr>
            <tr><td><strong>Condition Events</strong></td><td>{conditionCount}</td></tr>
            <tr><td><strong>Outcome Events</strong></td><td>{outcomeCount}</td></tr>
          </tbody>
        </table>
      </div>

      <div className="card">
        <h2>Stored Events ({events.length})</h2>
        {events.length > 0 ? (
          <table>
            <thead>
              <tr>
                <th>Event Key</th>
                <th>Type</th>
                <th>Side</th>
                <th>Season</th>
                <th>Game</th>
                <th>Meta</th>
              </tr>
            </thead>
            <tbody>
              {events.map((ev) => (
                <tr key={ev.id}>
                  <td className="mono">{ev.eventKey}</td>
                  <td>{ev.type}</td>
                  <td>{ev.side}</td>
                  <td>{ev.season}</td>
                  <td>{gameLabelById.get(ev.gameId) ?? ev.gameId}</td>
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
