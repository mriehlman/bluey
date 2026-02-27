import { prisma } from "@/lib/prisma";
import { fmtDate, fmtFloat } from "@/lib/format";
import type { Prisma } from "@prisma/client";

export const dynamic = "force-dynamic";

interface Props {
  searchParams: Promise<{ legs?: string; q?: string; top?: string }>;
}

export default async function PatternsPage({ searchParams }: Props) {
  const params = await searchParams;
  const legsFilter = params.legs ? parseInt(params.legs, 10) : undefined;
  const query = params.q ?? "";
  const limit = Math.min(parseInt(params.top ?? "50", 10) || 50, 500);

  const where: Prisma.PatternWhereInput = {};
  if (legsFilter && !isNaN(legsFilter)) where.legs = legsFilter;
  if (query) where.patternKey = { contains: query, mode: "insensitive" };

  const patterns = await prisma.pattern.findMany({
    where,
    orderBy: { overallScore: "desc" },
    take: limit,
    include: { watchlist: true },
  });

  return (
    <>
      <h1>Patterns</h1>

      <form className="filters" method="GET" action="/patterns">
        <label>
          Legs:{" "}
          <select name="legs" defaultValue={params.legs ?? ""}>
            <option value="">All</option>
            <option value="1">1</option>
            <option value="2">2</option>
            <option value="3">3</option>
            <option value="4">4</option>
            <option value="5">5</option>
          </select>
        </label>
        <label>
          Search: <input type="text" name="q" defaultValue={query} placeholder="pattern key…" />
        </label>
        <label>
          Top: <input type="number" name="top" defaultValue={limit} style={{ width: 70 }} />
        </label>
        <button type="submit">Filter</button>
      </form>

      <table>
        <thead>
          <tr>
            <th>Score</th>
            <th>Stab</th>
            <th>Bal</th>
            <th>Rare</th>
            <th>Rec</th>
            <th>Legs</th>
            <th>Occ</th>
            <th>Seasons</th>
            <th>Last Hit</th>
            <th>Gap</th>
            <th>Pattern Key</th>
            <th>W</th>
          </tr>
        </thead>
        <tbody>
          {patterns.map((p) => (
            <tr key={p.id}>
              <td><strong>{fmtFloat(p.overallScore)}</strong></td>
              <td>{fmtFloat(p.stabilityScore)}</td>
              <td>{fmtFloat(p.balanceScore)}</td>
              <td>{fmtFloat(p.rarityScore)}</td>
              <td>{fmtFloat(p.recencyScore)}</td>
              <td>{p.legs}</td>
              <td>{p.occurrences}</td>
              <td>{p.seasons}</td>
              <td className="mono">{fmtDate(p.lastHitDate)}</td>
              <td>{p.longestGapDays ?? "—"}</td>
              <td>
                <a href={`/patterns/${p.id}`} className="mono">
                  {p.patternKey}
                </a>
              </td>
              <td>
                {p.watchlist ? (
                  <span className={`badge ${p.watchlist.enabled ? "badge-green" : "badge-gray"}`}>
                    W
                  </span>
                ) : null}
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {patterns.length === 0 && <p className="muted">No patterns found.</p>}
    </>
  );
}
