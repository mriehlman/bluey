import { PrismaClient } from "../dashboard/node_modules/@prisma/client";

const prisma = new PrismaClient();

async function main() {
  const ids = [
    "cmmy6hast0005uozcj0i5z7ij", // DET@GSW
    "cmmy6hau8000luozck7q8pjpj", // DEN@TOR
    "cmmy6hath000duozcotl06b0g", // MEM@BOS
    "cmmy6has50001uozcf1peg17n", // BKN@NYK
    "cmmy6hatv000huozc6rws15ml", // MIN@POR
    "cmmy6hat50009uozcubmvw21i", // HOU@ATL
  ];
  for (const gid of ids) {
    const game = await prisma.game.findUnique({
      where: { id: gid },
      select: {
        homeTeam: { select: { code: true } },
        awayTeam: { select: { code: true } },
      },
    });
    const odds = await prisma.gameOdds.findMany({
      where: { gameId: gid },
      select: {
        source: true,
        mlHome: true,
        mlAway: true,
        spreadHome: true,
        spreadAway: true,
        totalOver: true,
        totalUnder: true,
      },
    });
    console.log(
      `${game?.awayTeam.code} @ ${game?.homeTeam.code}: ${odds.length} odds rows`,
    );
    for (const o of odds) {
      console.log(
        `  ${o.source}: mlHome=${o.mlHome} mlAway=${o.mlAway} spread=${o.spreadHome}/${o.spreadAway} total=${o.totalOver}/${o.totalUnder}`,
      );
    }
  }
  await prisma.$disconnect();
}

main().catch(console.error);
