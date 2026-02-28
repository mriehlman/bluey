-- AlterTable
ALTER TABLE "PlayerGameStat" ADD COLUMN     "dreb" INTEGER,
ADD COLUMN     "fg3a" INTEGER,
ADD COLUMN     "fg3m" INTEGER,
ADD COLUMN     "fga" INTEGER,
ADD COLUMN     "fgm" INTEGER,
ADD COLUMN     "fta" INTEGER,
ADD COLUMN     "ftm" INTEGER,
ADD COLUMN     "oreb" INTEGER,
ADD COLUMN     "pf" INTEGER,
ADD COLUMN     "plusMinus" INTEGER;

-- CreateTable
CREATE TABLE "GameOdds" (
    "id" TEXT NOT NULL,
    "gameId" TEXT NOT NULL,
    "source" TEXT NOT NULL,
    "spreadHome" DOUBLE PRECISION,
    "spreadAway" DOUBLE PRECISION,
    "totalOver" DOUBLE PRECISION,
    "totalUnder" DOUBLE PRECISION,
    "mlHome" INTEGER,
    "mlAway" INTEGER,
    "fetchedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "GameOdds_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "GameOdds_gameId_idx" ON "GameOdds"("gameId");

-- CreateIndex
CREATE UNIQUE INDEX "GameOdds_gameId_source_key" ON "GameOdds"("gameId", "source");

-- AddForeignKey
ALTER TABLE "GameOdds" ADD CONSTRAINT "GameOdds_gameId_fkey" FOREIGN KEY ("gameId") REFERENCES "Game"("id") ON DELETE CASCADE ON UPDATE CASCADE;
