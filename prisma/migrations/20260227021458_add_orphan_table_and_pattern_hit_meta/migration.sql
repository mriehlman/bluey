-- AlterTable
ALTER TABLE "PatternHit" ADD COLUMN     "meta" JSONB;

-- CreateTable
CREATE TABLE "OrphanPlayerStat" (
    "id" TEXT NOT NULL,
    "sourceGameId" INTEGER NOT NULL,
    "season" INTEGER,
    "stage" INTEGER,
    "league" TEXT,
    "teamId" INTEGER NOT NULL,
    "playerId" INTEGER,
    "reason" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "OrphanPlayerStat_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "OrphanPlayerStat_sourceGameId_idx" ON "OrphanPlayerStat"("sourceGameId");

-- CreateIndex
CREATE INDEX "OrphanPlayerStat_reason_idx" ON "OrphanPlayerStat"("reason");

-- CreateIndex
CREATE INDEX "OrphanPlayerStat_season_idx" ON "OrphanPlayerStat"("season");
