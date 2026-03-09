-- AlterTable
ALTER TABLE "Game" ADD COLUMN     "awayLossesPreGame" INTEGER,
ADD COLUMN     "awayWinsPreGame" INTEGER,
ADD COLUMN     "broadcasts" JSONB,
ADD COLUMN     "gameCode" TEXT,
ADD COLUMN     "homeLossesPreGame" INTEGER,
ADD COLUMN     "homeWinsPreGame" INTEGER,
ADD COLUMN     "isNeutralSite" BOOLEAN,
ADD COLUMN     "nbaGameId" TEXT,
ADD COLUMN     "periods" INTEGER,
ADD COLUMN     "poRoundDesc" TEXT,
ADD COLUMN     "seriesText" TEXT,
ADD COLUMN     "status" TEXT;

-- AlterTable
ALTER TABLE "Player" ADD COLUMN     "jerseyNum" TEXT,
ADD COLUMN     "slug" TEXT;

-- AlterTable
ALTER TABLE "PlayerGameStat" ADD COLUMN     "comment" TEXT,
ADD COLUMN     "fg3Pct" DOUBLE PRECISION,
ADD COLUMN     "fgPct" DOUBLE PRECISION,
ADD COLUMN     "ftPct" DOUBLE PRECISION,
ADD COLUMN     "minutesRaw" TEXT,
ADD COLUMN     "position" TEXT,
ADD COLUMN     "starter" BOOLEAN,
ALTER COLUMN "plusMinus" SET DATA TYPE DOUBLE PRECISION;

-- CreateTable
CREATE TABLE "PlayerPropOdds" (
    "id" TEXT NOT NULL,
    "gameId" TEXT NOT NULL,
    "playerId" INTEGER NOT NULL,
    "source" TEXT NOT NULL,
    "market" TEXT NOT NULL,
    "line" DOUBLE PRECISION,
    "overPrice" INTEGER,
    "underPrice" INTEGER,
    "fetchedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "PlayerPropOdds_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "BacktestRun" (
    "id" TEXT NOT NULL,
    "name" TEXT,
    "trainSeasons" INTEGER[],
    "testSeasons" INTEGER[],
    "config" JSONB NOT NULL,
    "totalPatterns" INTEGER NOT NULL,
    "passingPatterns" INTEGER NOT NULL,
    "aggregatePnL" DOUBLE PRECISION NOT NULL,
    "aggregateROI" DOUBLE PRECISION NOT NULL,
    "avgHitRate" DOUBLE PRECISION NOT NULL,
    "runtimeMs" INTEGER NOT NULL,
    "completedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "BacktestRun_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "BacktestResult" (
    "id" TEXT NOT NULL,
    "runId" TEXT NOT NULL,
    "patternKey" TEXT NOT NULL,
    "conditions" TEXT[],
    "outcome" TEXT NOT NULL,
    "trainHitRate" DOUBLE PRECISION NOT NULL,
    "trainSampleSize" INTEGER NOT NULL,
    "testHitRate" DOUBLE PRECISION NOT NULL,
    "testSampleSize" INTEGER NOT NULL,
    "zScoreVsChance" DOUBLE PRECISION NOT NULL,
    "pValueVsChance" DOUBLE PRECISION NOT NULL,
    "zScoreVsTrain" DOUBLE PRECISION NOT NULL,
    "pValueVsTrain" DOUBLE PRECISION NOT NULL,
    "isSignificant" BOOLEAN NOT NULL,
    "isConsistent" BOOLEAN NOT NULL,
    "pnlROI" DOUBLE PRECISION NOT NULL,
    "pnlNetPnL" DOUBLE PRECISION NOT NULL,
    "maxDrawdown" DOUBLE PRECISION NOT NULL,
    "sharpeRatio" DOUBLE PRECISION,
    "grade" TEXT NOT NULL,
    "gradeScore" INTEGER NOT NULL,

    CONSTRAINT "BacktestResult_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "PlayerPropOdds_gameId_idx" ON "PlayerPropOdds"("gameId");

-- CreateIndex
CREATE INDEX "PlayerPropOdds_playerId_idx" ON "PlayerPropOdds"("playerId");

-- CreateIndex
CREATE INDEX "PlayerPropOdds_market_idx" ON "PlayerPropOdds"("market");

-- CreateIndex
CREATE UNIQUE INDEX "PlayerPropOdds_gameId_playerId_source_market_key" ON "PlayerPropOdds"("gameId", "playerId", "source", "market");

-- CreateIndex
CREATE INDEX "BacktestRun_completedAt_idx" ON "BacktestRun"("completedAt");

-- CreateIndex
CREATE INDEX "BacktestResult_grade_idx" ON "BacktestResult"("grade");

-- CreateIndex
CREATE INDEX "BacktestResult_pnlROI_idx" ON "BacktestResult"("pnlROI");

-- CreateIndex
CREATE UNIQUE INDEX "BacktestResult_runId_patternKey_key" ON "BacktestResult"("runId", "patternKey");

-- CreateIndex
CREATE UNIQUE INDEX "Game_nbaGameId_key" ON "Game"("nbaGameId");

-- AddForeignKey
ALTER TABLE "PlayerPropOdds" ADD CONSTRAINT "PlayerPropOdds_gameId_fkey" FOREIGN KEY ("gameId") REFERENCES "Game"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PlayerPropOdds" ADD CONSTRAINT "PlayerPropOdds_playerId_fkey" FOREIGN KEY ("playerId") REFERENCES "Player"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "BacktestResult" ADD CONSTRAINT "BacktestResult_runId_fkey" FOREIGN KEY ("runId") REFERENCES "BacktestRun"("id") ON DELETE CASCADE ON UPDATE CASCADE;
