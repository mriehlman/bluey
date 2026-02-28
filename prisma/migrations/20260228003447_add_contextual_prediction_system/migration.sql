-- CreateTable
CREATE TABLE "GameContext" (
    "id" TEXT NOT NULL,
    "gameId" TEXT NOT NULL,
    "homeWins" INTEGER NOT NULL,
    "homeLosses" INTEGER NOT NULL,
    "homePpg" DOUBLE PRECISION NOT NULL,
    "homeOppg" DOUBLE PRECISION NOT NULL,
    "homePace" DOUBLE PRECISION,
    "homeRebPg" DOUBLE PRECISION,
    "homeAstPg" DOUBLE PRECISION,
    "homeFg3Pct" DOUBLE PRECISION,
    "homeFtPct" DOUBLE PRECISION,
    "homeRankOff" INTEGER,
    "homeRankDef" INTEGER,
    "homeRankPace" INTEGER,
    "homeStreak" INTEGER,
    "awayWins" INTEGER NOT NULL,
    "awayLosses" INTEGER NOT NULL,
    "awayPpg" DOUBLE PRECISION NOT NULL,
    "awayOppg" DOUBLE PRECISION NOT NULL,
    "awayPace" DOUBLE PRECISION,
    "awayRebPg" DOUBLE PRECISION,
    "awayAstPg" DOUBLE PRECISION,
    "awayFg3Pct" DOUBLE PRECISION,
    "awayFtPct" DOUBLE PRECISION,
    "awayRankOff" INTEGER,
    "awayRankDef" INTEGER,
    "awayRankPace" INTEGER,
    "awayStreak" INTEGER,
    "homeRestDays" INTEGER,
    "awayRestDays" INTEGER,
    "homeIsB2b" BOOLEAN NOT NULL DEFAULT false,
    "awayIsB2b" BOOLEAN NOT NULL DEFAULT false,
    "h2hHomeWins" INTEGER NOT NULL DEFAULT 0,
    "h2hAwayWins" INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT "GameContext_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PlayerGameContext" (
    "id" TEXT NOT NULL,
    "gameId" TEXT NOT NULL,
    "playerId" INTEGER NOT NULL,
    "teamId" INTEGER NOT NULL,
    "gamesPlayed" INTEGER NOT NULL,
    "ppg" DOUBLE PRECISION NOT NULL,
    "rpg" DOUBLE PRECISION NOT NULL,
    "apg" DOUBLE PRECISION NOT NULL,
    "mpg" DOUBLE PRECISION NOT NULL,
    "fg3Pct" DOUBLE PRECISION,
    "ftPct" DOUBLE PRECISION,
    "last5Ppg" DOUBLE PRECISION,
    "rankPpg" INTEGER,
    "rankRpg" INTEGER,
    "rankApg" INTEGER,
    "oppRankDef" INTEGER,

    CONSTRAINT "PlayerGameContext_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "GameEvent" (
    "id" TEXT NOT NULL,
    "gameId" TEXT NOT NULL,
    "season" INTEGER NOT NULL,
    "eventKey" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "side" TEXT NOT NULL DEFAULT 'game',
    "meta" JSONB,

    CONSTRAINT "GameEvent_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "GamePattern" (
    "id" TEXT NOT NULL,
    "patternKey" TEXT NOT NULL,
    "conditions" TEXT[],
    "outcome" TEXT NOT NULL,
    "sampleSize" INTEGER NOT NULL,
    "hitCount" INTEGER NOT NULL,
    "hitRate" DOUBLE PRECISION NOT NULL,
    "seasons" INTEGER NOT NULL,
    "perSeason" JSONB NOT NULL,
    "lastHitDate" DATE,
    "confidenceScore" DOUBLE PRECISION,
    "valueScore" DOUBLE PRECISION,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "GamePattern_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "GamePatternHit" (
    "id" TEXT NOT NULL,
    "patternId" TEXT NOT NULL,
    "gameId" TEXT NOT NULL,
    "season" INTEGER NOT NULL,
    "hit" BOOLEAN NOT NULL,

    CONSTRAINT "GamePatternHit_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "GameContext_gameId_key" ON "GameContext"("gameId");

-- CreateIndex
CREATE INDEX "PlayerGameContext_playerId_gameId_idx" ON "PlayerGameContext"("playerId", "gameId");

-- CreateIndex
CREATE UNIQUE INDEX "PlayerGameContext_gameId_playerId_key" ON "PlayerGameContext"("gameId", "playerId");

-- CreateIndex
CREATE INDEX "GameEvent_eventKey_idx" ON "GameEvent"("eventKey");

-- CreateIndex
CREATE INDEX "GameEvent_season_idx" ON "GameEvent"("season");

-- CreateIndex
CREATE INDEX "GameEvent_gameId_idx" ON "GameEvent"("gameId");

-- CreateIndex
CREATE INDEX "GameEvent_type_eventKey_idx" ON "GameEvent"("type", "eventKey");

-- CreateIndex
CREATE UNIQUE INDEX "GameEvent_gameId_eventKey_side_key" ON "GameEvent"("gameId", "eventKey", "side");

-- CreateIndex
CREATE UNIQUE INDEX "GamePattern_patternKey_key" ON "GamePattern"("patternKey");

-- CreateIndex
CREATE INDEX "GamePattern_outcome_idx" ON "GamePattern"("outcome");

-- CreateIndex
CREATE INDEX "GamePattern_hitRate_idx" ON "GamePattern"("hitRate");

-- CreateIndex
CREATE INDEX "GamePattern_confidenceScore_idx" ON "GamePattern"("confidenceScore");

-- CreateIndex
CREATE INDEX "GamePatternHit_gameId_idx" ON "GamePatternHit"("gameId");

-- CreateIndex
CREATE UNIQUE INDEX "GamePatternHit_patternId_gameId_key" ON "GamePatternHit"("patternId", "gameId");

-- AddForeignKey
ALTER TABLE "GameContext" ADD CONSTRAINT "GameContext_gameId_fkey" FOREIGN KEY ("gameId") REFERENCES "Game"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PlayerGameContext" ADD CONSTRAINT "PlayerGameContext_gameId_fkey" FOREIGN KEY ("gameId") REFERENCES "Game"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PlayerGameContext" ADD CONSTRAINT "PlayerGameContext_playerId_fkey" FOREIGN KEY ("playerId") REFERENCES "Player"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "GameEvent" ADD CONSTRAINT "GameEvent_gameId_fkey" FOREIGN KEY ("gameId") REFERENCES "Game"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "GamePatternHit" ADD CONSTRAINT "GamePatternHit_patternId_fkey" FOREIGN KEY ("patternId") REFERENCES "GamePattern"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "GamePatternHit" ADD CONSTRAINT "GamePatternHit_gameId_fkey" FOREIGN KEY ("gameId") REFERENCES "Game"("id") ON DELETE CASCADE ON UPDATE CASCADE;
