-- CreateTable
CREATE TABLE "Team" (
    "id" INTEGER NOT NULL,
    "name" TEXT,
    "code" TEXT,
    "city" TEXT,

    CONSTRAINT "Team_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Game" (
    "id" TEXT NOT NULL,
    "sourceGameId" INTEGER NOT NULL,
    "date" DATE NOT NULL,
    "season" INTEGER NOT NULL,
    "stage" INTEGER NOT NULL,
    "league" TEXT NOT NULL,
    "homeTeamId" INTEGER NOT NULL,
    "awayTeamId" INTEGER NOT NULL,
    "homeScore" INTEGER NOT NULL,
    "awayScore" INTEGER NOT NULL,

    CONSTRAINT "Game_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Player" (
    "id" INTEGER NOT NULL,
    "firstname" TEXT,
    "lastname" TEXT,

    CONSTRAINT "Player_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PlayerGameStat" (
    "id" TEXT NOT NULL,
    "gameId" TEXT NOT NULL,
    "playerId" INTEGER NOT NULL,
    "teamId" INTEGER NOT NULL,
    "minutes" INTEGER NOT NULL,
    "points" INTEGER NOT NULL,
    "assists" INTEGER NOT NULL,
    "rebounds" INTEGER NOT NULL,
    "steals" INTEGER NOT NULL,
    "blocks" INTEGER NOT NULL,
    "turnovers" INTEGER NOT NULL,

    CONSTRAINT "PlayerGameStat_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "NightEvent" (
    "id" TEXT NOT NULL,
    "date" DATE NOT NULL,
    "season" INTEGER NOT NULL,
    "eventKey" TEXT NOT NULL,
    "value" BOOLEAN NOT NULL DEFAULT true,
    "meta" JSONB,

    CONSTRAINT "NightEvent_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Pattern" (
    "id" TEXT NOT NULL,
    "patternKey" TEXT NOT NULL,
    "eventKeys" TEXT[],
    "legs" INTEGER NOT NULL,
    "occurrences" INTEGER NOT NULL,
    "seasons" INTEGER NOT NULL,
    "perSeason" JSONB NOT NULL,
    "longestGapDays" INTEGER,
    "lastHitDate" DATE,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Pattern_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PatternHit" (
    "id" TEXT NOT NULL,
    "patternId" TEXT NOT NULL,
    "date" DATE NOT NULL,
    "season" INTEGER NOT NULL,

    CONSTRAINT "PatternHit_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Game_sourceGameId_key" ON "Game"("sourceGameId");

-- CreateIndex
CREATE INDEX "Game_date_idx" ON "Game"("date");

-- CreateIndex
CREATE INDEX "Game_season_date_idx" ON "Game"("season", "date");

-- CreateIndex
CREATE INDEX "Game_homeTeamId_date_idx" ON "Game"("homeTeamId", "date");

-- CreateIndex
CREATE INDEX "Game_awayTeamId_date_idx" ON "Game"("awayTeamId", "date");

-- CreateIndex
CREATE INDEX "PlayerGameStat_gameId_idx" ON "PlayerGameStat"("gameId");

-- CreateIndex
CREATE INDEX "PlayerGameStat_playerId_idx" ON "PlayerGameStat"("playerId");

-- CreateIndex
CREATE INDEX "PlayerGameStat_teamId_idx" ON "PlayerGameStat"("teamId");

-- CreateIndex
CREATE UNIQUE INDEX "PlayerGameStat_gameId_playerId_key" ON "PlayerGameStat"("gameId", "playerId");

-- CreateIndex
CREATE INDEX "NightEvent_season_date_idx" ON "NightEvent"("season", "date");

-- CreateIndex
CREATE INDEX "NightEvent_eventKey_date_idx" ON "NightEvent"("eventKey", "date");

-- CreateIndex
CREATE UNIQUE INDEX "NightEvent_date_eventKey_key" ON "NightEvent"("date", "eventKey");

-- CreateIndex
CREATE UNIQUE INDEX "Pattern_patternKey_key" ON "Pattern"("patternKey");

-- CreateIndex
CREATE INDEX "Pattern_legs_occurrences_idx" ON "Pattern"("legs", "occurrences");

-- CreateIndex
CREATE INDEX "PatternHit_date_idx" ON "PatternHit"("date");

-- CreateIndex
CREATE UNIQUE INDEX "PatternHit_patternId_date_key" ON "PatternHit"("patternId", "date");

-- AddForeignKey
ALTER TABLE "Game" ADD CONSTRAINT "Game_homeTeamId_fkey" FOREIGN KEY ("homeTeamId") REFERENCES "Team"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Game" ADD CONSTRAINT "Game_awayTeamId_fkey" FOREIGN KEY ("awayTeamId") REFERENCES "Team"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PlayerGameStat" ADD CONSTRAINT "PlayerGameStat_gameId_fkey" FOREIGN KEY ("gameId") REFERENCES "Game"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PlayerGameStat" ADD CONSTRAINT "PlayerGameStat_playerId_fkey" FOREIGN KEY ("playerId") REFERENCES "Player"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PlayerGameStat" ADD CONSTRAINT "PlayerGameStat_teamId_fkey" FOREIGN KEY ("teamId") REFERENCES "Team"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PatternHit" ADD CONSTRAINT "PatternHit_patternId_fkey" FOREIGN KEY ("patternId") REFERENCES "Pattern"("id") ON DELETE CASCADE ON UPDATE CASCADE;
