CREATE TABLE "SuggestedPlayLedger" (
  "id" TEXT NOT NULL,
  "date" DATE NOT NULL,
  "season" INTEGER NOT NULL,
  "gameId" TEXT NOT NULL,
  "dedupKey" TEXT NOT NULL,
  "outcomeType" TEXT NOT NULL,
  "displayLabel" TEXT,
  "targetPlayerId" INTEGER,
  "targetPlayerName" TEXT,
  "market" TEXT,
  "line" DOUBLE PRECISION,
  "priceAmerican" INTEGER,
  "impliedProb" DOUBLE PRECISION,
  "estimatedProb" DOUBLE PRECISION,
  "modelEdge" DOUBLE PRECISION,
  "ev" DOUBLE PRECISION,
  "posteriorHitRate" DOUBLE PRECISION NOT NULL,
  "metaScore" DOUBLE PRECISION,
  "confidence" DOUBLE PRECISION NOT NULL,
  "votes" INTEGER NOT NULL,
  "stake" DOUBLE PRECISION NOT NULL,
  "isActionable" BOOLEAN NOT NULL DEFAULT false,
  "settledResult" TEXT,
  "settledHit" BOOLEAN,
  "payout" DOUBLE PRECISION,
  "profit" DOUBLE PRECISION,
  "capturedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "SuggestedPlayLedger_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "SuggestedPlayLedger_date_gameId_dedupKey_key"
  ON "SuggestedPlayLedger"("date", "gameId", "dedupKey");

CREATE INDEX "SuggestedPlayLedger_season_date_idx"
  ON "SuggestedPlayLedger"("season", "date");

CREATE INDEX "SuggestedPlayLedger_date_isActionable_idx"
  ON "SuggestedPlayLedger"("date", "isActionable");

CREATE INDEX "SuggestedPlayLedger_settledResult_date_idx"
  ON "SuggestedPlayLedger"("settledResult", "date");

ALTER TABLE "SuggestedPlayLedger"
ADD CONSTRAINT "SuggestedPlayLedger_gameId_fkey"
FOREIGN KEY ("gameId") REFERENCES "Game"("id") ON DELETE CASCADE ON UPDATE CASCADE;
