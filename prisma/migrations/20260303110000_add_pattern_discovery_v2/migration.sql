-- CreateTable
CREATE TABLE "FeatureBin" (
    "id" TEXT NOT NULL,
    "featureName" TEXT NOT NULL,
    "binEdges" JSONB NOT NULL,
    "method" TEXT NOT NULL,
    "seasonRange" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "FeatureBin_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "GameFeatureToken" (
    "id" TEXT NOT NULL,
    "gameId" TEXT NOT NULL,
    "season" INTEGER NOT NULL,
    "date" DATE NOT NULL,
    "tokens" TEXT[],
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "GameFeatureToken_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PatternV2" (
    "id" TEXT NOT NULL,
    "outcomeType" TEXT NOT NULL,
    "conditions" TEXT[],
    "discoverySource" TEXT NOT NULL,
    "trainStats" JSONB NOT NULL,
    "valStats" JSONB NOT NULL,
    "forwardStats" JSONB NOT NULL,
    "rawHitRate" DOUBLE PRECISION NOT NULL,
    "posteriorHitRate" DOUBLE PRECISION NOT NULL,
    "lift" DOUBLE PRECISION NOT NULL,
    "edge" DOUBLE PRECISION NOT NULL,
    "score" DOUBLE PRECISION NOT NULL,
    "n" INTEGER NOT NULL,
    "status" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "retiredAt" TIMESTAMP(3),

    CONSTRAINT "PatternV2_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PatternV2Hit" (
    "id" TEXT NOT NULL,
    "patternId" TEXT NOT NULL,
    "gameId" TEXT NOT NULL,
    "hitBool" BOOLEAN NOT NULL,
    "date" DATE NOT NULL,

    CONSTRAINT "PatternV2Hit_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "FeatureBin_featureName_idx" ON "FeatureBin"("featureName");

-- CreateIndex
CREATE INDEX "FeatureBin_createdAt_idx" ON "FeatureBin"("createdAt");

-- CreateIndex
CREATE INDEX "FeatureBin_seasonRange_idx" ON "FeatureBin"("seasonRange");

-- CreateIndex
CREATE UNIQUE INDEX "GameFeatureToken_gameId_key" ON "GameFeatureToken"("gameId");

-- CreateIndex
CREATE INDEX "GameFeatureToken_season_date_idx" ON "GameFeatureToken"("season", "date");

-- CreateIndex
CREATE INDEX "GameFeatureToken_date_idx" ON "GameFeatureToken"("date");

-- CreateIndex
CREATE INDEX "PatternV2_status_idx" ON "PatternV2"("status");

-- CreateIndex
CREATE INDEX "PatternV2_outcomeType_idx" ON "PatternV2"("outcomeType");

-- CreateIndex
CREATE INDEX "PatternV2_score_idx" ON "PatternV2"("score");

-- CreateIndex
CREATE INDEX "PatternV2_edge_idx" ON "PatternV2"("edge");

-- CreateIndex
CREATE UNIQUE INDEX "PatternV2Hit_patternId_gameId_key" ON "PatternV2Hit"("patternId", "gameId");

-- CreateIndex
CREATE INDEX "PatternV2Hit_patternId_date_idx" ON "PatternV2Hit"("patternId", "date");

-- CreateIndex
CREATE INDEX "PatternV2Hit_date_idx" ON "PatternV2Hit"("date");

-- AddForeignKey
ALTER TABLE "GameFeatureToken" ADD CONSTRAINT "GameFeatureToken_gameId_fkey" FOREIGN KEY ("gameId") REFERENCES "Game"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PatternV2Hit" ADD CONSTRAINT "PatternV2Hit_patternId_fkey" FOREIGN KEY ("patternId") REFERENCES "PatternV2"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PatternV2Hit" ADD CONSTRAINT "PatternV2Hit_gameId_fkey" FOREIGN KEY ("gameId") REFERENCES "Game"("id") ON DELETE CASCADE ON UPDATE CASCADE;
