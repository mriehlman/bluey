-- CreateTable
CREATE TABLE "Night" (
    "id" TEXT NOT NULL,
    "date" DATE NOT NULL,
    "season" INTEGER NOT NULL,
    "gameCount" INTEGER NOT NULL,
    "statCount" INTEGER NOT NULL,
    "eventHitCount" INTEGER NOT NULL,
    "eventLogicVersion" TEXT,
    "processedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Night_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Night_date_key" ON "Night"("date");

-- CreateIndex
CREATE INDEX "Night_season_idx" ON "Night"("season");
