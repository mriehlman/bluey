-- CreateTable
CREATE TABLE "NightTeamAggregate" (
    "id" TEXT NOT NULL,
    "date" DATE NOT NULL,
    "season" INTEGER NOT NULL,
    "teamId" INTEGER NOT NULL,
    "points" INTEGER NOT NULL,
    "rebounds" INTEGER NOT NULL,
    "assists" INTEGER NOT NULL,
    "steals" INTEGER NOT NULL,
    "blocks" INTEGER NOT NULL,
    "turnovers" INTEGER NOT NULL,
    "minutes" INTEGER NOT NULL,

    CONSTRAINT "NightTeamAggregate_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "NightTeamAggregate_season_date_idx" ON "NightTeamAggregate"("season", "date");

-- CreateIndex
CREATE INDEX "NightTeamAggregate_teamId_date_idx" ON "NightTeamAggregate"("teamId", "date");

-- CreateIndex
CREATE UNIQUE INDEX "NightTeamAggregate_date_teamId_key" ON "NightTeamAggregate"("date", "teamId");
