-- AlterTable
ALTER TABLE "Game" ADD COLUMN     "awayTeamNameSnapshot" TEXT,
ADD COLUMN     "homeTeamNameSnapshot" TEXT,
ADD COLUMN     "seasonType" TEXT,
ADD COLUMN     "tipoffTimeUtc" TIMESTAMP(3);

-- CreateTable
CREATE TABLE "ExternalIdMap" (
    "id" TEXT NOT NULL,
    "entityType" TEXT NOT NULL,
    "source" TEXT NOT NULL,
    "sourceId" TEXT NOT NULL,
    "internalId" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ExternalIdMap_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "ExternalIdMap_entityType_internalId_idx" ON "ExternalIdMap"("entityType", "internalId");

-- CreateIndex
CREATE UNIQUE INDEX "ExternalIdMap_entityType_source_sourceId_key" ON "ExternalIdMap"("entityType", "source", "sourceId");
