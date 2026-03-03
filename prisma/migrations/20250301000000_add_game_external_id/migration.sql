-- CreateTable
CREATE TABLE "GameExternalId" (
    "id" TEXT NOT NULL,
    "gameId" TEXT NOT NULL,
    "source" TEXT NOT NULL,
    "sourceId" TEXT NOT NULL,

    CONSTRAINT "GameExternalId_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "GameExternalId_gameId_source_key" ON "GameExternalId"("gameId", "source");

-- CreateIndex
CREATE UNIQUE INDEX "GameExternalId_source_sourceId_key" ON "GameExternalId"("source", "sourceId");

-- CreateIndex
CREATE INDEX "GameExternalId_gameId_idx" ON "GameExternalId"("gameId");

-- AddForeignKey
ALTER TABLE "GameExternalId" ADD CONSTRAINT "GameExternalId_gameId_fkey" FOREIGN KEY ("gameId") REFERENCES "Game"("id") ON DELETE CASCADE ON UPDATE CASCADE;
