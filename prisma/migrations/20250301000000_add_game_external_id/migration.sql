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

-- AddForeignKey (order-safe for shadow DB replay)
DO $$
BEGIN
  IF to_regclass('public."Game"') IS NOT NULL
     AND NOT EXISTS (
       SELECT 1
       FROM pg_constraint
       WHERE conname = 'GameExternalId_gameId_fkey'
     ) THEN
    ALTER TABLE "GameExternalId"
      ADD CONSTRAINT "GameExternalId_gameId_fkey"
      FOREIGN KEY ("gameId") REFERENCES "Game"("id")
      ON DELETE CASCADE ON UPDATE CASCADE;
  END IF;
END $$;
