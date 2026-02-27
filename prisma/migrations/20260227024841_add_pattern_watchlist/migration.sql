-- CreateTable
CREATE TABLE "PatternWatchlist" (
    "id" TEXT NOT NULL,
    "patternId" TEXT NOT NULL,
    "notes" TEXT,
    "enabled" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "PatternWatchlist_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "PatternWatchlist_patternId_key" ON "PatternWatchlist"("patternId");

-- AddForeignKey
ALTER TABLE "PatternWatchlist" ADD CONSTRAINT "PatternWatchlist_patternId_fkey" FOREIGN KEY ("patternId") REFERENCES "Pattern"("id") ON DELETE CASCADE ON UPDATE CASCADE;
