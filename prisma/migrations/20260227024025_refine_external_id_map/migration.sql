/*
  Warnings:

  - Added the required column `updatedAt` to the `ExternalIdMap` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "ExternalIdMap" ADD COLUMN     "updatedAt" TIMESTAMP(3) NOT NULL;

-- CreateIndex
CREATE INDEX "ExternalIdMap_source_entityType_idx" ON "ExternalIdMap"("source", "entityType");
