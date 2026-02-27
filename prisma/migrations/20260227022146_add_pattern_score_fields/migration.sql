-- AlterTable
ALTER TABLE "Pattern" ADD COLUMN     "balanceScore" DOUBLE PRECISION,
ADD COLUMN     "overallScore" DOUBLE PRECISION,
ADD COLUMN     "rarityScore" DOUBLE PRECISION,
ADD COLUMN     "recencyScore" DOUBLE PRECISION,
ADD COLUMN     "stabilityScore" DOUBLE PRECISION;

-- CreateIndex
CREATE INDEX "Pattern_overallScore_idx" ON "Pattern"("overallScore");
