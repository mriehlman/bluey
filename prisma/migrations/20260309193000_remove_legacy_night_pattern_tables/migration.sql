-- Remove legacy nightly pipeline and legacy pattern tables.
DROP TABLE IF EXISTS "PatternWatchlist" CASCADE;
DROP TABLE IF EXISTS "PatternHit" CASCADE;
DROP TABLE IF EXISTS "Pattern" CASCADE;

DROP TABLE IF EXISTS "NightEvent" CASCADE;
DROP TABLE IF EXISTS "NightTeamAggregate" CASCADE;
DROP TABLE IF EXISTS "Night" CASCADE;
