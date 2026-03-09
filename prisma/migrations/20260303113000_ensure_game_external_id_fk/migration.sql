DO $$
BEGIN
  IF to_regclass('public."GameExternalId"') IS NOT NULL
     AND to_regclass('public."Game"') IS NOT NULL
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
