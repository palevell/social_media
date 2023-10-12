-- Verify acct_maint_2023:dt_staging on pg

BEGIN;

SET search_path TO development;

-- Verify dt_staging table
SELECT	id, acct_id, user_id, username, asof
FROM	dt_staging
WHERE	FALSE;

-- Verify trb_staging_asof
SELECT 1/count(*) FROM pg_trigger WHERE tgname = 'trb_staging_asof';

ROLLBACK;
