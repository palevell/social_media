-- Verify acct_maint_2023:dt_user on pg

BEGIN;

SET search_path TO development;

-- Verify dt_user table
SELECT	user_id, username, asof
FROM	dt_user
WHERE	FALSE;

ROLLBACK;
