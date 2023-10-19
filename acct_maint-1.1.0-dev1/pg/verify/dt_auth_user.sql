-- Verify acct_maint_2023:dt_auth_user on pg

BEGIN;

SET search_path TO development;

SELECT	user_id, username, created_at, asof,
	status, status_date, notes
FROM	dt_auth_user
WHERE	FALSE;

ROLLBACK;
