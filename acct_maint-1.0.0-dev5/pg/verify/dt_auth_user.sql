-- Verify acct_maint_2023:dt_auth_user on pg

BEGIN;

SET search_path TO development;

SELECT	user_id, username, status, created_at,
	deactivated_at, locked_at, suspended_at,
	notes
FROM	dt_auth_user
WHERE	FALSE;

ROLLBACK;
