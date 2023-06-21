-- Verify acct_maint_2023:dt_user on pg

BEGIN;

SET search_path TO development;

-- Verify dt_user table
SELECT	id, username, asof
FROM	dt_user
WHERE	FALSE;

-- Verify function fn_user_asof
SELECT has_function_privilege('fn_user_asof()', 'execute');

-- Verify trigger
SELECT 1/count(*) FROM pg_trigger WHERE tgname = 'trb_user_asof';

ROLLBACK;
