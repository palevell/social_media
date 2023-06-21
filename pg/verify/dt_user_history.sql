-- Verify acct_maint_2023:dt_user_history on pg

BEGIN;

SET search_path TO development;

-- Verify dt_user_history
SELECT	id, user_id, username, asof
FROM	dt_user_history
WHERE	FALSE;

-- Verify function fn_user_asof
SELECT has_function_privilege('fn_user_history()', 'execute');

-- Verify trigger
SELECT 1/count(*) FROM pg_trigger WHERE tgname = 'tra_user_history';

ROLLBACK;
