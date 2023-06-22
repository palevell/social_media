-- Verify acct_maint_2023:fn_user_insert on pg

BEGIN;

SELECT 1/COUNT(*) proacl FROM pg_proc WHERE proname='fn_user_insert';

ROLLBACK;
