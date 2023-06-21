-- Revert acct_maint_2023:dt_user_history from pg

BEGIN;

SET search_path TO development;

DROP TABLE dt_user_history CASCADE;

DROP FUNCTION fn_user_history;

COMMIT;
