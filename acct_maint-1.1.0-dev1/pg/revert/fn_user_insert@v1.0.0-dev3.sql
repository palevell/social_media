-- Revert acct_maint_2023:fn_user_insert from pg

BEGIN;

SET search_path TO development;

DROP FUNCTION fn_user_insert;

COMMIT;
