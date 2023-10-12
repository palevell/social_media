-- Revert acct_maint_2023:fn_insert_relation from pg

BEGIN;

SET search_path TO development;

DROP FUNCTION fn_relation;

COMMIT;
