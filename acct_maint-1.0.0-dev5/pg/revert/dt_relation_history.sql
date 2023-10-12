-- Revert acct_maint_2023:dt_relation_history from pg

BEGIN;

SET search_path TO development;

DROP TABLE dt_relation_history CASCADE;

COMMIT;
