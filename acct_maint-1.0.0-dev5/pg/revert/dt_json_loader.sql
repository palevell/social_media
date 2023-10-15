-- Revert acct_maint_2023:dt_json_loader from pg

BEGIN;

SET search_path TO development;

DROP TABLE dt_json_loader CASCADE;

COMMIT;
