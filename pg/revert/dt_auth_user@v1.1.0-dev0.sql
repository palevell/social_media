-- Revert acct_maint_2023:dt_auth_user from pg

BEGIN;

SET search_path TO development;

DROP TABLE IF EXISTS dt_auth_user CASCADE;

COMMIT;
