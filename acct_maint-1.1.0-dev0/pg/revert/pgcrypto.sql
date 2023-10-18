-- Revert acct_maint_2023:pgcrypto from pg

BEGIN;

DROP EXTENSION pgcrypto;

COMMIT;
