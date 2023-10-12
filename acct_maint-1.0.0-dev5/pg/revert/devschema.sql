-- Revert acct_maint_2023:devschema from pg

BEGIN;

DROP SCHEMA development;

COMMIT;
