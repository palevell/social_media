-- Deploy acct_maint_2023:pgcrypto to pg

BEGIN;

CREATE EXTENSION pgcrypto;

COMMIT;

