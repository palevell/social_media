-- Verify acct_maint_2023:devschema on pg

BEGIN;

SELECT pg_catalog.has_schema_privilege('development', 'usage');

ROLLBACK;
