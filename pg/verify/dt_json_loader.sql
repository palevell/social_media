-- Verify acct_maint_2023:dt_json_loader on pg

BEGIN;

SET search_path TO development;

-- Verify dt_json_loader table
SELECT	id, acct_name, acct_id, asof, j
FROM	dt_json_loader
WHERE	FALSE;

-- Verify loaded_json view
SELECT 1/COUNT(*) FROM pg_views WHERE viewname = 'loaded_json';

ROLLBACK;
