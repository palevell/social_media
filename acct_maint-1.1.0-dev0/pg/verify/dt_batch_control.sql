-- Verify acct_maint_2023:dt_batch_control on pg

BEGIN;

SET search_path TO development;

-- Verify dt_batch_control table
SELECT id, batch_date, batch_status, completion_date
FROM dt_batch_control
WHERE FALSE;

-- Verify last_batch view
SELECT 1/COUNT(*) FROM pg_views WHERE viewname = 'last_batch';

ROLLBACK;
