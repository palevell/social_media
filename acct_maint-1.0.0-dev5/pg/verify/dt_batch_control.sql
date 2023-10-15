-- Verify acct_maint_2023:dt_batch_control on pg

BEGIN;

SET search_path TO development;

-- Verify dt_batch_control
SELECT id, batch_date
FROM dt_batch_control
WHERE FALSE;

-- verify last_batch
SELECT 1/COUNT(*) FROM pg_views WHERE viewname = 'last_batch';

ROLLBACK;
