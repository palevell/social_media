-- Verify acct_maint_2023:dt_file_control on pg

BEGIN;

SET search_path TO development;

-- Verify dt_file_control table
SELECT id, batch_id, filename, filedate
FROM dt_file_control
WHERE FALSE;

-- Verify trb_file_control_asof trigger
SELECT 1/count(*) FROM pg_trigger WHERE tgname = 'trb_file_control_asof';

ROLLBACK;
