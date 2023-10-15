-- Revert acct_maint_2023:dt_batch_control from pg

BEGIN;

SET search_path TO development;

-- TABLE: dt_batch_control
DROP TABLE dt_batch_control CASCADE;

COMMIT;
