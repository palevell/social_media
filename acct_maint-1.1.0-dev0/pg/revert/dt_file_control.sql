-- Revert acct_maint_2023:dt_file_control from pg

BEGIN;

-- SCHEMA: development
SET search_path TO development;

-- TABLE: dt_file_control
DROP TABLE dt_file_control CASCADE;

COMMIT;
