-- Revert acct_maint_2023:dt_staging from pg

BEGIN;

SET search_path TO development;

DROP TABLE dt_staging;

DROP TRIGGER trb_staging_asof ON dt_staging;

COMMIT;
