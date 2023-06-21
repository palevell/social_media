-- Revert acct_maint_2023:dt_user from pg

BEGIN;

SET search_path TO development;

-- Drop everything dependent on table dt_user
DROP TABLE dt_user CASCADE;

-- Drop function fn_user_asof
DROP FUNCTION fn_user_asof;

COMMIT;
