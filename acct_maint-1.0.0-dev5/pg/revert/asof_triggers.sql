-- Revert acct_maint_2023:asof_triggers from pg

BEGIN;

SET search_path TO development;

DROP TRIGGER trb_relation_asof ON dt_relation;
DROP TRIGGER trb_user_asof ON dt_user;
DROP FUNCTION fn_update_asof;

COMMIT;
