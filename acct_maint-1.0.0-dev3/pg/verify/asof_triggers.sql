-- Verify acct_maint_2023:asof_triggers on pg

BEGIN;

SET search_path TO development;

SELECT has_function_privilege('fn_update_asof()', 'execute');

SELECT 1/count(*) FROM pg_trigger WHERE tgname = 'trb_relation_asof';
SELECT 1/count(*) FROM pg_trigger WHERE tgname = 'trb_user_asof';

ROLLBACK;
