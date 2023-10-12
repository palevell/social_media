-- Verify acct_maint_2023:fn_issue on pg

BEGIN;

SET search_path TO development;

SELECT 1/COUNT(*) proacl FROM pg_proc WHERE proname='fn_insert_issue';
-- SELECT has_function_privilege('fn_insert_issue()', 'execute');
SELECT has_function_privilege('fn_issue_history()', 'execute');

SELECT 1/count(*) FROM pg_trigger WHERE tgname = 'tra_issue_history';
SELECT 1/count(*) FROM pg_trigger WHERE tgname = 'trb_issue_asof';

ROLLBACK;
