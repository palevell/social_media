-- Revert acct_maint_2023:fn_issue from pg

BEGIN;

SET search_path TO development;

DROP TRIGGER tra_issue_history ON dt_issue;
DROP TRIGGER trb_issue_asof ON dt_issue;

DROP FUNCTION fn_insert_issue;
DROP FUNCTION fn_issue_history;


COMMIT;
