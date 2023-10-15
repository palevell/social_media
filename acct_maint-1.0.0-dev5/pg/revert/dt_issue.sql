-- Revert acct_maint_2023:dt_issue from pg

BEGIN;

SET search_path TO development;

DROP TABLE dt_issue_history CASCADE;
DROP TABLE dt_issue CASCADE;

COMMIT;
