-- Verify acct_maint_2023:dt_issue on pg

BEGIN;

SET search_path TO development;

-- Verify dt_issue table
SELECT	user_id, asof, no_response, no_tweets, no_user, message
FROM	dt_issue
WHERE	FALSE;

-- Verify dt_issue_history table
SELECT	hist_id, user_id, asof, no_response, no_tweets, no_user, message
FROM	dt_issue_history
WHERE	FALSE;

ROLLBACK;
