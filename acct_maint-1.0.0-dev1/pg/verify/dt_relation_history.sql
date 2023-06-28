-- Verify acct_maint_2023:dt_relation_history on pg

BEGIN;

SET search_path TO development;

-- Verify dt_relation_history table
SELECT	hist_id, rel_id, user_id1, user_id2, asof, follows, blocked, muted
FROM	dt_relation_history
WHERE	FALSE;

ROLLBACK;
