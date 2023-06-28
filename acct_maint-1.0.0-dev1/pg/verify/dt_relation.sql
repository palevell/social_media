-- Verify acct_maint_2023:dt_relation on pg

BEGIN;

SET search_path TO development;

-- Verify dt_relation table
SELECT	rel_id, user_id1, user_id2, asof, follows, blocked, muted
FROM	dt_relation
WHERE	FALSE;

ROLLBACK;
