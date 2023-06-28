-- Deploy acct_maint_2023:fn_insert_relation to pg
-- requires: dt_relation
-- requires: dt_user
-- requires: devschema

BEGIN;

SET search_path TO development;

CREATE OR REPLACE FUNCTION fn_insert_relation (
	in_user_id1	BIGINT,
	in_user_id2	BIGINT,
	asof		TIMESTAMPTZ,
	follows		BOOLEAN,
	blocked		BOOLEAN,
	muted		BOOLEAN
)	RETURNS BIGINT 
	LANGUAGE plpgsql
	SECURITY DEFINER
AS $$
DECLARE 
	relation_id BIGINT;
BEGIN
	INSERT INTO dt_relation(user_id1, user_id2, asof, follows, blocked, muted)
	VALUES ($1,$2,$3,$4,$5,$6)
	ON CONFLICT (user_id1, user_id2) DO UPDATE
		SET	asof =    EXCLUDED.asof,
			follows = EXCLUDED.follows,
			blocked = EXCLUDED.blocked,
			muted =   EXCLUDED.muted
		WHERE	dt_relation.user_id1 = in_user_id1
		  AND	dt_relation.user_id2 = in_user_id2
		  AND	md5(ROW(dt_relation.follows, dt_relation.blocked, dt_relation.muted)::TEXT) 
			!= md5(ROW(EXCLUDED.follows, EXCLUDED.blocked, EXCLUDED.muted)::TEXT) 
		  AND	dt_relation.asof < EXCLUDED.asof
	RETURNING rel_id INTO relation_id;
	RETURN relation_id;
END; $$;

COMMIT;
