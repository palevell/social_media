-- Deploy acct_maint_2023:fn_relation to pg
-- requires: dt_relation
-- requires: dt_user
-- requires: devschema

BEGIN;

SET search_path TO development;

CREATE OR REPLACE FUNCTION fn_relation (
	operation	TEXT,
	in_user_id1	BIGINT,
	in_user_id2	BIGINT,
	asof		TIMESTAMPTZ
) RETURNS BIGINT AS $$
DECLARE 
	colname TEXT;
	relation_id BIGINT;
	value BOOLEAN;
BEGIN
	operation = LOWER(operation);
	CASE
		WHEN operation = 'follow'   THEN colname = 'follows'; value = true;
		WHEN operation = 'unfollow' THEN colname = 'follows'; value = false;
		WHEN operation = 'mute'     THEN colname = 'mutes'; value = true;
		WHEN operation = 'unmute'   THEN colname = 'mutes'; value = false;
		WHEN operation = 'block'    THEN colname = 'blocks'; value = true;
		WHEN operation = 'unblock'  THEN colname = 'blocks'; value = false;
		ELSE RAISE EXCEPTION 'Unknown Operation: %', operation;
	END CASE;
	RAISE NOTICE 'colname: %  value: %', colname, value;
	EXECUTE
		'INSERT INTO dt_relation(user_id1, user_id2, asof, '
		|| quote_ident(colname) || ') VALUES (%, %, %, %) RETURNING rel_id;',
		in_user_id1, in_user_id2, asof, value
	INTO relation_id;

	-- RETURNING rel_id INTO relation_id;
	RETURN relation_id;
END; $$ LANGUAGE plpgsql SECURITY DEFINER;

COMMIT;

/*
SELECT DISTINCT user_id FROM dt_user WHERE username IN ('iOS_Guy', 'DoctorLuv6');
SELECT user_id FROM dt_user LIMIT 10;

SELECT * FROM dt_relation LIMIT 1;

-- DoctorLuv6 - 105883359

SELECT 'follow', *	FROM fn_relation('follow',	105883359, 886832413, '1969-12-13 23:59:59') UNION
SELECT 'unfollow', *	FROM fn_relation('unfollow',	105883359, 886832413, '1969-12-13 23:59:59') UNION
SELECT 'mute', *	FROM fn_relation('mute',	105883359, 886832413, '1969-12-13 23:59:59') UNION
SELECT 'unmute', *	FROM fn_relation('unmute',	105883359, 886832413, '1969-12-13 23:59:59') UNION
SELECT 'block', *	FROM fn_relation('block',	105883359, 886832413, '1969-12-13 23:59:59') UNION
SELECT 'unblock', *	FROM fn_relation('unblock',	105883359, 886832413, '1969-12-13 23:59:59');
 */




