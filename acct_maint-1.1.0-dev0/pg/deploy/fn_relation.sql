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
	in_asof		TIMESTAMPTZ
) RETURNS BIGINT AS $$
DECLARE
	colname TEXT;
	relation_id BIGINT;
	value BOOLEAN;
BEGIN
	operation = LOWER(operation);
	CASE
		WHEN operation = 'follow'   THEN colname = 'follows';	value = true;
		WHEN operation = 'unfollow' THEN colname = 'follows';	value = false;
		WHEN operation = 'mute'     THEN colname = 'mutes';	value = true;
		WHEN operation = 'unmute'   THEN colname = 'mutes';	value = false;
		WHEN operation = 'block'    THEN colname = 'blocks';	value = true;
		WHEN operation = 'unblock'  THEN colname = 'blocks';	value = false;
		ELSE RAISE EXCEPTION 'Unknown Operation: %', operation;
	END CASE;

	CASE
		WHEN colname = 'follows' THEN
			INSERT INTO dt_relation AS dr (user_id1, user_id2, asof, follows)
			VALUES (in_user_id1, in_user_id2, in_asof, value)
			ON CONFLICT (user_id1, user_id2) DO UPDATE
				SET follows = value, asof = in_asof
				WHERE dr.user_id1 = in_user_id1
				AND dr.user_id2 = in_user_id2
				AND (dr.follows IS NULL OR dr.follows != value)
			RETURNING rel_id INTO relation_id;
		WHEN colname = 'mutes' THEN
			INSERT INTO dt_relation AS dr (user_id1, user_id2, asof, mutes)
			VALUES (in_user_id1, in_user_id2, in_asof, value)
			ON CONFLICT (user_id1, user_id2) DO UPDATE
				SET mutes = value, asof = in_asof
				WHERE dr.user_id1 = in_user_id1
				AND dr.user_id2 = in_user_id2
				AND (dr.mutes IS NULL OR dr.mutes != value)
			RETURNING rel_id INTO relation_id;
		WHEN colname = 'blocks' THEN
			INSERT INTO dt_relation AS dr(user_id1, user_id2, asof, blocks)
			VALUES (in_user_id1, in_user_id2, in_asof, value)
			ON CONFLICT (user_id1, user_id2) DO UPDATE
				SET blocks = value, asof = in_asof
				WHERE dr.user_id1 = in_user_id1
				AND dr.user_id2 = in_user_id2
				AND (dr.blocks IS NULL OR dr.blocks != value)
			RETURNING rel_id INTO relation_id;
	END CASE;
	RETURN relation_id;
END; $$ LANGUAGE plpgsql SECURITY DEFINER;

COMMIT;


/*
SELECT DISTINCT user_id FROM dt_user
WHERE username IN ('iOS_Guy', 'DoctorLuv6', 'JuliusCaesar69');

SELECT user_id FROM dt_user LIMIT 10;

SELECT * FROM dt_relation LIMIT 100;

SELECT * FROM dt_relation_history LIMIT 100;

-- DoctorLuv6 - 105883359
-- JuliusCaesar69 - 103317643

SELECT pg_sleep(1.1); SELECT 'follow', *	FROM fn_relation('follow',	103317643, 12, '1969-12-31 23:59:58');
SELECT pg_sleep(1.1); SELECT 'unfollow', *	FROM fn_relation('unfollow',	103317643, 12, '1969-12-31 23:59:58');
SELECT pg_sleep(1.1); SELECT 'mute', *		FROM fn_relation('mute',	103317643, 12, '1969-12-31 23:59:58');
SELECT pg_sleep(1.1); SELECT 'unmute', *	FROM fn_relation('unmute',	103317643, 12, '1969-12-31 23:59:58');
SELECT pg_sleep(1.1); SELECT 'block', *		FROM fn_relation('block',	103317643, 12, '1969-12-31 23:59:58');
SELECT pg_sleep(1.1); SELECT 'unblock', *	FROM fn_relation('unblock',	103317643, 12, '1969-12-31 23:59:58');

SELECT pg_sleep(1.1); SELECT 'follow', *	FROM fn_relation('follow',	105883359, 886832413, '1969-12-31 23:59:59');
SELECT pg_sleep(1.1); SELECT 'unfollow', *	FROM fn_relation('unfollow',	105883359, 886832413, '1969-12-31 23:59:59');
SELECT pg_sleep(1.1); SELECT 'mute', *		FROM fn_relation('mute',	105883359, 886832413, '1969-12-31 23:59:59');
SELECT pg_sleep(1.1); SELECT 'unmute', *	FROM fn_relation('unmute',	105883359, 886832413, '1969-12-31 23:59:59');
SELECT pg_sleep(1.1); SELECT 'block', *		FROM fn_relation('block',	105883359, 886832413, '1969-12-31 23:59:59');
SELECT pg_sleep(1.1); SELECT 'unblock', *	FROM fn_relation('unblock',	105883359, 886832413, '1969-12-31 23:59:59');

SELECT * FROM dt_relation_history ORDER BY hist_id DESC LIMIT 100;
 */




