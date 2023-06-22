-- Deploy acct_maint_2023:fn_user_insert to pg
-- requires: dt_user_history
-- requires: dt_user
-- requires: devschema

BEGIN;

SET search_path TO development;

CREATE OR REPLACE FUNCTION fn_user_insert (
	user_id		BIGINT,
	asof		TIMESTAMPTZ,
	username	VARCHAR,
	displayname	VARCHAR,
	created_at	TIMESTAMPTZ,
	followers_count	BIGINT,
	friends_count	BIGINT,
	listed_count	BIGINT,
	media_count	BIGINT,
	statuses_count	BIGINT,
	last_tweeted	TIMESTAMPTZ,
	protected	BOOLEAN,
	verified	BOOLEAN,
	has_image_url	BOOLEAN,
	description	VARCHAR,
	location	VARCHAR,
	url		VARCHAR,
	badge_url	VARCHAR,
	image_url	VARCHAR,
	banner_url	VARCHAR
) RETURNS BIGINT LANGUAGE plpgsql AS $$
BEGIN
	RAISE NOTICE 'fn_user_insert()';
	INSERT INTO dt_user VALUES(
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20
	) ON CONFLICT (id) DO UPDATE
	SET asof =		EXCLUDED.asof,
	    username  =		EXCLUDED.username,
	    displayname =	EXCLUDED.displayname,
	    created_at =	EXCLUDED.created_at,
	    followers_count =	EXCLUDED.followers_count,
	    friends_count =	EXCLUDED.friends_count,
	    listed_count =	EXCLUDED.listed_count,
	    media_count =	EXCLUDED.media_count,
	    statuses_count =	EXCLUDED.statuses_count,
	    last_tweeted =	EXCLUDED.last_tweeted,
	    protected =		EXCLUDED.protected,
	    verified =		EXCLUDED.verified,
	    has_image_url =	EXCLUDED.has_image_url,
	    description =	EXCLUDED.description,
	    location =		EXCLUDED.location,
	    url =		EXCLUDED.url,
	    badge_url =		EXCLUDED.badge_url,
	    image_url =		EXCLUDED.image_url,
	    banner_url =	EXCLUDED.banner_url
        WHERE dt_user.id = EXCLUDED.id
          AND dt_user.asof < EXCLUDED.asof;
          -- AND COALESCE(dt_user.last_tweeted, DATE('0001-01-01')) < EXCLUDED.last_tweeted;
	RETURN user_id;
END; $$;

COMMIT;
