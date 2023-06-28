-- Deploy acct_maint_2023:fn_user_insert to pg
-- requires: dt_user_history
-- requires: dt_user
-- requires: devschema

BEGIN;

SET search_path TO development;

CREATE OR REPLACE FUNCTION fn_user_insert (
	in_user_id	BIGINT,
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
	blue		BOOLEAN,
	protected	BOOLEAN,
	verified	BOOLEAN,
	default_profile	BOOLEAN,
	description	VARCHAR,
	label		VARCHAR,
	location	VARCHAR,
	url		VARCHAR,
	image_url	VARCHAR,
	banner_url	VARCHAR
) RETURNS BIGINT LANGUAGE plpgsql AS $$
BEGIN
	INSERT INTO dt_user VALUES(
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21
	) ON CONFLICT (user_id) DO UPDATE
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
	    blue =		EXCLUDED.blue,
	    protected =		EXCLUDED.protected,
	    verified =		EXCLUDED.verified,
	    default_profile =	EXCLUDED.default_profile,
	    description =	EXCLUDED.description,
	    label =		EXCLUDED.label,
	    location =		EXCLUDED.location,
	    url =		EXCLUDED.url,
	    image_url =		EXCLUDED.image_url,
	    banner_url =	EXCLUDED.banner_url
        WHERE dt_user.user_id = EXCLUDED.user_id
          AND dt_user.asof < EXCLUDED.asof;
	RETURN in_user_id;
END; $$;

COMMIT;
