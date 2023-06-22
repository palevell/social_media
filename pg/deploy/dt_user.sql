-- Deploy acct_maint_2023:dt_user to pg
-- requires: devschema

BEGIN;

SET search_path TO development;

-- TABLE: dt_user
CREATE TABLE dt_user (
	id		BIGINT NOT NULL PRIMARY KEY,
	asof		TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	username	VARCHAR(16) NOT NULL,
	displayname	VARCHAR(50) NOT NULL,
	created_at	TIMESTAMPTZ NOT NULL,
	followers_count	BIGINT,
	friends_count	BIGINT,
	listed_count	BIGINT,
	media_count	BIGINT,
	statuses_count	BIGINT,
	last_tweeted	TIMESTAMPTZ,
	protected	BOOLEAN,
	verified	BOOLEAN,
	has_image_url	BOOLEAN,
	description	VARCHAR(160),
	location	VARCHAR(30),
	url		VARCHAR(100),
	badge_url	VARCHAR(100),
	image_url	VARCHAR(100),
	banner_url	VARCHAR(100)
);

CREATE INDEX idx_user_asof_desc ON dt_user (asof DESC);
CREATE INDEX idx_user_last_tweeted ON dt_user (last_tweeted);
CREATE INDEX idx_user_status_count_desc ON dt_user (statuses_count DESC);
CREATE INDEX idx_user_username ON dt_user (username);

COMMENT ON TABLE  dt_user    IS 'Twitter Users';
COMMENT ON COLUMN dt_user.id IS 'Twitter User ID';
COMMENT ON COLUMN dt_user.asof IS 'As-of Date';
COMMENT ON COLUMN dt_user.username IS '@screenname';
COMMENT ON COLUMN dt_user.displayname IS 'Descriptive Name';
COMMENT ON COLUMN dt_user.created_at IS 'Account creation date';
COMMENT ON COLUMN dt_user.followers_count IS 'Number of accounts following (incoming)';
COMMENT ON COLUMN dt_user.friends_count IS 'Number of accounts being followed (outgoing)';
COMMENT ON COLUMN dt_user.listed_count IS 'Number of public lists';
COMMENT ON COLUMN dt_user.media_count IS 'Number of tweets with media attachments';
COMMENT ON COLUMN dt_user.statuses_count IS 'Number of tweets';
COMMENT ON COLUMN dt_user.last_tweeted IS 'Date of last tweet';
COMMENT ON COLUMN dt_user.protected IS 'Account is protected';
COMMENT ON COLUMN dt_user.verified IS 'Account is verified';
COMMENT ON COLUMN dt_user.has_image_url IS 'Non-anonymous profile picture';
COMMENT ON COLUMN dt_user.description IS 'Account description / bio';
COMMENT ON COLUMN dt_user.location IS 'User-supplied location';
COMMENT ON COLUMN dt_user.url IS 'Website URL';
COMMENT ON COLUMN dt_user.badge_url IS 'Badge URL';
COMMENT ON COLUMN dt_user.image_url IS 'Profile picture URL';
COMMENT ON COLUMN dt_user.banner_url IS 'Profile banner URL';


-- FUNCTION: fn_user_asof()
CREATE OR REPLACE FUNCTION fn_user_asof() RETURNS trigger
	LANGUAGE 'plpgsql' AS $$
BEGIN
	RAISE NOTICE 'fn_user_asof';
	new.asof = now();
	RETURN new;
END; $$;


-- TRIGGER: trb_user_asof - Calls fn_user_asof()
CREATE TRIGGER trb_user_asof BEFORE UPDATE ON dt_user
	FOR EACH ROW EXECUTE PROCEDURE fn_user_asof();

COMMIT;
