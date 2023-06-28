-- Deploy acct_maint_2023:dt_user to pg
-- requires: devschema

BEGIN;

SET search_path TO development;

-- TABLE: dt_user
CREATE TABLE dt_user (
	user_id		BIGINT NOT NULL PRIMARY KEY,
	asof		TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	username	TEXT NOT NULL,
	displayname	TEXT NOT NULL,
	created_at	TIMESTAMPTZ NOT NULL,
	followers_count	BIGINT,
	friends_count	BIGINT,
	listed_count	BIGINT,
	media_count	BIGINT,
	statuses_count	BIGINT,
	last_tweeted	TIMESTAMPTZ,
	blue		BOOLEAN,	-- New
	protected	BOOLEAN,
	verified	BOOLEAN,
	default_profile	BOOLEAN,	-- Renamed FROM has_image_url
	description	TEXT,
	label		TEXT,	-- Description, New
	location	TEXT,
	url		TEXT,
	-- badge_url	TEXT,	-- Part of Label, Dropped
	image_url	TEXT,
	banner_url	TEXT
);

CREATE INDEX idx_user_asof_desc ON dt_user (asof DESC);
CREATE INDEX idx_user_last_tweeted ON dt_user (last_tweeted);
CREATE INDEX idx_user_status_count_desc ON dt_user (statuses_count DESC);
CREATE INDEX idx_user_username ON dt_user (username);

COMMENT ON TABLE  dt_user IS 'Twitter Users';
COMMENT ON COLUMN dt_user.user_id IS 'Twitter User ID';
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
COMMENT ON COLUMN dt_user.blue IS 'Account has Twitter Blue (paid)';
COMMENT ON COLUMN dt_user.protected IS 'Account is protected';
COMMENT ON COLUMN dt_user.verified IS 'Account is verified';
COMMENT ON COLUMN dt_user.default_profile IS 'Account has default profile image';
COMMENT ON COLUMN dt_user.description IS 'Account description / bio';
COMMENT ON COLUMN dt_user.label IS 'User label (description)';
COMMENT ON COLUMN dt_user.location IS 'User-supplied location';
COMMENT ON COLUMN dt_user.url IS 'Website URL';
COMMENT ON COLUMN dt_user.image_url IS 'Profile picture URL';
COMMENT ON COLUMN dt_user.banner_url IS 'Profile banner URL';

COMMIT;
