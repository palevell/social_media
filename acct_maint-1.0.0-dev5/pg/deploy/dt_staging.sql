-- Deploy acct_maint_2023:dt_staging to pg
-- requires: asof_triggers
-- requires: devschema

BEGIN;

SET search_path TO development;

-- TABLE: dt_staging
CREATE TABLE dt_staging (
	id		BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	row_status	CHAR(1),
	row_type	TEXT,
	acct_id		BIGINT NOT NULL,
	user_id		BIGINT NOT NULL,
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
	blue		BOOLEAN,
	blue_type	TEXT,
	protected	BOOLEAN,
	verified	BOOLEAN,
	default_profile	BOOLEAN,	-- Renamed FROM has_image_url
	description	TEXT,
	label		TEXT,
	location	TEXT,
	url		TEXT,
	image_url	TEXT,
	banner_url	TEXT
);

-- Indexes


-- Comments
COMMENT ON TABLE  dt_staging IS 'Staging Table for Twitter Data';
COMMENT ON COLUMN dt_staging.acct_id IS 'Twitter User ID for my Accounts';
COMMENT ON COLUMN dt_staging.row_status IS 'Row Status';
COMMENT ON COLUMN dt_staging.row_type IS 'Row Type (User, Follower, Following, etc.)';
COMMENT ON COLUMN dt_staging.user_id IS 'Twitter User ID';
COMMENT ON COLUMN dt_staging.asof IS 'As-of Date';
COMMENT ON COLUMN dt_staging.username IS '@screenname';
COMMENT ON COLUMN dt_staging.displayname IS 'Descriptive Name';
COMMENT ON COLUMN dt_staging.created_at IS 'Account creation date';
COMMENT ON COLUMN dt_staging.followers_count IS 'Number of accounts following (incoming)';
COMMENT ON COLUMN dt_staging.friends_count IS 'Number of accounts being followed (outgoing)';
COMMENT ON COLUMN dt_staging.listed_count IS 'Number of public lists';
COMMENT ON COLUMN dt_staging.media_count IS 'Number of tweets with media attachments';
COMMENT ON COLUMN dt_staging.statuses_count IS 'Number of tweets';
COMMENT ON COLUMN dt_staging.last_tweeted IS 'Date of last tweet';
COMMENT ON COLUMN dt_staging.blue IS 'Account has Twitter Blue (paid)';
COMMENT ON COLUMN dt_staging.blue_type IS 'Type of Twitter Blue';
COMMENT ON COLUMN dt_staging.protected IS 'Account is protected';
COMMENT ON COLUMN dt_staging.verified IS 'Account is verified';
COMMENT ON COLUMN dt_staging.default_profile IS 'Account has default profile image';
COMMENT ON COLUMN dt_staging.description IS 'Account description / bio';
COMMENT ON COLUMN dt_staging.label IS 'User label (description)';
COMMENT ON COLUMN dt_staging.location IS 'User-supplied location';
COMMENT ON COLUMN dt_staging.url IS 'Website URL';
COMMENT ON COLUMN dt_staging.image_url IS 'Profile picture URL';
COMMENT ON COLUMN dt_staging.banner_url IS 'Profile banner URL';

-- TRIGGER: trb_staging_asof - Calls fn_update_asof()
CREATE TRIGGER trb_staging_asof BEFORE UPDATE ON dt_issue
	FOR EACH ROW EXECUTE PROCEDURE fn_update_asof();

COMMIT;
