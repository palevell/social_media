-- Deploy acct_maint_2023:dt_user_history to pg
-- requires: dt_user
-- requires: devschema

BEGIN;

SET search_path TO development;

CREATE TABLE dt_user_history (
	hist_id		BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10000001),
	user_id		BIGINT REFERENCES dt_user(user_id),
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

CREATE UNIQUE INDEX idx_user_history_user_id_asof ON dt_user_history (user_id, asof);
CREATE INDEX idx_user_hist_asof_desc ON dt_user_history (asof DESC);
CREATE INDEX idx_user_hist_last_tweeted ON dt_user_history (last_tweeted);
CREATE INDEX idx_user_hist_status_count_desc ON dt_user_history (statuses_count DESC);
CREATE INDEX idx_user_hist_user_id ON dt_user_history (user_id);
CREATE INDEX idx_user_hist_username ON dt_user_history (username);

COMMENT ON TABLE  dt_user_history IS 'Twitter Users History';
COMMENT ON COLUMN dt_user_history.hist_id IS 'History ID';
COMMENT ON COLUMN dt_user_history.user_id IS 'Twitter User ID';
COMMENT ON COLUMN dt_user_history.asof IS 'As-of Date';
COMMENT ON COLUMN dt_user_history.username IS '@screenname';
COMMENT ON COLUMN dt_user_history.displayname IS 'Descriptive Name';
COMMENT ON COLUMN dt_user_history.created_at IS 'Account creation date';
COMMENT ON COLUMN dt_user_history.followers_count IS 'Number of accounts following (incoming)';
COMMENT ON COLUMN dt_user_history.friends_count IS 'Number of accounts being followed (outgoing)';
COMMENT ON COLUMN dt_user_history.listed_count IS 'Number of public lists';
COMMENT ON COLUMN dt_user_history.media_count IS 'Number of tweets with media attachments';
COMMENT ON COLUMN dt_user_history.statuses_count IS 'Number of tweets';
COMMENT ON COLUMN dt_user_history.last_tweeted IS 'Date of last tweet';
COMMENT ON COLUMN dt_user_history.blue IS 'Account has Twitter Blue (paid)';
COMMENT ON COLUMN dt_user_history.protected IS 'Account is protected';
COMMENT ON COLUMN dt_user_history.verified IS 'Account is verified';
COMMENT ON COLUMN dt_user_history.default_profile IS 'Account has default profile image';
COMMENT ON COLUMN dt_user_history.description IS 'Account description / bio';
COMMENT ON COLUMN dt_user_history.label IS 'User label (description)';
COMMENT ON COLUMN dt_user_history.location IS 'User-supplied location';
COMMENT ON COLUMN dt_user_history.url IS 'Website URL';
COMMENT ON COLUMN dt_user_history.image_url IS 'Profile picture URL';
COMMENT ON COLUMN dt_user_history.banner_url IS 'Profile banner URL';


-- FUNCTION: fn_user_history()
CREATE OR REPLACE FUNCTION fn_user_history()
	RETURNS TRIGGER AS $dt_history$
BEGIN
	INSERT INTO dt_user_history (
		user_id, asof, username, displayname, created_at,
		followers_count, friends_count, listed_count,
		media_count, statuses_count, last_tweeted, blue,
		protected, verified, default_profile, description,
		label, location, url, image_url, banner_url
	) VALUES (
		new.user_id, new.asof, new.username, new.displayname, new.created_at,
		new.followers_count, new.friends_count, new.listed_count,
		new.media_count, new.statuses_count, new.last_tweeted, new.blue,
		new.protected, new.verified, new.default_profile, new.description,
		new.label, new.location, new.url, new.image_url, new.banner_url
	) ON CONFLICT (user_id, asof) DO NOTHING;
	RETURN new;
END;
$dt_history$ LANGUAGE plpgsql;


-- TRIGGER: tra_user_history - Calls fn_user_history()
CREATE TRIGGER tra_user_history
	AFTER INSERT OR UPDATE ON dt_user
		FOR EACH ROW EXECUTE PROCEDURE fn_user_history();

COMMIT;
