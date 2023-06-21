-- Deploy acct_maint_2023:dt_user_history to pg
-- requires: dt_user
-- requires: devschema

BEGIN;

SET search_path TO development;

CREATE TABLE dt_user_history (
	id		BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10000001),
	user_id	BIGINT REFERENCES dt_user(id),
	asof		TIMESTAMPTZ NOT NULL,
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

CREATE UNIQUE INDEX idx_user_history_user_id_asof ON dt_user_history (user_id, asof);
CREATE INDEX idx_user_hist_asof_desc ON dt_user_history (asof DESC);
CREATE INDEX idx_user_hist_last_tweeted ON dt_user_history (last_tweeted);
CREATE INDEX idx_user_hist_status_count_desc ON dt_user_history (statuses_count DESC);
CREATE INDEX idx_user_hist_user_id ON dt_user_history (user_id);
CREATE INDEX idx_user_hist_username ON dt_user_history (username);

COMMENT ON TABLE  dt_user_history    IS 'Twitter Users History';
COMMENT ON COLUMN dt_user_history.id IS 'History ID';
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
COMMENT ON COLUMN dt_user_history.protected IS 'Account is protected';
COMMENT ON COLUMN dt_user_history.verified IS 'Account is verified';
COMMENT ON COLUMN dt_user_history.has_image_url IS 'Non-anonymous profile picture';
COMMENT ON COLUMN dt_user_history.description IS 'Account description / bio';
COMMENT ON COLUMN dt_user_history.location IS 'User-supplied location';
COMMENT ON COLUMN dt_user_history.url IS 'Website URL';
COMMENT ON COLUMN dt_user_history.badge_url IS 'Badge URL';
COMMENT ON COLUMN dt_user_history.image_url IS 'Profile picture URL';
COMMENT ON COLUMN dt_user_history.banner_url IS 'Profile banner URL';


-- FUNCTION: fn_user_history()
CREATE OR REPLACE FUNCTION fn_user_history()
	RETURNS TRIGGER AS $dt_history$
BEGIN
	INSERT INTO dt_user_history (
		user_id, asof, username, displayname, created_at,
		followers_count, friends_count, listed_count,
		media_count, statuses_count, last_tweeted,
		protected, verified, has_image_url, description,
		location, url, badge_url, image_url, banner_url
	) VALUES (
		new.id, now(), new.username, new.displayname, new.created_at,
		new.followers_count, new.friends_count, new.listed_count,
		new.media_count, new.statuses_count, new.last_tweeted,
		new.protected, new.verified, new.has_image_url, new.description,
		new.location, new.url, new.badge_url, new.image_url, new.banner_url
	) ON CONFLICT (user_id, asof) DO NOTHING;
	RETURN new;
END;
$dt_history$ LANGUAGE plpgsql;


-- TRIGGER: tra_user_history - Calls fn_user_history()
CREATE TRIGGER tra_user_history
	AFTER INSERT OR UPDATE ON dt_user
		FOR EACH ROW EXECUTE PROCEDURE fn_user_history();

COMMIT;
