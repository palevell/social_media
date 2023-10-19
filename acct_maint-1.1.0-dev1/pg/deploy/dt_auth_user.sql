-- Deploy acct_maint_2023:dt_auth_user to pg
-- requires: dt_user
-- requires: devschema

BEGIN;

SET search_path TO development;

DROP TABLE IF EXISTS dt_auth_user;

CREATE TABLE dt_auth_user (
	id		BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	user_id		BIGINT NOT NULL REFERENCES dt_user(user_id),
	username	TEXT NOT NULL,
	created_at	TIMESTAMPTZ NOT NULL,
	asof		TIMESTAMPTZ NOT NULL DEFAULT now(),
	status		CHAR(1),
	status_date	TIMESTAMPTZ,
	notes		TEXT
);

CREATE UNIQUE INDEX idx_auth_user_username_id ON dt_auth_user(user_id, username);
CREATE INDEX idx_auth_user_user_id ON dt_auth_user(user_id);
CREATE INDEX idx_auth_user_username ON dt_auth_user(username);

COMMENT ON TABLE dt_auth_user IS 'Users with authentication credentials (ie. my accounts)';
COMMENT ON COLUMN dt_auth_user.user_id IS     'Twitter User ID';
COMMENT ON COLUMN dt_auth_user.username IS    '@ScreenName';
COMMENT ON COLUMN dt_auth_user.created_at IS  'Account creation date';
COMMENT ON COLUMN dt_auth_user.asof IS        'AsOf Date';
COMMENT ON COLUMN dt_auth_user.status IS      'Account status (active/suspended/deactivated)';
COMMENT ON COLUMN dt_auth_user.status_date IS 'Date of status change';

COMMIT;
