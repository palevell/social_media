-- Deploy acct_maint_2023:dt_auth_user to pg
-- requires: dt_user
-- requires: devschema

BEGIN;

SET search_path TO development;

CREATE TABLE dt_auth_user (
	id		BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	user_id		BIGINT NOT NULL REFERENCES dt_user(user_id),
	username	TEXT NOT NULL,
	status		VARCHAR(1),
	created_at	TIMESTAMPTZ NOT NULL,
	deactivated_at	TIMESTAMPTZ,
	locked_at	TIMESTAMPTZ,
	suspended_at	TIMESTAMPTZ,
	notes		TEXT
);

CREATE UNIQUE INDEX idx_auth_user_username_id ON dt_auth_user(user_id, username);
CREATE INDEX idx_auth_user_user_id ON dt_auth_user(user_id);
CREATE INDEX idx_auth_user_username ON dt_auth_user(username);

COMMENT ON TABLE dt_auth_user IS 'Users with authentication credentials (ie. my accounts)';
COMMENT ON COLUMN dt_auth_user.user_id IS        'Twitter User ID';
COMMENT ON COLUMN dt_auth_user.username IS       '@screenname';
COMMENT ON COLUMN dt_auth_user.status IS         'Account status (active/suspended/deactivated)';
COMMENT ON COLUMN dt_auth_user.created_at IS     'Account creation date';
COMMENT ON COLUMN dt_auth_user.deactivated_at IS 'Account deactivation date';
COMMENT ON COLUMN dt_auth_user.locked_at IS      'Account suspension date';
COMMENT ON COLUMN dt_auth_user.suspended_at IS   'Account suspension date';


INSERT INTO dt_auth_user(user_id, username, created_at)
SELECT	DISTINCT user_id, username, created_at
FROM	dt_user du 
WHERE	username IN (
		'palevell', 'palevell2', 'PPC_Retweets', 'SupportersOfPPC', 
		'JuliusCaesar69', 'Allan29501', 'pal29501', 'python29501',
		'iOS_Guy', 'iPadGuy100', 'DoctorLuv6', 'DoctorEvil4u', 'SWMisoSWF'
		)
ORDER BY user_id;


COMMIT;
