-- Deploy acct_maint_2023:dt_issue to pg
-- requires: devschema

BEGIN;

SET search_path TO development;

CREATE TABLE dt_issue (
	user_id		BIGINT NOT NULL PRIMARY KEY,
	asof		TIMESTAMPTZ NOT NULL DEFAULT now(),
	no_response	BOOLEAN,
	no_tweets	BOOLEAN,
	no_user		BOOLEAN,
	message		TEXT
);

CREATE INDEX idx_issue_asof ON dt_issue(asof);

COMMENT ON TABLE dt_issue IS 'Issues with Retrieving Twitter User Information';
COMMENT ON COLUMN dt_issue.user_id IS     'Twitter User ID (Primary Key)';
COMMENT ON COLUMN dt_issue.asof IS        'As-of Date';
COMMENT ON COLUMN dt_issue.no_response IS 'No response from Twitter';
COMMENT ON COLUMN dt_issue.no_tweets IS   'No Tweets for this User';
COMMENT ON COLUMN dt_issue.no_user IS     'No such User';
COMMENT ON COLUMN dt_issue.message IS     'Message';


CREATE TABLE dt_issue_history (
	hist_id		BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10000001),
	user_id		BIGINT NOT NULL REFERENCES dt_issue(user_id),
	asof		TIMESTAMPTZ NOT NULL,
	no_response	BOOLEAN,
	no_tweets	BOOLEAN,
	no_user		BOOLEAN,
	message		TEXT
);

CREATE UNIQUE INDEX idx_issue_history_user_id_asof ON dt_issue_history(user_id, asof);
CREATE INDEX idx_issue_history_user_id ON dt_issue_history(user_id);
CREATE INDEX idx_issue_history_asof ON dt_issue_history(asof);

COMMENT ON TABLE dt_issue_history IS 'Issue History';
COMMENT ON COLUMN dt_issue_history.hist_id IS     'History ID (Primary Key)';
COMMENT ON COLUMN dt_issue_history.user_id IS     'Twitter User ID';
COMMENT ON COLUMN dt_issue_history.asof IS        'As-of Date';
COMMENT ON COLUMN dt_issue_history.no_response IS 'No response from Twitter';
COMMENT ON COLUMN dt_issue_history.no_tweets IS   'No Tweets for this User';
COMMENT ON COLUMN dt_issue_history.no_user IS     'No such User';
COMMENT ON COLUMN dt_issue_history.message IS     'Message';

COMMIT;
