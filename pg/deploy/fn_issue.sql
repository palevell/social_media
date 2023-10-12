-- Deploy acct_maint_2023:fn_issue to pg
-- requires: asof_triggers
-- requires: dt_issue
-- requires: devschema

BEGIN;

SET search_path TO development;

-- TRIGGER: trb_issue_asof - Calls fn_update_asof()
CREATE TRIGGER trb_issue_asof BEFORE UPDATE ON dt_issue
	FOR EACH ROW EXECUTE PROCEDURE fn_update_asof();

-- FUNCTION: fn_issue_history()
CREATE OR REPLACE FUNCTION fn_issue_history() RETURNS TRIGGER
AS $$
BEGIN
	INSERT INTO dt_issue_history (
		user_id, asof, no_response,
		no_tweets, no_user, message
	) VALUES (
		NEW.user_id, NEW.asof, NEW.no_response,
		NEW.no_tweets, NEW.no_user, NEW.message
	) ON CONFLICT (user_id, asof) DO NOTHING;
	RETURN NEW;
END; $$ LANGUAGE plpgsql SECURITY DEFINER;


-- TRIGGER: tra_issue_history - Calls fn_issue_history()
CREATE OR REPLACE TRIGGER tra_issue_history
	AFTER INSERT OR UPDATE ON dt_issue
		FOR EACH ROW EXECUTE PROCEDURE fn_issue_history();

-- FUNCTION: insert function
CREATE OR REPLACE FUNCTION fn_insert_issue(
	in_user_id	BIGINT, 
	asof		TIMESTAMPTZ, 
	no_response	BOOLEAN,
	no_tweets	BOOLEAN,
	no_user		BOOLEAN,
	message		TEXT
) RETURNS BIGINT
AS $$
DECLARE returning_user_id BIGINT;
BEGIN 
	INSERT INTO dt_issue AS di VALUES ($1,$2,$3,$4,$5,$6)
	ON CONFLICT (user_id) DO UPDATE
	SET	asof = EXCLUDED.asof,
		no_response = EXCLUDED.no_response,
		no_tweets = EXCLUDED.no_tweets,
		no_user = EXCLUDED.no_user,
		message = EXCLUDED.message
	RETURNING user_id INTO returning_user_id;
	RETURN returning_user_id;
END; $$ LANGUAGE plpgsql SECURITY DEFINER;

COMMIT;
