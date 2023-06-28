-- Deploy acct_maint_2023:dt_relation_history to pg
-- requires: dt_relation
-- requires: dt_user
-- requires: devschema

BEGIN;

SET search_path TO development;

CREATE TABLE dt_relation_history (
	hist_id		BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10000001),
	rel_id		BIGINT NOT NULL REFERENCES dt_relation(rel_id),
	user_id1	BIGINT NOT NULL REFERENCES dt_user(user_id),
	user_id2	BIGINT NOT NULL REFERENCES dt_user(user_id),
	asof		TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	follows		BOOLEAN,
	blocked		BOOLEAN,
	muted		BOOLEAN
);

CREATE UNIQUE INDEX idx_rel_hist_users_asof ON dt_relation_history (user_id1, user_id2, asof);
CREATE INDEX idx_rel_hist_user_id1 ON dt_relation_history (user_id1);
CREATE INDEX idx_rel_hist_user_id2 ON dt_relation_history (user_id2);
CREATE INDEX idx_rel_hist_asof ON dt_relation_history (asof);

COMMENT ON TABLE  dt_relation_history IS 'Relationship History between Twitter Users';
COMMENT ON COLUMN dt_relation_history.hist_id IS  'History ID (Primary Key)';
COMMENT ON COLUMN dt_relation_history.rel_id IS   'Relation ID';
COMMENT ON COLUMN dt_relation_history.user_id1 IS 'Source Twitter User ID';
COMMENT ON COLUMN dt_relation_history.user_id2 IS 'Target Twitter User ID';
COMMENT ON COLUMN dt_relation_history.asof IS     'As-of Date';
COMMENT ON COLUMN dt_relation_history.follows IS  'User1 follows User2';
COMMENT ON COLUMN dt_relation_history.blocked IS  'User1 blocked User2';
COMMENT ON COLUMN dt_relation_history.muted IS    'User1 muted User2';


-- FUNCTION: fn_relation_history()
CREATE OR REPLACE FUNCTION fn_relation_history()
	RETURNS TRIGGER
        LANGUAGE plpgsql
        SECURITY DEFINER	
AS $$
BEGIN
	INSERT INTO dt_relation_history (
		rel_id, user_id1, user_id2, asof,
		follows, blocked, muted
	) VALUES (
		NEW.rel_id, NEW.user_id1, NEW.user_id2, NEW.asof,
		NEW.follows, NEW.blocked, NEW.muted
	) ON CONFLICT (user_id1, user_id2, asof) DO NOTHING;
	RETURN NEW;
END; $$;


-- TRIGGER: tra_relation_history - Calls fn_relation_history()
CREATE OR REPLACE TRIGGER tra_relation_history
	AFTER INSERT OR UPDATE ON dt_relation
		FOR EACH ROW EXECUTE PROCEDURE fn_relation_history();

COMMIT;
