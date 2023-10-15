-- Deploy acct_maint_2023:dt_relation to pg
-- requires: dt_user
-- requires: devschema

BEGIN;

SET search_path TO development;

CREATE TABLE dt_relation (
	rel_id		BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10000001),
	user_id1	BIGINT NOT NULL REFERENCES dt_user(user_id),
	user_id2	BIGINT NOT NULL REFERENCES dt_user(user_id),
	asof		TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	follows		BOOLEAN,
	blocks		BOOLEAN,
	mutes		BOOLEAN
);

CREATE UNIQUE INDEX idx_relation_user1_2 ON dt_relation (user_id1, user_id2);
CREATE INDEX idx_relation_user_id1 ON dt_relation (user_id1);
CREATE INDEX idx_relation_user_id2 ON dt_relation (user_id2);
CREATE INDEX idx_relation_asof ON dt_relation (asof);

COMMENT ON TABLE  dt_relation IS          'Relationship between Twitter Users';
COMMENT ON COLUMN dt_relation.rel_id IS   'Relation ID (Primary Key)';
COMMENT ON COLUMN dt_relation.user_id1 IS 'Source Twitter User ID';
COMMENT ON COLUMN dt_relation.user_id2 IS 'Target Twitter User ID';
COMMENT ON COLUMN dt_relation.asof IS     'As-of Date';
COMMENT ON COLUMN dt_relation.follows IS  'User1 follows User2';
COMMENT ON COLUMN dt_relation.blocks IS  'User1 blocks User2';
COMMENT ON COLUMN dt_relation.mutes IS    'User1 mutes User2';

COMMIT;
