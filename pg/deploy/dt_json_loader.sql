-- Deploy acct_maint_2023:dt_json_loader to pg
-- requires: devschema
-- requires: dt_batch_control
-- requires: dt_file_control
-- requires: asof_triggers
-- requires: pgcrypto

BEGIN;

SET search_path TO development;

-- TABLE: dt_json_loader
CREATE TABLE dt_json_loader(
	id		BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	batch_id	BIGINT,
	file_id		BIGINT,
	asof		TIMESTAMPTZ NOT NULL DEFAULT now(),
	posted_at	TIMESTAMPTZ,
	data_type	TEXT,
	acct_name	TEXT,
	acct_id		BIGINT,
	md5_hash	TEXT,
	j		JSONB
);

-- Indexes
CREATE INDEX idx_json_acct_name ON dt_json_loader(acct_name);
CREATE INDEX idx_json_acct_id ON dt_json_loader(acct_id);
CREATE INDEX idx_json_asof ON dt_json_loader(asof);
CREATE INDEX idx_json_batch_id ON dt_json_loader(batch_id);
CREATE INDEX idx_json_datatype ON dt_json_loader(data_type);
CREATE INDEX idx_json_file_id ON dt_json_loader(file_id);
CREATE INDEX idx_md5_hash ON dt_json_loader(md5_hash);
CREATE INDEX idx_posted_at ON dt_json_loader(posted_at);

-- Comments
COMMENT ON TABLE dt_json_loader IS 'Table for loading JSON files from twscrape';
COMMENT ON COLUMN dt_json_loader.id IS 'Primary key';
COMMENT ON COLUMN dt_json_loader.acct_name IS 'Account name for context';
COMMENT ON COLUMN dt_json_loader.acct_id IS 'Account number for context';
COMMENT ON COLUMN dt_json_loader.asof IS 'As-of Date';
COMMENT ON COLUMN dt_json_loader.batch_id IS 'Batch ID (Batch Control)';
COMMENT ON COLUMN dt_json_loader.data_type IS 'Following, Follower, User';
COMMENT ON COLUMN dt_json_loader.file_id IS 'File ID (File Control)';
COMMENT ON COLUMN dt_json_loader.j IS 'JSON data (short name for queries)';
COMMENT ON COLUMN dt_json_loader.md5_hash IS 'MD5 hash for JSON';
COMMENT ON COLUMN dt_json_loader.posted_at IS 'Date posted to tables';

-- Views
CREATE VIEW loaded_json AS
SELECT id, batch_id, file_id, asof, posted_at, data_type, acct_name, acct_id, md5_hash,
	j->>'id_str' id_str,
	j->>'username' username,
	j->>'displayname' displayname,
	j->>'created' created,
	j->>'statusesCount' statusesCount,
	j->>'friendsCount' friendsCount,
	j->>'followersCount' followersCount,
	j->>'favouritesCount' favouritesCount,
	j->>'listedCount' listedCount,
	j->>'mediaCount' mediaCount,
	j->>'protected' protected,
	j->>'verified' verified,
	j->>'blue' blue,
	j->>'blueType' blueType,
	j->>'profileImageUrl' profileImageUrl,
	j->>'profileBannerUrl' profileBannerUrl,
	j->>'location' location,
	j->>'rawDescription' rawDescription
FROM dt_json_loader;

COMMIT;
