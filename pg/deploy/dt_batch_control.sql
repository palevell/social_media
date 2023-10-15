-- Deploy acct_maint_2023:dt_batch_control to pg
-- requires: devschema

BEGIN;

SET search_path TO development;

-- TABLE: dt_batch_control
CREATE TABLE dt_batch_control (
	id		BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10001),
	batch_date	TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes
CREATE INDEX idx_batch_id_desc ON dt_batch_control(id DESC);
CREATE INDEX idx_batch_date ON dt_batch_control(batch_date);

-- Comments
COMMENT ON TABLE  dt_batch_control IS 'Control Table for Batch Processing';
COMMENT ON COLUMN dt_batch_control.id IS 'Batch Number';
COMMENT ON COLUMN dt_batch_control.batch_date IS 'Batch Date';

-- VIEW: last_batch
CREATE VIEW last_batch AS
SELECT id batch_id, batch_date
FROM dt_batch_control
ORDER BY id DESC
LIMIT 1;

COMMIT;
