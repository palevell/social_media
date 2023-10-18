-- Deploy acct_maint_2023:dt_batch_control to pg
-- requires: devschema

BEGIN;

SET search_path TO development;

-- TABLE: dt_batch_control
CREATE TABLE dt_batch_control (
	id		BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10001),
	batch_date	TIMESTAMPTZ NOT NULL DEFAULT now(),
	batch_status	CHAR(1),
	completion_date	TIMESTAMPTZ
);

-- Indexes
CREATE INDEX idx_batch_id_desc ON dt_batch_control(id DESC);
CREATE INDEX idx_batch_date ON dt_batch_control(batch_date);
CREATE INDEX idx_batch_completion_date ON dt_batch_control(completion_date);
CREATE INDEX idx_batch_id_status ON dt_batch_control(id, batch_status);
CREATE INDEX idx_batch_id_completion ON dt_batch_control(id, completion_date);

-- Comments
COMMENT ON TABLE  dt_batch_control IS 'Control Table for Batch Processing';
COMMENT ON COLUMN dt_batch_control.id IS 'Batch Number';
COMMENT ON COLUMN dt_batch_control.batch_date IS 'Batch Date';
COMMENT ON COLUMN dt_batch_control.batch_status IS 'Batch Status';
COMMENT ON COLUMN dt_batch_control.completion_date IS 'Batch Completion Date';

-- VIEW: last_batch
CREATE VIEW last_batch AS
SELECT id batch_id, batch_date, batch_status, completion_date
FROM dt_batch_control
ORDER BY id DESC
LIMIT 1;

COMMIT;
