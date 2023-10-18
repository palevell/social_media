-- Deploy acct_maint_2023:dt_file_control to pg
-- requires: devschema
-- requires: asof_triggers
-- requires: dt_batch_control

BEGIN;

SET search_path TO development;

CREATE TABLE dt_file_control (
	id		BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10001),
	batch_id	BIGINT NOT NULL,
	file_status	CHAR(1),
	filename	TEXT NOT NULL,
	filedate	TIMESTAMPTZ NOT NULL,
	asof		TIMESTAMPTZ NOT NULL DEFAULT now(),
	lines		INT NOT NULL DEFAULT 0,
	db_rows		INT,
	status_date	TIMESTAMPTZ
);

-- Indexes
CREATE UNIQUE INDEX idx_batch_filename ON dt_file_control(batch_id, filename);
CREATE INDEX idx_file_batch_id ON dt_file_control(batch_id);
CREATE INDEX idx_file_status ON dt_file_control(id, file_status);
CREATE INDEX idx_filename_filedate ON dt_file_control(filename, filedate);
CREATE INDEX idx_file_asof ON dt_file_control(asof);

-- Comments
COMMENT ON TABLE  dt_file_control IS 'Control Table for File Processing';
COMMENT ON COLUMN dt_file_control.id IS 'Primary Key (File ID)';
COMMENT ON COLUMN dt_file_control.batch_id IS 'Batch ID';
COMMENT ON COLUMN dt_file_control.file_status IS 'File Status';
COMMENT ON COLUMN dt_file_control.filename IS 'Name of file being processed';
COMMENT ON COLUMN dt_file_control.filedate IS 'File Modification Date';
COMMENT ON COLUMN dt_file_control.asof IS 'As-Of Date';
COMMENT ON COLUMN dt_file_control.lines IS 'Number of lines in the file';
COMMENT ON COLUMN dt_file_control.db_rows IS 'Number of rows added to database';
COMMENT ON COLUMN dt_file_control.status_date IS 'Date file data was staged';

-- TRIGGER: trb_file_control_asof - Calls fn_update_asof()
CREATE TRIGGER trb_file_control_asof BEFORE UPDATE ON dt_file_control
	FOR EACH ROW EXECUTE PROCEDURE fn_update_asof();

-- VIEW: incomplete_batches
CREATE VIEW incomplete_batches AS
SELECT	DISTINCT bc.id batch_id, bc.batch_date, bc.batch_status
FROM	dt_batch_control bc INNER JOIN
	dt_file_control fc ON
		bc.id = fc.batch_id
WHERE	bc.completion_date IS NULL;

COMMIT;
