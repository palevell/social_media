-- Deploy acct_maint_2023:asof_triggers to pg
-- requires: dt_relation
-- requires: dt_user
-- requires: devschema

BEGIN;

SET search_path TO development;

-- FUNCTION: fn_update_asof()
CREATE OR REPLACE FUNCTION fn_update_asof() RETURNS TRIGGER
	LANGUAGE plpgsql
	SECURITY DEFINER
AS $$
BEGIN
	IF COALESCE(NEW.asof, DATE('0001-01-01')) <= OLD.asof THEN
	        NEW.asof = now();
	END IF;
	RETURN NEW;
END; $$;


-- TRIGGER: trb_relation_asof - Calls fn_update_asof()
CREATE TRIGGER trb_relation_asof BEFORE UPDATE ON dt_relation
	FOR EACH ROW EXECUTE PROCEDURE fn_update_asof();


-- TRIGGER: trb_user_asof - Calls fn_update_asof()
CREATE TRIGGER trb_user_asof BEFORE UPDATE ON dt_user
	FOR EACH ROW EXECUTE PROCEDURE fn_update_asof();

COMMIT;
