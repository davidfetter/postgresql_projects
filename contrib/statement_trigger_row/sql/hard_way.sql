CREATE TABLE IF NOT EXISTS h(
    i INTEGER
);

CREATE FUNCTION set_up_h_rows()
RETURNS TRIGGER LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TEMPORARY TABLE IF NOT EXISTS h_rows(LIKE a) ON COMMIT DROP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER statement_before_writing_h
    BEFORE INSERT OR UPDATE OR DELETE ON a
    FOR EACH STATEMENT
        EXECUTE PROCEDURE set_up_h_rows();

CREATE OR REPLACE FUNCTION stash_h_row_deltas()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO h_rows(i)
    VALUES(
        CASE TG_OP
            WHEN 'INSERT' THEN COALESCE(NEW.i,0)
            WHEN 'UPDATE' THEN COALESCE(NEW.i,0) - COALESCE(OLD.i,0)
            WHEN 'DELETE' THEN -1 * COALESCE(OLD.i,0)
        END
    );
    IF TG_OP IN ('INSERT','UPDATE')
        THEN
            RETURN NEW;
        ELSE
            RETURN OLD;
    END IF;
END;
$$;

CREATE TRIGGER during_trg
    BEFORE INSERT OR UPDATE OR DELETE ON a
    FOR EACH ROW
        EXECUTE PROCEDURE stash_h_row_deltas();

CREATE FUNCTION summarize_h_rows()
RETURNS TRIGGER LANGUAGE plpgsql
AS $$
DECLARE the_sum BIGINT;
BEGIN
    SELECT INTO the_sum sum(i) FROM h_rows;
    RAISE NOTICE 'Total change: %.', the_sum;
    TRUNCATE h_rows;
    RETURN NULL;
END;
$$;

CREATE TRIGGER statement_after_writing_h
    AFTER INSERT OR UPDATE OR DELETE ON a
    FOR EACH STATEMENT
        EXECUTE PROCEDURE summarize_h_rows();

INSERT INTO h(i)
SELECT * FROM generate_series(1,10000);

UPDATE h SET i=i+1;

DELETE FROM h WHERE i < 5000;
