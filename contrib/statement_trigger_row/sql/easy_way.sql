/*
 * If these were surfaced to PL/pgsql, this is what it might look like.
 */

BEGIN;

CREATE TABLE a(
    id SERIAL PRIMARY KEY,
    i INT
);

CREATE FUNCTION summarize_a_inserts()
RETURNS TRIGGER LANGUAGE plpgsql
AS $$
DECLARE
    the_sum BIGINT;
BEGIN
    SELECT INTO the_sum sum(NEW.i)
    FROM
        new_a;
    RAISE NOTICE 'Total change: %.', the_sum;
    RETURN NULL;
END;
$$;

CREATE FUNCTION summarize_a_updates()
RETURNS TRIGGER LANGUAGE plpgsql
AS $$
DECLARE
    the_sum BIGINT;
BEGIN
    SELECT INTO the_sum sum(COALESCE(NEW.i,0) - COALESCE(OLD.i, 0))
    FROM
        old_a
    JOIN
        new_a
        ON(old_a.id = new_a.id);
    RAISE NOTICE 'Total change: %.', the_sum;
    RETURN NULL;
END;
$$;

CREATE FUNCTION summarize_a_deletes()
RETURNS TRIGGER LANGUAGE plpgsql
AS $$
DECLARE
    the_sum BIGINT;
BEGIN
    SELECT INTO the_sum -1 * sum(OLD.i)
    FROM
        old_a;
    RAISE NOTICE 'Total change: %.', the_sum;
    RETURN NULL;
END;
$$;

CREATE TRIGGER statement_after_insert_a
    AFTER INSERT ON a
    REFERENCING
        NEW TABLE AS new_a
    FOR EACH STATEMENT
        EXECUTE PROCEDURE summarize_a_inserts();

CREATE TRIGGER statement_after_update_a
    AFTER UPDATE ON a
    REFERENCING
        OLD TABLE AS old_a
        NEW TABLE AS new_a
    FOR EACH STATEMENT
        EXECUTE PROCEDURE summarize_a_updates();

CREATE TRIGGER statement_after_delete_a
    AFTER DELETE ON a
    REFERENCING
        OLD TABLE AS old_a
    FOR EACH STATEMENT
        EXECUTE PROCEDURE summarize_a_deletes();

INSERT INTO a(i)
SELECT * FROM generate_series(1,10000);

UPDATE a SET i=i+1;

ROLLBACK;

