CREATE TABLE IF NOT EXISTS e(
    i INT
);

CREATE TRIGGER statement_dml_e
    AFTER INSERT OR UPDATE OR DELETE ON e
    REFERENCING
        OLD TABLE AS old_e
        NEW TABLE AS new_e
    FOR EACH STATEMENT
        EXECUTE PROCEDURE statement_trigger_row();

INSERT INTO e(i)
SELECT * FROM generate_series(1,10000);

UPDATE e SET i=i+1;

DELETE FROM e WHERE i < 5000;
