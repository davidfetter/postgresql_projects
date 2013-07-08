--
-- Test BEFORE/AFTER feature in RETURNING statements

CREATE TABLE foo (
		bar1 INTEGER,
		bar2 TEXT
		);

INSERT INTO foo VALUES (1, 'x'),(2,'y');

UPDATE foo SET bar1=bar1+1 RETURNING before.*, bar1, bar2;

UPDATE foo SET bar1=bar1-1 RETURNING after.bar1, before.bar1*2;

UPDATE foo SET bar1=bar1+1, bar2=bar2 || 'z' RETURNING before.*, after.*;


-- it should fail
UPDATE foo SET bar1=bar1+before.bar1 RETURNING before.*;
UPDATE foo SET bar1=bar1+after.bar1 RETURNING after.*;


-- test inheritance
CREATE TABLE foo2 (bar INTEGER) INHERITS(foo);

INSERT INTO foo2 VALUES (1,'b',5);

UPDATE foo2 SET bar1=bar1*2, bar=bar1+5, bar2=bar1::text || bar::text RETURNING before.*, after.*, *;

-- check views

CREATE VIEW view_foo AS SELECT * FROM foo;

UPDATE foo SET bar1=bar1+1 RETURNING before.*, bar1, bar2;

CREATE TABLE foo3 (bar1 INTEGER, bar4 FLOAT);

INSERT INTO foo2 VALUES (2, 'asdf', 33);
INSERT INTO foo3 VALUES (2, 7.77);

CREATE VIEW view_join AS SELECT f2.*, f3.bar1 AS f1bar1, f3.bar4 FROM foo2 f2 
JOIN foo3 f3 ON f2.bar1 = f3.bar1;

UPDATE view_join SET bar1=bar1+5, bar2=bar2||'join', bar=bar1*2, bar4=7 RETURNING before.*, after.*;

-- check triggers
CREATE FUNCTION returning_trig() returns trigger as $$
BEGIN
NEW.bar1 = NEW.bar1*NEW.bar1; 
RETURN NEW;
END; $$ language plpgsql;

DROP TABLE foo2 CASCADE;
CREATE TRIGGER bef_foo BEFORE UPDATE ON foo FOR EACH ROW EXECUTE PROCEDURE returning_trig();

UPDATE foo SET bar1=bar1+1, bar2=bar2 || 'z' RETURNING before.*, after.*, *;

DROP TABLE foo CASCADE;
DROP TABLE foo3 CASCADE;


