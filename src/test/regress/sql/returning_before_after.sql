--
-- Test BEFORE/AFTER feature in RETURNING statements

CREATE TABLE foo_ret (
		bar1 INTEGER,
		bar2 TEXT
		);

INSERT INTO foo_ret VALUES (1, 'x'),(2,'y');

UPDATE foo_ret SET bar1=bar1+1 RETURNING before.*, bar1, bar2;

UPDATE foo_ret SET bar1=bar1-1 RETURNING after.bar1, before.bar1*2;

UPDATE foo_ret SET bar1=bar1+1, bar2=bar2 || 'z' RETURNING before.*, after.*;

-- check single after

UPDATE foo_ret SET bar1=bar1+1, bar2=bar2 || 'a' RETURNING after.*;

-- check single before

UPDATE foo_ret SET bar1=bar1+1, bar2=bar2 || 'b' RETURNING before.*;

-- it should fail
UPDATE foo_ret SET bar1=bar1+before.bar1 RETURNING before.*;
UPDATE foo_ret SET bar1=bar1+after.bar1 RETURNING after.*;

-- test before/after aliases
UPDATE foo_ret AS before SET bar1=bar1+1 RETURNING before.*,after.*;
UPDATE foo_ret AS after SET bar1=bar1-1 RETURNING before.*,after.*;

-- test inheritance
CREATE TABLE foo_ret2_ret (bar INTEGER) INHERITS(foo_ret);

INSERT INTO foo_ret2_ret VALUES (1,'b',5);

UPDATE foo_ret2_ret SET bar1=bar1*2, bar=bar1+5, bar2=bar1::text || bar::text RETURNING before.*, after.*, *;
UPDATE foo_ret SET bar1=bar1+1, bar2=bar2 || 'z' RETURNING before.*, after.*;

-- check views

CREATE VIEW view_foo_ret AS SELECT * FROM foo_ret;

UPDATE view_foo_ret SET bar1=bar1+1 RETURNING before.*, bar1, bar2;

CREATE TABLE foo_ret3 (bar1 INTEGER, bar4 FLOAT);

INSERT INTO foo_ret2_ret VALUES (2, 'asdf', 33);
INSERT INTO foo_ret3 VALUES (2, 7.77);

CREATE VIEW view_join AS SELECT f2.*, f3.bar1 AS f1bar1, f3.bar4 FROM foo_ret2_ret f2
JOIN foo_ret3 f3 ON f2.bar1 = f3.bar1;

UPDATE view_join SET bar1=bar1+5, bar2=bar2||'join', bar=bar1*2, bar4=7 RETURNING before.*, after.*;

-- check triggers
CREATE FUNCTION returning_trig() returns trigger as $$
BEGIN
NEW.bar1 = NEW.bar1*NEW.bar1;
RETURN NEW;
END; $$ language plpgsql;

DROP TABLE foo_ret2_ret CASCADE;
CREATE TRIGGER bef_foo_ret BEFORE UPDATE ON foo_ret FOR EACH ROW EXECUTE PROCEDURE returning_trig();

UPDATE foo_ret SET bar1=bar1+1, bar2=bar2 || 'z' RETURNING before.*, after.*, *;

DROP TABLE foo_ret CASCADE;
DROP TABLE foo_ret3 CASCADE;

CREATE TABLE t1_ret (id serial, x int, y int, z int);
CREATE TABLE t2_ret (id serial, x int, y int, z int);

INSERT INTO t1_ret VALUES (DEFAULT,1,2,3);
INSERT INTO t1_ret VALUES (DEFAULT,4,5,6);

-- check WITH statement
WITH foo_ret AS (UPDATE t1_ret SET x=x*2, y=y+1, z=x+y+z RETURNING BEFORE.x, BEFORE.y, AFTER.z) INSERT INTO t2_ret (x,y,z) SELECT x, y, z FROM foo_ret RETURNING *;

-- check UPDATE ... FROM statement
UPDATE t2_ret SET x = t1_ret.x+2 FROM t1_ret WHERE t2_ret.id=t1_ret.id RETURNING after.x, before.x;
UPDATE t2_ret SET x = t1_ret.x*2 FROM t1_ret WHERE t2_ret.id=t1_ret.id RETURNING after.*, before.*;

DROP TABLE t1_ret;
DROP TABLE t2_ret;
