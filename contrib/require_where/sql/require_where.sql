--
--   Test require_where
--

\set echo all

CREATE TABLE test_require_where(t TEXT);

UPDATE test_require_where SET t=t; -- succeeds

DELETE FROM test_require_where; -- succeeds

LOAD 'require_where';

UPDATE test_require_where SET t=t; -- fails

UPDATE test_require_where SET t=t WHERE true; -- succeeds

DELETE FROM test_require_where; -- fails

DELETE FROM test_require_where WHERE true; -- succeeds
