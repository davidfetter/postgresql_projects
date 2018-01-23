--
-- Test routine mapping from a foreign data wrapper
--

-- Suppress NOTICE messages during pre-cleanup
SET client_min_messages TO 'warning';

DROP FUNCTION IF EXISTS local_func(x integer);
DROP FUNCTION IF EXISTS local_srf(t text);
DROP FUNCTION IF EXISTS remote_func(x integer);
DROP FUNCTION IF EXISTS remote_srf(t text);
DROP FOREIGN DATA WRAPPER IF EXISTS test_crm_fdw;
DROP FOREIGN DATA WRAPPER IF EXISTS other_test_crm_fdw;

RESET client_min_messages;

CREATE FOREIGN DATA WRAPPER test_crm_fdw;
COMMENT ON FOREIGN DATA WRAPPER test_crm_fdw IS 'test create routine mapping fdw';
CREATE FOREIGN DATA WRAPPER other_test_crm_fdw;
COMMENT ON FOREIGN DATA WRAPPER other_test_crm_fdw IS 'other test create routine mapping fdw';

SELECT EXISTS(  SELECT null
                FROM pg_language
                WHERE lanname = 'sqlmed'
                AND lanispl IS FALSE
                AND lanpltrusted IS TRUE);
-- lanplcallfoid?
-- laninline?
-- lanvalidator?
-- lanacl?

CREATE FUNCTION local_func(x integer) RETURNS integer LANGUAGE SQL as
$$
SELECT 1;
$$;

CREATE FUNCTION local_srf(t text) RETURNS TABLE (a text, b integer) LANGUAGE SQL as
$$
SELECT t, g.g
FROM generate_series (1,10) as g(g);
$$;

CREATE ROUTINE MAPPING remote_func FOR local_func(x integer) SERVER test_crm_fdw;
CREATE ROUTINE MAPPING remote_srf FOR local_srf(t text) SERVER test_crm_fdw;

-- compare and contrast local_func vs remote_func
SELECT proname, prolang, procost, prorows -- what else?
FROM pg_proc
WHERE proname = 'local_func';

SELECT proname, prolang, procost, prorows -- what else?
FROM pg_proc
WHERE proname = 'remote_func';

-- compare and contrast local_srf vs remote_srf
SELECT proname, prolang, procost, prorows -- what else?
FROM pg_proc
WHERE proname = 'local_srf';

SELECT proname, prolang, procost, prorows -- what else?
FROM pg_proc
WHERE proname = 'remote_func';

-- ALTER ROUTINE MAPPING...need an OPTION to have something to alter...TODO


-- REPLACINGing a function to change the language from sqlmed should fail
CREATE OR REPLACE FUNCTION remote_func(x integer)
LANGUAGE SQL as $$
SELECT 2;
$$;

-- REPLACINGing a function to change the server should fail?
CREATE ROUTINE MAPPING remote_func FOR local_func(x integer) SERVER other_test_crm_fdw;

-- DROP ROUTINE MAPPING works on just the name, but maybe we need the full spec?
DROP ROUTINE MAPPING remote_srf;

SELECT proname
FROM pg_proc
WHERE proname = 'remote_srf';

-- DROPPING a routine mapping by dropping the function should work too?
DROP FUNCTION remote_func(x integer);

SELECT proname
FROM pg_proc
WHERE proname = 'remote_func';

DROP FUNCTION local_srf(t text);
DROP FUNCTION local_srf(t text);
DROP FOREIGN DATA WRAPPER test_crm_fdw;
DROP FOREIGN DATA WRAPPER other_test_crm_fdw;

