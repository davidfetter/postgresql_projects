CREATE TABLE preorder_test(a int,b int);
CREATE TABLE preorder_test2(i int, u int);



INSERT INTO preorder_test2 VALUES(100,105);
INSERT INTO preorder_test2 VALUES(95,110);
INSERT INTO preorder_test2 VALUES(57,112);
INSERT INTO preorder_test2 VALUES(95,110);


INSERT INTO preorder_test VALUES(5,6);
INSERT INTO preorder_test VALUES(17,22);
INSERT INTO preorder_test VALUES(17,11);
INSERT INTO preorder_test VALUES(4,1);

CREATE OR REPLACE FUNCTION correct_order_singlecol(OUT e int, OUT f int) returns setof record as ' SELECT a,b FROM preorder_test ORDER BY a; ' language 'sql' ORDER BY e;
CREATE OR REPLACE FUNCTION incorrect_order_singlecol(OUT e int, OUT f int) returns setof record as ' SELECT a,b FROM preorder_test; ' language 'sql' ORDER BY e;
CREATE OR REPLACE FUNCTION correct_order_multicol(OUT e int, OUT f int) returns setof record as ' SELECT a,b FROM preorder_test ORDER BY a,b; ' language 'sql' ORDER BY e,f;
CREATE OR REPLACE FUNCTION multiple_tables_correct(OUT e int, OUT f int, OUT g int, OUT h int) returns setof record as ' SELECT * FROM preorder_test,preorder_test2 ORDER BY a,b,i; ' language 'sql' ORDER BY e,f,g;


SELECT * FROM correct_order_singlecol();

SELECT * FROM correct_order_singlecol() ORDER BY e;
SELECT * FROM correct_order_singlecol() ORDER BY e,f;
SELECT * FROM correct_order_singlecol() ORDER BY f;

SELECT * FROM incorrect_order_singlecol() ORDER BY e;
SELECT * FROM incorrect_order_singlecol() ORDER BY f;
SELECT * FROM incorrect_order_singlecol() ORDER BY e,f;

SELECT * FROM correct_order_multicol() ORDER BY e,f;
SELECT * FROM correct_order_multicol() ORDER BY e;
SELECT * FROM correct_order_multicol() ORDER BY f;

EXPLAIN (COSTS OFF)SELECT * FROM incorrect_order_singlecol();
EXPLAIN (COSTS OFF) SELECT * FROM correct_order_singlecol() ORDER BY e;
EXPLAIN (COSTS OFF) SELECT * FROM correct_order_singlecol() ORDER BY e,f;
EXPLAIN (COSTS OFF) SELECT * FROM correct_order_singlecol() ORDER BY f;
EXPLAIN (COSTS OFF) SELECT * FROM incorrect_order_singlecol() ORDER BY e,f;

EXPLAIN (COSTS OFF) SELECT * FROM incorrect_order_singlecol() ORDER BY f;

EXPLAIN (COSTS OFF) SELECT * FROM correct_order_multicol() ORDER BY e,f;
EXPLAIN (COSTS OFF) SELECT * FROM correct_order_multicol() ORDER BY e;
EXPLAIN (COSTS OFF) SELECT * FROM correct_order_singlecol() WHERE e=5 ORDER BY f;
EXPLAIN (COSTS OFF) SELECT * FROM correct_order_singlecol() WHERE e=17 ORDER BY f;
EXPLAIN (COSTS OFF) SELECT * FROM multiple_tables_correct() ORDER BY e;
EXPLAIN (COSTS OFF) SELECT * FROM multiple_tables_correct() ORDER BY e,f;

DROP FUNCTION correct_order_singlecol();
DROP FUNCTION incorrect_order_singlecol();
DROP FUNCTION correct_order_multicol();
DROP FUNCTION multiple_tables_correct();

DROP TABLE preorder_test;
DROP TABLE preorder_test2;