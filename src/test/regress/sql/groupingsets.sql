select a, b from (values (1,2)) v(a,b) group by rollup (a,b);

select a, sum(b) from (values (1,10),(1,20),(2,40)) v(a,b) group by rollup (a);

select a, b, sum(c) from (values (1,1,10),(1,1,11),(1,2,12),(1,2,13),(1,3,14),(2,3,15),(3,3,16),(3,4,17),(4,1,18),(4,1,19)) v(a,b,c) group by rollup (a,b); 

select (select grouping(a,b) from (values (1)) v2(b)) from (values (1)) v1(a) group by a;

select grouping(p), percentile_disc(p) within group (order by x::float8), array_agg(p)
from generate_series(1,5) x,
     (values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1)) v(p)
group by rollup (p) order by p;

select a, array_agg(b) from (values (1,10),(1,20),(2,40)) v(a,b) group by rollup (a);

select grouping(a), array_agg(b) from (values (1,10),(1,20),(2,40)) v(a,b) group by rollup (a);

select a, sum(b) from aggtest v(a,b) group by rollup (a);

select grouping(a), sum(b) from aggtest v(a,b) group by rollup (a);

select (select grouping(a,b) from (values (1)) v2(c)) from (values (1,2)) v1(a,b) group by a,b;

SELECT four, ten, SUM(SUM(four)) OVER (PARTITION BY four), AVG(ten) FROM tenk1
GROUP BY ROLLUP(four, ten) ORDER BY four, ten;

select a, b from (values (1,2),(2,3)) v(a,b) group by grouping sets((a,b),());

select a, b from (values (1,2),(2,3)) v(a,b) group by rollup((a,b));

select a, b, sum(c) from (values (1,1,10,5),(1,1,11,5),(1,2,12,5),(1,2,13,5),(1,3,14,5),(2,3,15,5),(3,3,16,5),(3,4,17,5),(4,1,18,5),(4,1,19,5)) v(a,b,c,d) group by rollup ((a,b));

create temp view tv2(a,b,c,d,e,f,g) as select a[1], a[2], a[3], a[4], a[5], a[6], generate_series(1,3) from (select (array[1,1,1,1,1,1])[1:6-i] || (array[2,2,2,2,2,2])[7-i:6] as a from generate_series(0,6) i) s;

select a,b, sum(g) from tv2 group by grouping sets ((a,b,c),(a,b));

SELECT grouping(onek.four),grouping(tenk1.four) FROM onek,tenk1 GROUP BY ROLLUP(onek.four,tenk1.four);

CREATE TEMP TABLE testgs_emptytable(a int,b int,c int);

SELECT sum(a) FROM testgs_emptytable GROUP BY ROLLUP(a,b);

SELECT grouping(four), ten FROM tenk1
GROUP BY ROLLUP(four, ten) ORDER BY four, ten;

SELECT grouping(four), ten FROM tenk1
GROUP BY ROLLUP(four, ten) ORDER BY ten;


