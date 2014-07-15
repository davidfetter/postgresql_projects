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
