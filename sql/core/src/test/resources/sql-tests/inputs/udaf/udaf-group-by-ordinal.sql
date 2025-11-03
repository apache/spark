-- group by ordinal positions

create temporary view data as select * from values
  (1, 1),
  (1, 2),
  (2, 1),
  (2, 2),
  (3, 1),
  (3, 2)
  as data(a, b);

-- basic case
select a, udaf(b) from data group by 1;

-- constant case
select 1, 2, udaf(b) from data group by 1, 2;

-- duplicate group by column
select a, 1, udaf(b) from data group by a, 1;
select a, 1, udaf(b) from data group by 1, 2;

-- group by a non-aggregate expression's ordinal
select a, b + 2, udaf(2) from data group by a, 2;

-- with alias
select a as aa, b + 2 as bb, udaf(2) from data group by 1, 2;

-- foldable non-literal: this should be the same as no grouping.
select udaf(b) from data group by 1 + 0;

-- negative case: position is an aggregate expression
select a, b, udaf(b) from data group by 3;
select a, b, udaf(b) + 2 from data group by 3;

-- negative case: nondeterministic expression
select a, rand(0), udaf(b)
from 
(select /*+ REPARTITION(1) */ a, b from data) group by a, 2;

-- group by ordinal followed by order by
select a, udaf(a) from (select 1 as a) tmp group by 1 order by 1;

-- group by ordinal followed by having
select udaf(a), a from (select 1 as a) tmp group by 2 having a > 0;

-- mixed cases: group-by ordinals and aliases
select a, a AS k, udaf(b) from data group by k, 1;

-- can use ordinal in CUBE
select a, b, udaf(1) from data group by cube(1, 2);

-- mixed cases: can use ordinal in CUBE
select a, b, udaf(1) from data group by cube(1, b);

-- can use ordinal with cube
select a, b, udaf(1) from data group by 1, 2 with cube;

-- can use ordinal in ROLLUP
select a, b, udaf(1) from data group by rollup(1, 2);

-- mixed cases: can use ordinal in ROLLUP
select a, b, udaf(1) from data group by rollup(1, b);

-- can use ordinal with rollup
select a, b, udaf(1) from data group by 1, 2 with rollup;

-- can use ordinal in GROUPING SETS
select a, b, udaf(1) from data group by grouping sets((1), (2), (1, 2));

-- mixed cases: can use ordinal in GROUPING SETS
select a, b, udaf(1) from data group by grouping sets((1), (b), (a, 2));

select a, b, udaf(1) from data group by a, 2 grouping sets((1), (b), (a, 2));

-- range error
select a, b, udaf(1) from data group by a, -1;

select a, b, udaf(1) from data group by a, 3;

select a, b, udaf(1) from data group by cube(-1, 2);

select a, b, udaf(1) from data group by cube(1, 3);

-- turn off group by ordinal
set spark.sql.groupByOrdinal=false;

-- can now group by negative literal
select udaf(b) from data group by -1;
