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
select a, sum(b) from data group by 1;

-- constant case
select 1, 2, sum(b) from data group by 1, 2;

-- duplicate group by column
select a, 1, sum(b) from data group by a, 1;
select a, 1, sum(b) from data group by 1, 2;

-- group by a non-aggregate expression's ordinal
select a, b + 2, count(2) from data group by a, 2;

-- with alias
select a as aa, b + 2 as bb, count(2) from data group by 1, 2;

-- foldable non-literal: this should be the same as no grouping.
select sum(b) from data group by 1 + 0;

-- negative cases: ordinal out of range
select a, b from data group by -1;
select a, b from data group by 0;
select a, b from data group by 3;

-- negative case: position is an aggregate expression
select a, b, sum(b) from data group by 3;
select a, b, sum(b) + 2 from data group by 3;

-- negative case: nondeterministic expression
select a, rand(0), sum(b) from data group by a, 2;

-- negative case: star
select * from data group by a, b, 1;

-- group by ordinal followed by order by
select a, count(a) from (select 1 as a) tmp group by 1 order by 1;

-- group by ordinal followed by having
select count(a), a from (select 1 as a) tmp group by 2 having a > 0;

-- turn of group by ordinal
set spark.sql.groupByOrdinal=false;

-- can now group by negative literal
select sum(b) from data group by -1;
