-- order by and sort by ordinal positions

create temporary view data as select * from values
  (1, 1),
  (1, 2),
  (2, 1),
  (2, 2),
  (3, 1),
  (3, 2)
  as data(a, b);

select * from data order by 1 desc;

-- mix ordinal and column name
select * from data order by 1 desc, b desc;

-- order by multiple ordinals
select * from data order by 1 desc, 2 desc;

-- 1 + 0 is considered a constant (not an ordinal) and thus ignored
select * from data order by 1 + 0 desc, b desc;

-- negative cases: ordinal position out of range
select * from data order by 0;
select * from data order by -1;
select * from data order by 3;

-- sort by ordinal
select * from data sort by 1 desc;

-- SPARK-52565: Enforce ordinal resolution before other sort order expressions
set spark.sql.prioritizeOrdinalResolutionInSort.enabled=true;
SELECT a FROM data ORDER BY 2, b;
SELECT a FROM data ORDER BY b, 2;
SELECT a FROM data ORDER BY 'b', 2;
SELECT a FROM data ORDER BY `b`, 2;
SELECT a FROM data ORDER BY a, 2;
SELECT a FROM data ORDER BY b, 3;
SELECT a, a + 1 FROM data ORDER BY b, 3;

set spark.sql.prioritizeOrdinalResolutionInSort.enabled=false;
SELECT a FROM data ORDER BY 2, b;
SELECT a FROM data ORDER BY b, 2;
SELECT a FROM data ORDER BY 'b', 2;
SELECT a FROM data ORDER BY `b`, 2;
SELECT a FROM data ORDER BY a, 2;
SELECT a FROM data ORDER BY b, 3;
SELECT a, a + 1 FROM data ORDER BY b, 3;

-- turn off order by ordinal
set spark.sql.orderByOrdinal=false;

-- 0 is now a valid literal
select * from data order by 0;
select * from data sort by 0;
