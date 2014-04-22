set hive.merge.mapfiles = false;
set hive.merge.mapredfiles = false;
-- Set merging to false above to make the explain more readable

CREATE TABLE T1(a STRING, b STRING, c STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS TEXTFILE; 

LOAD DATA LOCAL INPATH '../data/files/grouping_sets.txt' INTO TABLE T1;

-- This tests that cubes and rollups work fine inside sub-queries.
EXPLAIN
SELECT * FROM
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq1
join
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq2
on subq1.a = subq2.a order by subq1.a, subq1.b, subq2.a, subq2.b;

SELECT * FROM
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq1
join
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq2
on subq1.a = subq2.a order by subq1.a, subq1.b, subq2.a, subq2.b;

set hive.new.job.grouping.set.cardinality=2;

-- Since 4 grouping sets would be generated for each sub-query, an additional MR job should be created
-- for each of them
EXPLAIN
SELECT * FROM
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq1
join
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq2
on subq1.a = subq2.a order by subq1.a, subq1.b, subq2.a, subq2.b;

SELECT * FROM
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq1
join
(SELECT a, b, count(*) from T1 where a < 3 group by a, b with cube) subq2
on subq1.a = subq2.a order by subq1.a, subq1.b, subq2.a, subq2.b;

