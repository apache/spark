set hive.new.job.grouping.set.cardinality=2;

CREATE TABLE T1(a STRING, b STRING, c STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS TEXTFILE; 

-- Since 4 grouping sets would be generated for the query below, an additional MR job should be created
-- This is not allowed with distincts.
SELECT a, b, count(distinct c) from T1 group by a, b with cube;

