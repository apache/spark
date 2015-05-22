set hive.mapred.supports.subdirectories=true;
set hive.internal.ddl.list.bucketing.enable=true;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1(key STRING, val STRING)
SKEWED BY (key) ON ((2)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

CREATE TABLE T2(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2;

CREATE TABLE T3(key STRING, val STRING)
SKEWED BY (val) ON ((12)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T3.txt' INTO TABLE T3;

-- This test is for skewed join compile time optimization for more than 2 tables.
-- The join key for table 3 is different from the join key used for joining
-- tables 1 and 2. Tables 1 and 3 are skewed. Since one of the join sources for table
-- 3 consist of a sub-query which contains a join, the compile time skew join 
-- optimization is not enabled for table 3, but it is used for the first join between
-- tables 1 and 2
-- adding a order by at the end to make the results deterministic

EXPLAIN
select *
from 
T1 a join T2 b on a.key = b.key 
join T3 c on a.val = c.val;

select *
from 
T1 a join T2 b on a.key = b.key 
join T3 c on a.val = c.val
order by a.key, b.key, a.val, b.val;

