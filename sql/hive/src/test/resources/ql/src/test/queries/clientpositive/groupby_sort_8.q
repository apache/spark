set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

CREATE TABLE T1(key STRING, val STRING) PARTITIONED BY (ds string)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1 PARTITION (ds='1');

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1 PARTITION (ds='1') select key, val from T1 where ds = '1';

-- The plan is not converted to a map-side, since although the sorting columns and grouping
-- columns match, the user is issueing a distinct.
-- However, after HIVE-4310, partial aggregation is performed on the mapper
EXPLAIN
select count(distinct key) from T1;
select count(distinct key) from T1;

set hive.map.groupby.sorted.testmode=true;
-- In testmode, the plan is not changed
EXPLAIN
select count(distinct key) from T1;
select count(distinct key) from T1;

DROP TABLE T1;
