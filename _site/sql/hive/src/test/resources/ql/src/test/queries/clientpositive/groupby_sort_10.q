set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

CREATE TABLE T1(key STRING, val STRING) PARTITIONED BY (ds string)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1 PARTITION (ds='1')
SELECT * from src where key = 0 or key = 11;

-- The plan is converted to a map-side plan
EXPLAIN select distinct key from T1;
select distinct key from T1;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1 PARTITION (ds='2')
SELECT * from src where key = 0 or key = 11;

-- The plan is not converted to a map-side, since although the sorting columns and grouping
-- columns match, the user is querying multiple input partitions
EXPLAIN select distinct key from T1;
select distinct key from T1;

DROP TABLE T1;
