set hive.exec.infer.bucket.sort=true;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set mapred.reduce.tasks=2;

CREATE TABLE test_table (key INT, value STRING) PARTITIONED BY (ds STRING, hr STRING);

-- Tests dynamic partitions where bucketing/sorting can be inferred, but not all reducers write
-- all partitions.  The subquery produces rows as follows
-- key = 0:
--    0, <value>, 0
-- key = 1:
--    0, <value>, 1
-- key = 2:
--    1, <value>, 0
-- This means that by distributing by the first column into two reducers, and using the third
-- columns as a dynamic partition, the dynamic partition for 0 will get written in both reducers
-- and the partition for 1 will get written in one reducer.  So hr=0 should be bucketed by key
-- and hr=1 should not.

EXPLAIN
INSERT OVERWRITE TABLE test_table PARTITION (ds = '2008-04-08', hr)
SELECT key2, value, cast(hr as int) FROM
(SELECT if ((key % 3) < 2, 0, 1) as key2, value, (key % 2) as hr
FROM srcpart
WHERE ds = '2008-04-08') a
DISTRIBUTE BY key2;

INSERT OVERWRITE TABLE test_table PARTITION (ds = '2008-04-08', hr)
SELECT key2, value, cast(hr as int) FROM
(SELECT if ((key % 3) < 2, 0, 1) as key2, value, (key % 3 % 2) as hr
FROM srcpart
WHERE ds = '2008-04-08') a
DISTRIBUTE BY key2;

DESCRIBE FORMATTED test_table PARTITION (ds='2008-04-08', hr='0');
DESCRIBE FORMATTED test_table PARTITION (ds='2008-04-08', hr='1');
