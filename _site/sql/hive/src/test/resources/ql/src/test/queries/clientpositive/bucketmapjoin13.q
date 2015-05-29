set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max=1;

CREATE TABLE srcbucket_mapjoin_part_1 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (value) INTO 2 BUCKETS;

-- part=1 partition for srcbucket_mapjoin_part_1 is bucketed by 'value'
INSERT OVERWRITE TABLE srcbucket_mapjoin_part_1 PARTITION (part='1')
SELECT * FROM src;

ALTER TABLE srcbucket_mapjoin_part_1 CLUSTERED BY (key) INTO 2 BUCKETS;

-- part=2 partition for srcbucket_mapjoin_part_1 is bucketed by 'key'
INSERT OVERWRITE TABLE srcbucket_mapjoin_part_1 PARTITION (part='2')
SELECT * FROM src;

CREATE TABLE srcbucket_mapjoin_part_2 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) INTO 2 BUCKETS;

-- part=1 partition for srcbucket_mapjoin_part_2 is bucketed by 'key'
INSERT OVERWRITE TABLE srcbucket_mapjoin_part_2 PARTITION (part='1')
SELECT * FROM src;

set hive.optimize.bucketmapjoin=true;

-- part=1 partition for srcbucket_mapjoin_part_1 is bucketed by 'value'
-- and it is also being joined. So, bucketed map-join cannot be performed
EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key;

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key;

-- part=2 partition for srcbucket_mapjoin_part_1 is bucketed by 'key'
-- and it is being joined. So, bucketed map-join can be performed
EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key and a.part = '2';

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key and a.part = '2';

ALTER TABLE srcbucket_mapjoin_part_1 drop partition (part = '1');

-- part=2 partition for srcbucket_mapjoin_part_1 is bucketed by 'key'
-- and it is being joined. So, bucketed map-join can be performed
EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key;

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key;

ALTER TABLE srcbucket_mapjoin_part_1 CLUSTERED BY (value) INTO 2 BUCKETS;

-- part=2 partition for srcbucket_mapjoin_part_1 is bucketed by 'key'
-- and it is being joined. So, bucketed map-join can be performed
-- The fact that the table is being bucketed by 'value' does not matter
EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key;

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key;
