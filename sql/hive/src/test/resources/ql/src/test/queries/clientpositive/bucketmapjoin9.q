set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE srcbucket_mapjoin_part_1 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part_1 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part_1 PARTITION (part='1');

CREATE TABLE srcbucket_mapjoin_part_2 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) INTO 3 BUCKETS STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part_2 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part_2 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin_part_2 PARTITION (part='1');

ALTER TABLE srcbucket_mapjoin_part_2 CLUSTERED BY (key) INTO 2 BUCKETS;

set hive.optimize.bucketmapjoin=true;

-- The table bucketing metadata matches but the partitions have different numbers of buckets, bucket map join should not be used

EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key AND a.part = '1' and b.part = '1';

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key AND a.part = '1' and b.part = '1';

ALTER TABLE srcbucket_mapjoin_part_2 DROP PARTITION (part='1');
ALTER TABLE srcbucket_mapjoin_part_2 CLUSTERED BY (value) INTO 2 BUCKETS;
LOAD DATA LOCAL INPATH '../../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part_2 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part_2 PARTITION (part='1');

ALTER TABLE srcbucket_mapjoin_part_2 CLUSTERED BY (key) INTO 2 BUCKETS;

-- The table bucketing metadata matches but the partitions are bucketed on different columns, bucket map join should not be used

EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key AND a.part = '1' AND b.part = '1';

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key AND a.part = '1' AND b.part = '1';
