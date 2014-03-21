set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE srcbucket_mapjoin_part_1 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part_1 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part_1 PARTITION (part='1');

ALTER TABLE srcbucket_mapjoin_part_1 CLUSTERED BY (key) INTO 3 BUCKETS;
LOAD DATA LOCAL INPATH '../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part_1 PARTITION (part='2');
LOAD DATA LOCAL INPATH '../data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part_1 PARTITION (part='2');
LOAD DATA LOCAL INPATH '../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin_part_1 PARTITION (part='2');

CREATE TABLE srcbucket_mapjoin_part_2 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) INTO 3 BUCKETS STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part_2 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part_2 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin_part_2 PARTITION (part='1');

ALTER TABLE srcbucket_mapjoin_part_2 CLUSTERED BY (key) INTO 2 BUCKETS;
LOAD DATA LOCAL INPATH '../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part_2 PARTITION (part='2');
LOAD DATA LOCAL INPATH '../data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part_2 PARTITION (part='2');

ALTER TABLE srcbucket_mapjoin_part_2 CLUSTERED BY (key) INTO 3 BUCKETS;

set hive.optimize.bucketmapjoin=true;

-- The table bucketing metadata matches but the partition metadata does not, bucket map join should not be used

EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key AND a.part IS NOT NULL AND b.part IS NOT NULL;

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key AND a.part IS NOT NULL AND b.part IS NOT NULL;
