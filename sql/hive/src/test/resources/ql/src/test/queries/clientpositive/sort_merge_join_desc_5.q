set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;

CREATE TABLE srcbucket_mapjoin_part_1 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) SORTED BY (key DESC) INTO 1 BUCKETS;
INSERT OVERWRITE TABLE srcbucket_mapjoin_part_1 PARTITION (part='1') SELECT * FROM src;

CREATE TABLE srcbucket_mapjoin_part_2 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) SORTED BY (key DESC) INTO 1 BUCKETS;
INSERT OVERWRITE TABLE srcbucket_mapjoin_part_2 PARTITION (part='1') SELECT * FROM src;

ALTER TABLE srcbucket_mapjoin_part_2 CLUSTERED BY (key) SORTED BY (value DESC) INTO 1 BUCKETS;

set hive.optimize.bucketmapjoin=true;
set hive.optimize.bucketmapjoin.sortedmerge = true;

-- The partition sorting metadata matches but the table metadata does not, sorted merge join should still be used

EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key AND a.part = '1' AND b.part = '1';

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2 b
ON a.key = b.key AND a.part = '1' AND b.part = '1';
