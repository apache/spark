set hive.exec.infer.bucket.sort=true;
set hive.exec.infer.bucket.sort.num.buckets.power.two=true;
set hive.merge.mapredfiles=true;
set mapred.reduce.tasks=2;

-- This tests inferring how data is bucketed/sorted from the operators in the reducer
-- and populating that information in partitions' metadata.  In particular, those cases
-- where where merging may or may not be used.

CREATE TABLE test_table (key STRING, value STRING) PARTITIONED BY (part STRING);

-- Tests a reduce task followed by a merge.  The output should be neither bucketed nor sorted.
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT a.key, b.value FROM src a JOIN src b ON a.key = b.key;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

set hive.merge.smallfiles.avgsize=2;
set hive.exec.compress.output=false;

-- Tests a reduce task followed by a move. The output should be bucketed and sorted.
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT a.key, b.value FROM src a JOIN src b ON a.key = b.key;

DESCRIBE FORMATTED test_table PARTITION (part = '1');
