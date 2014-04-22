set hive.exec.infer.bucket.sort=true;
set hive.exec.infer.bucket.sort.num.buckets.power.two=true;
set hive.auto.convert.join=true;

-- This tests inferring how data is bucketed/sorted from the operators in the reducer
-- and populating that information in partitions' metadata.  In particular, those cases
-- where joins may be auto converted to map joins.

CREATE TABLE test_table (key STRING, value STRING) PARTITIONED BY (part STRING);

-- Tests a join which is converted to a map join, the output should be neither bucketed nor sorted
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT a.key, b.value FROM src a JOIN src b ON a.key = b.key;

DESCRIBE FORMATTED test_table PARTITION (part = '1');

set hive.mapjoin.check.memory.rows=1;
set hive.mapjoin.localtask.max.memory.usage = 0.0001;
set hive.auto.convert.join.noconditionaltask = false;

-- This test tests the scenario when the mapper dies. So, create a conditional task for the mapjoin.
-- Tests a join which is not converted to a map join, the output should be bucketed and sorted.

INSERT OVERWRITE TABLE test_table PARTITION (part = '1') 
SELECT a.key, b.value FROM src a JOIN src b ON a.key = b.key;

DESCRIBE FORMATTED test_table PARTITION (part = '1');
