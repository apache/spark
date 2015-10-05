set hive.exec.infer.bucket.sort=true;
set hive.exec.infer.bucket.sort.num.buckets.power.two=true;

-- This tests inferring how data is bucketed/sorted from the operators in the reducer
-- and populating that information in partitions' metadata.  In particular, those cases
-- where multi insert is used.

CREATE TABLE test_table (key STRING, value STRING) PARTITIONED BY (part STRING);

-- Simple case, neither partition should be bucketed or sorted

FROM src
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') SELECT key, value
INSERT OVERWRITE TABLE test_table PARTITION (part = '2') SELECT value, key;

DESCRIBE FORMATTED test_table PARTITION (part = '1');
DESCRIBE FORMATTED test_table PARTITION (part = '2');

-- The partitions should be bucketed and sorted by different keys

FROM src
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') SELECT key, COUNT(*) GROUP BY key
INSERT OVERWRITE TABLE test_table PARTITION (part = '2') SELECT COUNT(*), value GROUP BY value;

DESCRIBE FORMATTED test_table PARTITION (part = '1');
DESCRIBE FORMATTED test_table PARTITION (part = '2');

-- The first partition should be bucketed and sorted, the second should not

FROM src
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') SELECT key, COUNT(*) GROUP BY key
INSERT OVERWRITE TABLE test_table PARTITION (part = '2') SELECT key, value;

DESCRIBE FORMATTED test_table PARTITION (part = '1');
DESCRIBE FORMATTED test_table PARTITION (part = '2');

set hive.multigroupby.singlereducer=true;

-- Test the multi group by single reducer optimization
-- Both partitions should be bucketed by key
FROM src
INSERT OVERWRITE TABLE test_table PARTITION (part = '1') SELECT key, COUNT(*) GROUP BY key
INSERT OVERWRITE TABLE test_table PARTITION (part = '2') SELECT key, SUM(SUBSTR(value, 5)) GROUP BY key;

DESCRIBE FORMATTED test_table PARTITION (part = '1');
DESCRIBE FORMATTED test_table PARTITION (part = '2');
