set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;
set hive.exec.infer.bucket.sort=true;

-- Test writing to a bucketed table, the output should be bucketed by the bucketing key into the
-- a number of files equal to the number of buckets
CREATE TABLE test_table_bucketed (key STRING, value STRING) PARTITIONED BY (part STRING)
CLUSTERED BY (value) SORTED BY (value) INTO 3 BUCKETS;

-- Despite the fact that normally inferring would say this table is bucketed and sorted on key,
-- this should be bucketed and sorted by value into 3 buckets
INSERT OVERWRITE TABLE test_table_bucketed PARTITION (part = '1')
SELECT key, count(1) FROM src GROUP BY KEY;

DESCRIBE FORMATTED test_table_bucketed PARTITION (part = '1');

-- If the count(*) from sampling the buckets matches the count(*) from each file, the table is
-- bucketed
SELECT COUNT(*) FROM test_table_bucketed TABLESAMPLE (BUCKET 1 OUT OF 3) WHERE part = '1';

SELECT COUNT(*) FROM test_table_bucketed TABLESAMPLE (BUCKET 2 OUT OF 3) WHERE part = '1';

SELECT COUNT(*) FROM test_table_bucketed TABLESAMPLE (BUCKET 3 OUT OF 3) WHERE part = '1';

SELECT cnt FROM (SELECT INPUT__FILE__NAME, COUNT(*) cnt FROM test_table_bucketed WHERE part = '1'
GROUP BY INPUT__FILE__NAME ORDER BY INPUT__FILE__NAME ASC LIMIT 3) a;