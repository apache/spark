EXPLAIN
CREATE TEMPORARY FUNCTION test_avg AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage';

CREATE TEMPORARY FUNCTION test_avg AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage';

EXPLAIN
SELECT
    test_avg(1),
    test_avg(substr(value,5))
FROM src;

SELECT
    test_avg(1),
    test_avg(substr(value,5))
FROM src;

DROP TEMPORARY FUNCTIOn test_avg;
