CREATE TEMPORARY FUNCTION test_udaf AS 'org.apache.hadoop.hive.ql.udf.UDAFWrongArgLengthForTestCase';

EXPLAIN
SELECT test_udaf(length(src.value)) FROM src;

SELECT test_udaf(length(src.value)) FROM src;
