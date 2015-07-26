set hive.fetch.task.conversion=more;

EXPLAIN
CREATE TEMPORARY FUNCTION testlength2 AS 'org.apache.hadoop.hive.ql.udf.UDFTestLength2';

CREATE TEMPORARY FUNCTION testlength2 AS 'org.apache.hadoop.hive.ql.udf.UDFTestLength2';

SELECT testlength2(src.value) FROM src;

DROP TEMPORARY FUNCTION testlength2;
