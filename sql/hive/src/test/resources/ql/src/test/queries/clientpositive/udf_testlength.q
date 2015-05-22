set hive.fetch.task.conversion=more;

EXPLAIN
CREATE TEMPORARY FUNCTION testlength AS 'org.apache.hadoop.hive.ql.udf.UDFTestLength';

CREATE TEMPORARY FUNCTION testlength AS 'org.apache.hadoop.hive.ql.udf.UDFTestLength';

SELECT testlength(src.value) FROM src;

DROP TEMPORARY FUNCTION testlength;
