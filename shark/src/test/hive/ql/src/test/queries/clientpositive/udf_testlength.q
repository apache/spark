EXPLAIN
CREATE TEMPORARY FUNCTION testlength AS 'org.apache.hadoop.hive.ql.udf.UDFTestLength';

CREATE TEMPORARY FUNCTION testlength AS 'org.apache.hadoop.hive.ql.udf.UDFTestLength';

CREATE TABLE dest1(len INT);

FROM src INSERT OVERWRITE TABLE dest1 SELECT testlength(src.value);

SELECT dest1.* FROM dest1;

DROP TEMPORARY FUNCTION testlength;
