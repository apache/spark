EXPLAIN
CREATE TEMPORARY FUNCTION testlength2 AS 'org.apache.hadoop.hive.ql.udf.UDFTestLength2';

CREATE TEMPORARY FUNCTION testlength2 AS 'org.apache.hadoop.hive.ql.udf.UDFTestLength2';

CREATE TABLE dest1(len INT);

FROM src INSERT OVERWRITE TABLE dest1 SELECT testlength2(src.value);

SELECT dest1.* FROM dest1;

DROP TEMPORARY FUNCTION testlength2;
