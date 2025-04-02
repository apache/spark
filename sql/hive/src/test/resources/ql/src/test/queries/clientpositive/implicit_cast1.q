CREATE TABLE implicit_test1(a BIGINT, b STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe' WITH SERDEPROPERTIES('serialization.format'= 'org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol') STORED AS TEXTFILE;

EXPLAIN
SELECT implicit_test1.*
FROM implicit_test1
WHERE implicit_test1.a <> 0;

SELECT implicit_test1.*
FROM implicit_test1
WHERE implicit_test1.a <> 0;



