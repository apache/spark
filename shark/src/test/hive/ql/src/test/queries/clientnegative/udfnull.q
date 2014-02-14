
CREATE TEMPORARY FUNCTION example_arraysum AS 'org.apache.hadoop.hive.contrib.udf.example.UDFExampleArraySum';

SELECT example_arraysum(lint)FROM src_thrift;

DROP TEMPORARY FUNCTION example_arraysum;
