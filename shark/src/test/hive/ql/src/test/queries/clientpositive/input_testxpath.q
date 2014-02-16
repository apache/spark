CREATE TABLE dest1(key INT, value STRING, mapvalue STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src_thrift
INSERT OVERWRITE TABLE dest1 SELECT src_thrift.lint[1], src_thrift.lintstring[0].mystring, src_thrift.mstringstring['key_2'];

FROM src_thrift
INSERT OVERWRITE TABLE dest1 SELECT src_thrift.lint[1], src_thrift.lintstring[0].mystring, src_thrift.mstringstring['key_2'];

SELECT dest1.* FROM dest1;
