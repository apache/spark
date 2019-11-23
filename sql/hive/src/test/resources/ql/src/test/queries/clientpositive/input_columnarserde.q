
CREATE TABLE input_columnarserde(a array<int>, b array<string>, c map<string,string>, d int, e string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat';

EXPLAIN
FROM src_thrift
INSERT OVERWRITE TABLE input_columnarserde SELECT src_thrift.lint, src_thrift.lstring, src_thrift.mstringstring, src_thrift.aint, src_thrift.astring DISTRIBUTE BY 1;

FROM src_thrift
INSERT OVERWRITE TABLE input_columnarserde SELECT src_thrift.lint, src_thrift.lstring, src_thrift.mstringstring, src_thrift.aint, src_thrift.astring DISTRIBUTE BY 1;

SELECT input_columnarserde.* FROM input_columnarserde DISTRIBUTE BY 1;

SELECT input_columnarserde.a[0], input_columnarserde.b[0], input_columnarserde.c['key2'], input_columnarserde.d, input_columnarserde.e FROM input_columnarserde DISTRIBUTE BY 1;

