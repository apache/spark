set hive.map.aggr.hash.percentmemory = 0.3;
set hive.mapred.local.mem = 256;

add file ../../data/scripts/dumpdata_script.py;

CREATE table columnTable_Bigdata (key STRING, value STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat';

FROM (FROM src MAP src.key,src.value USING 'python dumpdata_script.py' AS (key,value) WHERE src.key = 10) subq
INSERT OVERWRITE TABLE columnTable_Bigdata SELECT subq.key, subq.value;

describe columnTable_Bigdata;
select count(columnTable_Bigdata.key) from columnTable_Bigdata;


