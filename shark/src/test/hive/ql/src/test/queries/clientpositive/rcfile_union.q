
CREATE table rcfile_unionTable (b STRING, c STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS RCFILE;

FROM src
INSERT OVERWRITE TABLE rcfile_unionTable SELECT src.key, src.value LIMIT 10;

SELECT * FROM (
SELECT b AS cola FROM rcfile_unionTable
UNION ALL
SELECT c AS cola FROM rcfile_unionTable) s ORDER BY cola ASC;

