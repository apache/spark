CREATE TABLE dest1(key STRING, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM (
  FROM src_thrift
  SELECT TRANSFORM(src_thrift.lint, src_thrift.lintstring)
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.tkey, tmap.tvalue;

FROM (
  FROM src_thrift
  SELECT TRANSFORM(src_thrift.lint, src_thrift.lintstring)
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.tkey, tmap.tvalue;

SELECT dest1.* FROM dest1;
