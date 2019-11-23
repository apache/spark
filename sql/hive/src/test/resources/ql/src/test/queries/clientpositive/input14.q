CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

SELECT dest1.* FROM dest1;
