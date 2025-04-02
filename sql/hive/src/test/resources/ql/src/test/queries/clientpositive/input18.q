CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value, 1+2, 3+4)
         USING 'cat'
  CLUSTER BY key
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.key, regexp_replace(tmap.value,'\t','+') WHERE tmap.key < 100;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value, 1+2, 3+4)
         USING 'cat'
  CLUSTER BY key
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.key, regexp_replace(tmap.value,'\t','+') WHERE tmap.key < 100;

SELECT dest1.* FROM dest1;
