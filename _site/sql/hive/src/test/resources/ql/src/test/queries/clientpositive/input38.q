
CREATE TABLE dest1(key STRING, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value, 1+2, 3+4)
         USING 'cat'
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.key, tmap.value;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value, 1+2, 3+4)
         USING 'cat'
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.key, tmap.value;


SELECT dest1.* FROM dest1;


