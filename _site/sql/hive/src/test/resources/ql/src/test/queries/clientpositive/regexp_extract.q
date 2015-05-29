EXPLAIN EXTENDED
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value, 1+2, 3+4)
         USING 'cat'
  CLUSTER BY key
) tmap
SELECT tmap.key, regexp_extract(tmap.value, 'val_(\\d+\\t\\d+)',1) WHERE tmap.key < 100;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value, 1+2, 3+4)
         USING 'cat'
  CLUSTER BY key
) tmap
SELECT tmap.key, regexp_extract(tmap.value, 'val_(\\d+\\t\\d+)',1) WHERE tmap.key < 100;

EXPLAIN EXTENDED
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value, 1+2, 3+4)
         USING 'cat'
  CLUSTER BY key
) tmap
SELECT tmap.key, regexp_extract(tmap.value, 'val_(\\d+\\t\\d+)') WHERE tmap.key < 100;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value, 1+2, 3+4)
         USING 'cat'
  CLUSTER BY key
) tmap
SELECT tmap.key, regexp_extract(tmap.value, 'val_(\\d+\\t\\d+)') WHERE tmap.key < 100;
