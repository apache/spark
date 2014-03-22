CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

ADD FILE ../data/scripts/input20_script;

EXPLAIN
FROM (
  FROM src
  MAP src.key, src.key 
  USING 'cat'
  DISTRIBUTE BY key
  SORT BY key, value
) tmap
INSERT OVERWRITE TABLE dest1
REDUCE tmap.key, tmap.value
USING 'input20_script'
AS key, value;

FROM (
  FROM src
  MAP src.key, src.key
  USING 'cat' 
  DISTRIBUTE BY key
  SORT BY key, value
) tmap
INSERT OVERWRITE TABLE dest1
REDUCE tmap.key, tmap.value
USING 'input20_script'
AS key, value;

SELECT * FROM dest1 SORT BY key, value;
