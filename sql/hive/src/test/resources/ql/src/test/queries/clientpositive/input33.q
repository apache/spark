CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

ADD FILE ../../data/scripts/input20_script.py;

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
USING 'python input20_script.py'
AS (key STRING, value STRING);

FROM (
  FROM src
  MAP src.key, src.key
  USING 'cat' 
  DISTRIBUTE BY key
  SORT BY key, value
) tmap
INSERT OVERWRITE TABLE dest1
REDUCE tmap.key, tmap.value
USING 'python input20_script.py'
AS (key STRING, value STRING);

SELECT * FROM dest1 SORT BY key, value;
