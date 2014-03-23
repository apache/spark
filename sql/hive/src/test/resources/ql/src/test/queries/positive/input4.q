FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING '/bin/cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100
