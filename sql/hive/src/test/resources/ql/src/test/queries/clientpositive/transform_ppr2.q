set hive.optimize.ppd=true;

EXPLAIN EXTENDED
FROM (
  FROM srcpart src
  SELECT TRANSFORM(src.ds, src.key, src.value)
         USING 'cat' AS (ds, tkey, tvalue)
  WHERE src.ds = '2008-04-08' 
  CLUSTER BY tkey 
) tmap
SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

FROM (
  FROM srcpart src
  SELECT TRANSFORM(src.ds, src.key, src.value)
         USING 'cat' AS (ds, tkey, tvalue) 
  WHERE src.ds = '2008-04-08' 
  CLUSTER BY tkey 
) tmap
SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

