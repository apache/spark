set hive.optimize.ppd=true;

EXPLAIN EXTENDED
FROM (
  FROM srcpart src
  SELECT TRANSFORM(src.ds, src.key, src.value)
         USING 'cat' AS (ds, tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100 AND tmap.ds = '2008-04-08';

FROM (
  FROM srcpart src
  SELECT TRANSFORM(src.ds, src.key, src.value)
         USING 'cat' AS (ds, tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100 AND tmap.ds = '2008-04-08';

