set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

EXPLAIN
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

-- test described in HIVE-4598

EXPLAIN
FROM (
    FROM ( SELECT * FROM src ) mapout REDUCE * USING 'cat' AS x,y
) reduced
insert overwrite local directory '/tmp/a' select * where x='a' or x='b'
insert overwrite local directory '/tmp/b' select * where x='c' or x='d';
