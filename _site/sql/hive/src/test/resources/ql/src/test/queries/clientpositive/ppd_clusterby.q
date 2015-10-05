set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

EXPLAIN
SELECT * FROM SRC x where x.key = 10 CLUSTER BY x.key;
SELECT * FROM SRC x where x.key = 10 CLUSTER BY x.key;

EXPLAIN 
SELECT x.key, x.value as v1, y.key  FROM SRC x JOIN SRC y ON (x.key = y.key)  where x.key = 20 CLUSTER BY v1;;
SELECT x.key, x.value as v1, y.key  FROM SRC x JOIN SRC y ON (x.key = y.key) where x.key = 20 CLUSTER BY v1;

set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
SELECT * FROM SRC x where x.key = 10 CLUSTER BY x.key;
SELECT * FROM SRC x where x.key = 10 CLUSTER BY x.key;

EXPLAIN 
SELECT x.key, x.value as v1, y.key  FROM SRC x JOIN SRC y ON (x.key = y.key)  where x.key = 20 CLUSTER BY v1;;
SELECT x.key, x.value as v1, y.key  FROM SRC x JOIN SRC y ON (x.key = y.key) where x.key = 20 CLUSTER BY v1;
