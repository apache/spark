EXPLAIN
SELECT * FROM SRC x where x.key = 10 CLUSTER BY x.key;
SELECT * FROM SRC x where x.key = 10 CLUSTER BY x.key;

EXPLAIN
SELECT * FROM SRC x  where x.key = 20 CLUSTER BY key ;
SELECT * FROM SRC x where x.key = 20 CLUSTER BY key ;

EXPLAIN
SELECT x.* FROM SRC x where x.key = 20 CLUSTER BY key;
SELECT x.* FROM SRC x where x.key = 20 CLUSTER BY key;

EXPLAIN
SELECT x.*  FROM SRC x where x.key = 20 CLUSTER BY x.key;
SELECT x.*  FROM SRC x where x.key = 20 CLUSTER BY x.key;

EXPLAIN
SELECT x.key, x.value as v1 FROM SRC x where x.key = 20 CLUSTER BY key ;
SELECT x.key, x.value as v1 FROM SRC x where x.key = 20 CLUSTER BY key ;

EXPLAIN
SELECT x.key, x.value as v1 FROM SRC x where x.key = 20 CLUSTER BY x.key;
SELECT x.key, x.value as v1 FROM SRC x where x.key = 20 CLUSTER BY x.key;

EXPLAIN
SELECT x.key, x.value as v1  FROM SRC x where x.key = 20 CLUSTER BY v1;
SELECT x.key, x.value as v1  FROM SRC x where x.key = 20 CLUSTER BY v1;

EXPLAIN
SELECT y.* from (SELECT x.* FROM SRC x CLUSTER BY x.key) y where y.key = 20;
SELECT y.* from (SELECT x.* FROM SRC x CLUSTER BY x.key) y where y.key = 20;


EXPLAIN 
SELECT x.key, x.value as v1, y.key  FROM SRC x JOIN SRC y ON (x.key = y.key)  where x.key = 20 CLUSTER BY v1;;
SELECT x.key, x.value as v1, y.key  FROM SRC x JOIN SRC y ON (x.key = y.key) where x.key = 20 CLUSTER BY v1;

EXPLAIN 
SELECT x.key, x.value as v1, y.*  FROM SRC x JOIN SRC y ON (x.key = y.key) where x.key = 20 CLUSTER BY v1;
SELECT x.key, x.value as v1, y.*  FROM SRC x JOIN SRC y ON (x.key = y.key) where x.key = 20 CLUSTER BY v1;

EXPLAIN
SELECT x.key, x.value as v1, y.*  FROM SRC x JOIN SRC y ON (x.key = y.key) where x.key = 20 CLUSTER BY x.key ;
SELECT x.key, x.value as v1, y.*  FROM SRC x JOIN SRC y ON (x.key = y.key) where x.key = 20 CLUSTER BY x.key ;

EXPLAIN
SELECT x.key, x.value as v1, y.key as yk  FROM SRC x JOIN SRC y ON (x.key = y.key) where x.key = 20 CLUSTER BY key ;
SELECT x.key, x.value as v1, y.key as yk  FROM SRC x JOIN SRC y ON (x.key = y.key) where x.key = 20 CLUSTER BY key ;

EXPLAIN
SELECT unioninput.*
FROM (
  FROM src select src.key, src.value WHERE src.key < 100
  UNION ALL
  FROM src SELECT src.* WHERE src.key > 100
) unioninput
CLUSTER BY unioninput.key;

SELECT unioninput.*
FROM (
  FROM src select src.key, src.value WHERE src.key < 100
  UNION ALL
  FROM src SELECT src.* WHERE src.key > 100
) unioninput
CLUSTER BY unioninput.key;
