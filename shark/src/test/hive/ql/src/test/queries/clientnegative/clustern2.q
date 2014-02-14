EXPLAIN
SELECT x.key, x.value as v1, y.*  FROM SRC x JOIN SRC y ON (x.key = y.key) CLUSTER BY key;

