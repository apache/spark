EXPLAIN
SELECT x.key as k1, x.value FROM SRC x CLUSTER BY key;
