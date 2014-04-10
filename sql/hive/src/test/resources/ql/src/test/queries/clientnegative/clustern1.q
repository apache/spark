EXPLAIN
SELECT x.key, x.value as key FROM SRC x CLUSTER BY key;
