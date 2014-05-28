EXPLAIN SELECT count(value) AS c FROM src GROUP BY key HAVING c > 3;
SELECT count(value) AS c FROM src GROUP BY key HAVING c > 3;

EXPLAIN SELECT key, max(value) AS c FROM src GROUP BY key HAVING key != 302;
SELECT key, max(value) AS c FROM src GROUP BY key HAVING key != 302;

EXPLAIN SELECT key FROM src GROUP BY key HAVING max(value) > "val_255";
SELECT key FROM src GROUP BY key HAVING max(value) > "val_255";

EXPLAIN SELECT key FROM src where key > 300 GROUP BY key HAVING max(value) > "val_255";
SELECT key FROM src where key > 300 GROUP BY key HAVING max(value) > "val_255";

EXPLAIN SELECT key, max(value) FROM src GROUP BY key HAVING max(value) > "val_255";
SELECT key, max(value) FROM src GROUP BY key HAVING max(value) > "val_255";
