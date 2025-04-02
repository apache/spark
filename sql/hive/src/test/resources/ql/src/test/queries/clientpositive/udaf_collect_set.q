DESCRIBE FUNCTION collect_set;
DESCRIBE FUNCTION EXTENDED collect_set;

DESCRIBE FUNCTION collect_list;
DESCRIBE FUNCTION EXTENDED collect_list;

set hive.map.aggr = false;
set hive.groupby.skewindata = false;

SELECT key, collect_set(value)
FROM src
GROUP BY key ORDER BY key limit 20;

SELECT key, collect_list(value)
FROM src
GROUP BY key ORDER by key limit 20;

set hive.map.aggr = true;
set hive.groupby.skewindata = false;

SELECT key, collect_set(value)
FROM src
GROUP BY key ORDER BY key limit 20;

SELECT key, collect_list(value)
FROM src
GROUP BY key ORDER BY key limit 20;

set hive.map.aggr = false;
set hive.groupby.skewindata = true;

SELECT key, collect_set(value)
FROM src
GROUP BY key ORDER BY key limit 20;

set hive.map.aggr = true;
set hive.groupby.skewindata = true;

SELECT key, collect_set(value)
FROM src
GROUP BY key ORDER BY key limit 20;
