set hive.map.aggr=true;

SELECT key, value, count(key) FROM src GROUP BY key, value with rollup;

