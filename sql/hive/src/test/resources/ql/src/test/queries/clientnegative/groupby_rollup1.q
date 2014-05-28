set hive.map.aggr=false;

SELECT key, value, count(1) FROM src GROUP BY key, value with rollup;

