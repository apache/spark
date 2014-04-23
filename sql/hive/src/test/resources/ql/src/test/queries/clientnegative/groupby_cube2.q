set hive.map.aggr=true;

SELECT key, value, count(distinct value) FROM src GROUP BY key, value with cube;

