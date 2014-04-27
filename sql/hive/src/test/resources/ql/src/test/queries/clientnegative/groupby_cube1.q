set hive.map.aggr=false;

SELECT key, count(distinct value) FROM src GROUP BY key with cube;

