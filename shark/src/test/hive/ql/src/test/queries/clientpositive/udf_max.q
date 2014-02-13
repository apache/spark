DESCRIBE FUNCTION max;
DESCRIBE FUNCTION EXTENDED max;


set hive.map.aggr = false;
set hive.groupby.skewindata = false;

SELECT max(struct(CAST(key as INT), value)),
       max(struct(key, value))
FROM src;


set hive.map.aggr = true;
set hive.groupby.skewindata = false;

SELECT max(struct(CAST(key as INT), value)),
       max(struct(key, value))
FROM src;


set hive.map.aggr = false;
set hive.groupby.skewindata = true;

SELECT max(struct(CAST(key as INT), value)),
       max(struct(key, value))
FROM src;


set hive.map.aggr = true;
set hive.groupby.skewindata = true;

SELECT max(struct(CAST(key as INT), value)),
       max(struct(key, value))
FROM src;
