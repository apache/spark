DESCRIBE FUNCTION min;
DESCRIBE FUNCTION EXTENDED min;


set hive.map.aggr = false;
set hive.groupby.skewindata = false;

SELECT min(struct(CAST(key as INT), value)),
       min(struct(key, value))
FROM src;


set hive.map.aggr = true;
set hive.groupby.skewindata = false;

SELECT min(struct(CAST(key as INT), value)),
       min(struct(key, value))
FROM src;


set hive.map.aggr = false;
set hive.groupby.skewindata = true;

SELECT min(struct(CAST(key as INT), value)),
       min(struct(key, value))
FROM src;


set hive.map.aggr = true;
set hive.groupby.skewindata = true;

SELECT min(struct(CAST(key as INT), value)),
       min(struct(key, value))
FROM src;
