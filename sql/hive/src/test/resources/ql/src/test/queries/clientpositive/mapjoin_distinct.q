set hive.map.aggr = true;
set hive.groupby.skewindata = true;
explain
FROM srcpart c
JOIN srcpart d
ON ( c.key=d.key AND c.ds='2008-04-08' AND d.ds='2008-04-08')
SELECT /*+ MAPJOIN(d) */ DISTINCT c.value;

FROM srcpart c
JOIN srcpart d
ON ( c.key=d.key AND c.ds='2008-04-08' AND d.ds='2008-04-08')
SELECT /*+ MAPJOIN(d) */ DISTINCT c.value as value order by value limit 10;

set hive.map.aggr = true;
set hive.groupby.skewindata = false;
explain
FROM srcpart c
JOIN srcpart d
ON ( c.key=d.key AND c.ds='2008-04-08' AND d.ds='2008-04-08')
SELECT /*+ MAPJOIN(d) */ DISTINCT c.value;

FROM srcpart c
JOIN srcpart d
ON ( c.key=d.key AND c.ds='2008-04-08' AND d.ds='2008-04-08')
SELECT /*+ MAPJOIN(d) */ DISTINCT c.value as value order by value limit 10;


set hive.map.aggr = false;
set hive.groupby.skewindata = true;
explain
FROM srcpart c
JOIN srcpart d
ON ( c.key=d.key AND c.ds='2008-04-08' AND d.ds='2008-04-08')
SELECT /*+ MAPJOIN(d) */ DISTINCT c.value;

FROM srcpart c
JOIN srcpart d
ON ( c.key=d.key AND c.ds='2008-04-08' AND d.ds='2008-04-08')
SELECT /*+ MAPJOIN(d) */ DISTINCT c.value as value order by value limit 10;


set hive.map.aggr = false;
set hive.groupby.skewindata = false;
explain
FROM srcpart c
JOIN srcpart d
ON ( c.key=d.key AND c.ds='2008-04-08' AND d.ds='2008-04-08')
SELECT /*+ MAPJOIN(d) */ DISTINCT c.value;

FROM srcpart c
JOIN srcpart d
ON ( c.key=d.key AND c.ds='2008-04-08' AND d.ds='2008-04-08')
SELECT /*+ MAPJOIN(d) */ DISTINCT c.value as value order by value limit 10;


