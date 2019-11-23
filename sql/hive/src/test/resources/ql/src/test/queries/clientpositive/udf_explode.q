set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION explode;
DESCRIBE FUNCTION EXTENDED explode;

EXPLAIN EXTENDED SELECT explode(array(1,2,3)) AS myCol FROM src tablesample (1 rows);
EXPLAIN EXTENDED SELECT a.myCol, count(1) FROM (SELECT explode(array(1,2,3)) AS myCol FROM src tablesample (1 rows)) a GROUP BY a.myCol;

SELECT explode(array(1,2,3)) AS myCol FROM src tablesample (1 rows);
SELECT explode(array(1,2,3)) AS (myCol) FROM src tablesample (1 rows);
SELECT a.myCol, count(1) FROM (SELECT explode(array(1,2,3)) AS myCol FROM src tablesample (1 rows)) a GROUP BY a.myCol;

EXPLAIN EXTENDED SELECT explode(map(1,'one',2,'two',3,'three')) AS (key,val) FROM src tablesample (1 rows);
EXPLAIN EXTENDED SELECT a.key, a.val, count(1) FROM (SELECT explode(map(1,'one',2,'two',3,'three')) AS (key,val) FROM src tablesample (1 rows)) a GROUP BY a.key, a.val;

SELECT explode(map(1,'one',2,'two',3,'three')) AS (key,val) FROM src tablesample (1 rows);
SELECT a.key, a.val, count(1) FROM (SELECT explode(map(1,'one',2,'two',3,'three')) AS (key,val) FROM src tablesample (1 rows)) a GROUP BY a.key, a.val;

drop table lazy_array_map;
create table lazy_array_map (map_col map<int,string>, array_col array<string>);
INSERT OVERWRITE TABLE lazy_array_map select map(1,'one',2,'two',3,'three'), array('100','200','300') FROM src tablesample (1 rows);

SELECT array_col, myCol from lazy_array_map lateral view explode(array_col) X AS myCol;
SELECT map_col, myKey, myValue from lazy_array_map lateral view explode(map_col) X AS myKey, myValue;