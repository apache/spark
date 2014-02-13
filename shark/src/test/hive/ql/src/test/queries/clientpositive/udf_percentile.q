DESCRIBE FUNCTION percentile;
DESCRIBE FUNCTION EXTENDED percentile;


set hive.map.aggr = false;
set hive.groupby.skewindata = false;

SELECT CAST(key AS INT) DIV 10,
       percentile(CAST(substr(value, 5) AS INT), 0.0),
       percentile(CAST(substr(value, 5) AS INT), 0.5),
       percentile(CAST(substr(value, 5) AS INT), 1.0),
       percentile(CAST(substr(value, 5) AS INT), array(0.0, 0.5, 0.99, 1.0))
FROM src
GROUP BY CAST(key AS INT) DIV 10;


set hive.map.aggr = true;
set hive.groupby.skewindata = false;

SELECT CAST(key AS INT) DIV 10,
       percentile(CAST(substr(value, 5) AS INT), 0.0),
       percentile(CAST(substr(value, 5) AS INT), 0.5),
       percentile(CAST(substr(value, 5) AS INT), 1.0),
       percentile(CAST(substr(value, 5) AS INT), array(0.0, 0.5, 0.99, 1.0))
FROM src
GROUP BY CAST(key AS INT) DIV 10;



set hive.map.aggr = false;
set hive.groupby.skewindata = true;

SELECT CAST(key AS INT) DIV 10,
       percentile(CAST(substr(value, 5) AS INT), 0.0),
       percentile(CAST(substr(value, 5) AS INT), 0.5),
       percentile(CAST(substr(value, 5) AS INT), 1.0),
       percentile(CAST(substr(value, 5) AS INT), array(0.0, 0.5, 0.99, 1.0))
FROM src
GROUP BY CAST(key AS INT) DIV 10;


set hive.map.aggr = true;
set hive.groupby.skewindata = true;

SELECT CAST(key AS INT) DIV 10,
       percentile(CAST(substr(value, 5) AS INT), 0.0),
       percentile(CAST(substr(value, 5) AS INT), 0.5),
       percentile(CAST(substr(value, 5) AS INT), 1.0),
       percentile(CAST(substr(value, 5) AS INT), array(0.0, 0.5, 0.99, 1.0))
FROM src
GROUP BY CAST(key AS INT) DIV 10;


set hive.map.aggr = true;
set hive.groupby.skewindata = false;

-- test null handling
SELECT CAST(key AS INT) DIV 10,
       percentile(NULL, 0.0),
       percentile(NULL, array(0.0, 0.5, 0.99, 1.0))
FROM src
GROUP BY CAST(key AS INT) DIV 10;


-- test empty array handling
SELECT CAST(key AS INT) DIV 10,
       percentile(IF(CAST(key AS INT) DIV 10 < 5, 1, NULL), 0.5),
       percentile(IF(CAST(key AS INT) DIV 10 < 5, 1, NULL), array(0.0, 0.5, 0.99, 1.0))
FROM src
GROUP BY CAST(key AS INT) DIV 10;

select percentile(cast(key as bigint), 0.5) from src where false;

-- test where percentile list is empty
select percentile(cast(key as bigint), array()) from src where false;
