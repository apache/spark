CREATE TABLE T1(key INT, value INT) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/groupby_groupingid.txt' INTO TABLE T1;

set hive.groupby.skewindata = true;

SELECT key, value, GROUPING__ID, count(*) from T1 GROUP BY key, value WITH ROLLUP;

SELECT GROUPING__ID, count(*)
FROM
(
SELECT key, value, GROUPING__ID, count(*) from T1 GROUP BY key, value WITH ROLLUP
) t 
GROUP BY GROUPING__ID;

SELECT t1.GROUPING__ID, t2.GROUPING__ID FROM (SELECT GROUPING__ID FROM T1  GROUP BY key,value WITH ROLLUP) t1
JOIN 
(SELECT GROUPING__ID FROM T1 GROUP BY key, value WITH ROLLUP) t2
ON t1.GROUPING__ID = t2.GROUPING__ID;





set hive.groupby.skewindata = false;

SELECT key, value, GROUPING__ID, count(*) from T1 GROUP BY key, value WITH ROLLUP;

SELECT GROUPING__ID, count(*)
FROM
(
SELECT key, value, GROUPING__ID, count(*) from T1 GROUP BY key, value WITH ROLLUP
) t 
GROUP BY GROUPING__ID;

SELECT t1.GROUPING__ID, t2.GROUPING__ID FROM (SELECT GROUPING__ID FROM T1  GROUP BY key,value WITH ROLLUP) t1
JOIN 
(SELECT GROUPING__ID FROM T1 GROUP BY key, value WITH ROLLUP) t2
ON t1.GROUPING__ID = t2.GROUPING__ID;


