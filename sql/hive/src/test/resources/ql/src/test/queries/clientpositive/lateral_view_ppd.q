set hive.optimize.ppd=true;

EXPLAIN SELECT value, myCol FROM (SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol) a WHERE key='0';
SELECT value, myCol FROM (SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol) a WHERE key='0';

EXPLAIN SELECT value, myCol FROM (SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol) a WHERE key='0' AND myCol=1;
SELECT value, myCol FROM (SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol) a WHERE key='0' AND myCol=1;

EXPLAIN SELECT value, myCol FROM (SELECT * FROM srcpart LATERAL VIEW explode(array(1,2,3)) myTable AS myCol) a WHERE ds='2008-04-08' AND hr="12" LIMIT 12;
SELECT value, myCol FROM (SELECT * FROM srcpart LATERAL VIEW explode(array(1,2,3)) myTable AS myCol) a WHERE ds='2008-04-08' AND hr="12" LIMIT 12;

EXPLAIN SELECT value, myCol FROM (SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LATERAL VIEW explode(array(1,2,3)) myTable2 AS myCol2) a WHERE key='0';
SELECT value, myCol FROM (SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LATERAL VIEW explode(array(1,2,3)) myTable2 AS myCol2) a WHERE key='0';

-- HIVE-4293 Predicates following UDTF operator are removed by PPD
EXPLAIN SELECT value, myCol FROM (SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol WHERE myCol > 1) a WHERE key='0';
SELECT value, myCol FROM (SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol WHERE myCol > 1) a WHERE key='0';