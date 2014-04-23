set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

EXPLAIN
SELECT key, randum123
FROM (SELECT *, cast(rand() as double) AS randum123 FROM src WHERE key = 100) a
WHERE randum123 <=0.1;

EXPLAIN 
SELECT * FROM
(
SELECT key, randum123
FROM (SELECT *, cast(rand() as double) AS randum123 FROM src WHERE key = 100) a
WHERE randum123 <=0.1)s WHERE s.randum123>0.1 LIMIT 20;

EXPLAIN
SELECT key,randum123, h4
FROM (SELECT *, cast(rand() as double) AS randum123, hex(4) AS h4 FROM src WHERE key = 100) a
WHERE a.h4 <= 3;

EXPLAIN
SELECT key,randum123, v10
FROM (SELECT *, cast(rand() as double) AS randum123, value*10 AS v10 FROM src WHERE key = 100) a
WHERE a.v10 <= 200;

set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
SELECT key, randum123
FROM (SELECT *, cast(rand() as double) AS randum123 FROM src WHERE key = 100) a
WHERE randum123 <=0.1;

EXPLAIN 
SELECT * FROM
(
SELECT key, randum123
FROM (SELECT *, cast(rand() as double) AS randum123 FROM src WHERE key = 100) a
WHERE randum123 <=0.1)s WHERE s.randum123>0.1 LIMIT 20;

EXPLAIN
SELECT key,randum123, h4
FROM (SELECT *, cast(rand() as double) AS randum123, hex(4) AS h4 FROM src WHERE key = 100) a
WHERE a.h4 <= 3;

EXPLAIN
SELECT key,randum123, v10
FROM (SELECT *, cast(rand() as double) AS randum123, value*10 AS v10 FROM src WHERE key = 100) a
WHERE a.v10 <= 200;
