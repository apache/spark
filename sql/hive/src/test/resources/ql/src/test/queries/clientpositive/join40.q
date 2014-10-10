set hive.join.cache.size=1;

EXPLAIN SELECT x.key, x.value, y.key, y.value
FROM src x left outer JOIN (select * from src where key <= 100) y ON (x.key = y.key);

SELECT x.key, x.value, y.key, y.value
FROM src x left outer JOIN (select * from src where key <= 100) y ON (x.key = y.key);


EXPLAIN select src1.key, src2.value 
FROM src src1 JOIN src src2 ON (src1.key = src2.key);

select src1.key, src2.value 
FROM src src1 JOIN src src2 ON (src1.key = src2.key);


EXPLAIN
SELECT * FROM src src1 JOIN src src2 ON (src1.key = src2.key AND src1.key < 10) RIGHT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key < 20)
SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

SELECT * FROM src src1 JOIN src src2 ON (src1.key = src2.key AND src1.key < 10) RIGHT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key < 20)
SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;


EXPLAIN
SELECT * FROM src src1 JOIN src src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key < 15) RIGHT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key < 20)
SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

SELECT * FROM src src1 JOIN src src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key < 15) RIGHT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key < 20)
SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;


EXPLAIN 
SELECT /*+ MAPJOIN(y) */ x.key, x.value, y.key, y.value
FROM src x left outer JOIN (select * from src where key <= 100) y ON (x.key = y.key);

SELECT /*+ MAPJOIN(y) */ x.key, x.value, y.key, y.value
FROM src x left outer JOIN (select * from src where key <= 100) y ON (x.key = y.key);

EXPLAIN
SELECT COUNT(1) FROM SRC A JOIN SRC B ON (A.KEY=B.KEY);

SELECT COUNT(1) FROM SRC A JOIN SRC B ON (A.KEY=B.KEY);
