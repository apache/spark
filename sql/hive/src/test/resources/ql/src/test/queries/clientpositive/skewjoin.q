set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 2;







CREATE TABLE T1(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T2(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T3(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T4(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE dest_j1(key INT, value STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;
LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2;
LOAD DATA LOCAL INPATH '../../data/files/T3.txt' INTO TABLE T3;
LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T4;


EXPLAIN
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1 SELECT src1.key, src2.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1 SELECT src1.key, src2.value;

SELECT sum(hash(key)), sum(hash(value)) FROM dest_j1;


EXPLAIN
SELECT /*+ STREAMTABLE(a) */ *
FROM T1 a JOIN T2 b ON a.key = b.key
          JOIN T3 c ON b.key = c.key
          JOIN T4 d ON c.key = d.key;

SELECT /*+ STREAMTABLE(a) */ *
FROM T1 a JOIN T2 b ON a.key = b.key
          JOIN T3 c ON b.key = c.key
          JOIN T4 d ON c.key = d.key;

EXPLAIN
SELECT /*+ STREAMTABLE(a,c) */ *
FROM T1 a JOIN T2 b ON a.key = b.key
          JOIN T3 c ON b.key = c.key
          JOIN T4 d ON c.key = d.key;

SELECT /*+ STREAMTABLE(a,c) */ *
FROM T1 a JOIN T2 b ON a.key = b.key
          JOIN T3 c ON b.key = c.key
          JOIN T4 d ON c.key = d.key;


EXPLAIN FROM T1 a JOIN src c ON c.key+1=a.key SELECT /*+ STREAMTABLE(a) */ sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));
FROM T1 a JOIN src c ON c.key+1=a.key SELECT /*+ STREAMTABLE(a) */ sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));

EXPLAIN FROM 
(SELECT src.* FROM src) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key)
SELECT sum(hash(Y.key)), sum(hash(Y.value));

FROM 
(SELECT src.* FROM src) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key)
SELECT sum(hash(Y.key)), sum(hash(Y.value));


EXPLAIN FROM 
(SELECT src.* FROM src) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key and substring(x.value, 5)=substring(y.value, 5)+1)
SELECT sum(hash(Y.key)), sum(hash(Y.value));

FROM 
(SELECT src.* FROM src) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key and substring(x.value, 5)=substring(y.value, 5)+1)
SELECT sum(hash(Y.key)), sum(hash(Y.value));


EXPLAIN
SELECT sum(hash(src1.c1)), sum(hash(src2.c4)) 
FROM
(SELECT src.key as c1, src.value as c2 from src) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src) src2
ON src1.c1 = src2.c3 AND src1.c1 < 100
JOIN
(SELECT src.key as c5, src.value as c6 from src) src3
ON src1.c1 = src3.c5 AND src3.c5 < 80;

SELECT sum(hash(src1.c1)), sum(hash(src2.c4))
FROM
(SELECT src.key as c1, src.value as c2 from src) src1
JOIN
(SELECT src.key as c3, src.value as c4 from src) src2
ON src1.c1 = src2.c3 AND src1.c1 < 100
JOIN
(SELECT src.key as c5, src.value as c6 from src) src3
ON src1.c1 = src3.c5 AND src3.c5 < 80;

EXPLAIN
SELECT /*+ mapjoin(v)*/ sum(hash(k.key)), sum(hash(v.val)) FROM T1 k LEFT OUTER JOIN T1 v ON k.key+1=v.key;
SELECT /*+ mapjoin(v)*/ sum(hash(k.key)), sum(hash(v.val)) FROM T1 k LEFT OUTER JOIN T1 v ON k.key+1=v.key;

select /*+ mapjoin(k)*/ sum(hash(k.key)), sum(hash(v.val)) from T1 k join T1 v on k.key=v.val;

select /*+ mapjoin(k)*/ sum(hash(k.key)), sum(hash(v.val)) from T1 k join T1 v on k.key=v.key;

select sum(hash(k.key)), sum(hash(v.val)) from T1 k join T1 v on k.key=v.key;

select count(1) from  T1 a join T1 b on a.key = b.key;

FROM T1 a LEFT OUTER JOIN T2 c ON c.key+1=a.key SELECT sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));

FROM T1 a RIGHT OUTER JOIN T2 c ON c.key+1=a.key SELECT /*+ STREAMTABLE(a) */ sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));

FROM T1 a FULL OUTER JOIN T2 c ON c.key+1=a.key SELECT /*+ STREAMTABLE(a) */ sum(hash(a.key)), sum(hash(a.val)), sum(hash(c.key));

SELECT sum(hash(src1.key)), sum(hash(src1.val)), sum(hash(src2.key)) FROM T1 src1 LEFT OUTER JOIN T2 src2 ON src1.key+1 = src2.key RIGHT OUTER JOIN T2 src3 ON src2.key = src3.key;

SELECT sum(hash(src1.key)), sum(hash(src1.val)), sum(hash(src2.key)) FROM T1 src1 JOIN T2 src2 ON src1.key+1 = src2.key JOIN T2 src3 ON src2.key = src3.key;

select /*+ mapjoin(v)*/ sum(hash(k.key)), sum(hash(v.val)) from T1 k left outer join T1 v on k.key+1=v.key;






