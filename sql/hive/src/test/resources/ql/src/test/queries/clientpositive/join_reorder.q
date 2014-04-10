



CREATE TABLE T1(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T2(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T3(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../data/files/T1.txt' INTO TABLE T1;
LOAD DATA LOCAL INPATH '../data/files/T2.txt' INTO TABLE T2;
LOAD DATA LOCAL INPATH '../data/files/T3.txt' INTO TABLE T3;

EXPLAIN FROM T1 a JOIN src c ON c.key+1=a.key
SELECT a.key, a.val, c.key;

EXPLAIN FROM T1 a JOIN src c ON c.key+1=a.key
SELECT /*+ STREAMTABLE(a) */ a.key, a.val, c.key;

FROM T1 a JOIN src c ON c.key+1=a.key
SELECT a.key, a.val, c.key;

FROM T1 a JOIN src c ON c.key+1=a.key
SELECT /*+ STREAMTABLE(a) */ a.key, a.val, c.key;

EXPLAIN FROM T1 a
  LEFT OUTER JOIN T2 b ON (b.key=a.key)
  RIGHT OUTER JOIN T3 c ON (c.val = a.val)
SELECT a.key, b.key, a.val, c.val;

EXPLAIN FROM T1 a
  LEFT OUTER JOIN T2 b ON (b.key=a.key)
  RIGHT OUTER JOIN T3 c ON (c.val = a.val)
SELECT /*+ STREAMTABLE(a) */ a.key, b.key, a.val, c.val;

FROM T1 a
  LEFT OUTER JOIN T2 b ON (b.key=a.key)
  RIGHT OUTER JOIN T3 c ON (c.val = a.val)
SELECT a.key, b.key, a.val, c.val;

FROM T1 a
  LEFT OUTER JOIN T2 b ON (b.key=a.key)
  RIGHT OUTER JOIN T3 c ON (c.val = a.val)
SELECT /*+ STREAMTABLE(a) */ a.key, b.key, a.val, c.val;

EXPLAIN FROM UNIQUEJOIN
  PRESERVE T1 a (a.key, a.val),
  PRESERVE T2 b (b.key, b.val),
  PRESERVE T3 c (c.key, c.val)
SELECT a.key, b.key, c.key;

EXPLAIN FROM UNIQUEJOIN
  PRESERVE T1 a (a.key, a.val),
  PRESERVE T2 b (b.key, b.val),
  PRESERVE T3 c (c.key, c.val)
SELECT /*+ STREAMTABLE(b) */ a.key, b.key, c.key;

FROM UNIQUEJOIN
  PRESERVE T1 a (a.key, a.val),
  PRESERVE T2 b (b.key, b.val),
  PRESERVE T3 c (c.key, c.val)
SELECT a.key, b.key, c.key;

FROM UNIQUEJOIN
  PRESERVE T1 a (a.key, a.val),
  PRESERVE T2 b (b.key, b.val),
  PRESERVE T3 c (c.key, c.val)
SELECT /*+ STREAMTABLE(b) */ a.key, b.key, c.key;




