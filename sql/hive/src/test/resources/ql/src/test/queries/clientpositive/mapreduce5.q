CREATE TABLE dest1(key INT, ten INT, one INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1
SELECT src.key as c1, CAST(src.key / 10 AS INT) as c2, CAST(src.key % 10 AS INT) as c3, src.value as c4
DISTRIBUTE BY c4, c1
SORT BY c2 DESC, c3 ASC;


FROM src
INSERT OVERWRITE TABLE dest1
SELECT src.key as c1, CAST(src.key / 10 AS INT) as c2, CAST(src.key % 10 AS INT) as c3, src.value as c4
DISTRIBUTE BY c4, c1
SORT BY c2 DESC, c3 ASC;

SELECT dest1.* FROM dest1;
