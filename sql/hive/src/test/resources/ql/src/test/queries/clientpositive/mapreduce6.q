CREATE TABLE dest1(key INT, ten INT, one INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1
SELECT src.key, CAST(src.key / 10 AS INT) as c2, CAST(src.key % 10 AS INT) as c3, src.value
DISTRIBUTE BY value, key
SORT BY c2 DESC, c3 ASC;


FROM src
INSERT OVERWRITE TABLE dest1
SELECT src.key, CAST(src.key / 10 AS INT) as c2, CAST(src.key % 10 AS INT) as c3, src.value
DISTRIBUTE BY value, key
SORT BY c2 DESC, c3 ASC;

SELECT dest1.* FROM dest1;
