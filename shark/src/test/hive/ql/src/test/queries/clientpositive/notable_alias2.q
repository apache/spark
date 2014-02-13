CREATE TABLE dest1(dummy STRING, key INT, value DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT '1234', src.key, count(1) WHERE key < 100 group by src.key;

FROM src
INSERT OVERWRITE TABLE dest1 SELECT '1234', src.key, count(1) WHERE key < 100 group by src.key;

SELECT dest1.* FROM dest1;
