CREATE TABLE dest1(dummy STRING, key INT, value DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT '1234', key, count(1) WHERE src.key < 100 group by key;

FROM src
INSERT OVERWRITE TABLE dest1 SELECT '1234', key, count(1) WHERE src.key < 100 group by key;

SELECT dest1.* FROM dest1;
