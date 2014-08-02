CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE dest2(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key, src.value WHERE src.key < 100 LIMIT 10
INSERT OVERWRITE TABLE dest2 SELECT src.key, src.value WHERE src.key < 100 LIMIT 5;

FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key, src.value WHERE src.key < 100 LIMIT 10
INSERT OVERWRITE TABLE dest2 SELECT src.key, src.value WHERE src.key < 100 LIMIT 5;

SELECT dest1.* FROM dest1 ORDER BY dest1.key ASC, dest1.value ASC;
SELECT dest2.* FROM dest2 ORDER BY dest2.key ASC, dest2.value ASC;




