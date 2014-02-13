CREATE TABLE dest1(key STRING, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src1
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src1.value WHERE src1.key is null;

FROM src1
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src1.value WHERE src1.key is null;

SELECT dest1.* FROM dest1;
