CREATE TABLE dest1(key1 INT, value1 STRING, key2 INT, value2 STRING) STORED AS TEXTFILE;

EXPLAIN EXTENDED
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.*, src2.*;

FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.*, src2.*;

SELECT dest1.* FROM dest1;
