set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
CREATE TABLE dest_j1(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1 SELECT src1.key, src2.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1 SELECT src1.key, src2.value;

SELECT dest_j1.* FROM dest_j1;
