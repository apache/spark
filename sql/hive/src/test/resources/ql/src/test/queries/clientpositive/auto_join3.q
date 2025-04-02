set hive.auto.convert.join = true;

CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

explain
FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key = src3.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src3.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key = src3.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src3.value;

SELECT sum(hash(dest1.key,dest1.value)) FROM dest1;