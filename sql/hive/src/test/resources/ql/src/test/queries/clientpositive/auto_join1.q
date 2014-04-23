set hive.auto.convert.join =true;

CREATE TABLE dest_j1(key INT, value STRING) STORED AS TEXTFILE;

explain
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1 SELECT src1.key, src2.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1 SELECT src1.key, src2.value;

SELECT sum(hash(dest_j1.key,dest_j1.value)) FROM dest_j1;