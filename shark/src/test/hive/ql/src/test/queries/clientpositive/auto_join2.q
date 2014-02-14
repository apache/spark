set hive.auto.convert.join = true;

CREATE TABLE dest_j2(key INT, value STRING) STORED AS TEXTFILE;

explain
FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key + src2.key = src3.key)
INSERT OVERWRITE TABLE dest_j2 SELECT src1.key, src3.value;


FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key + src2.key = src3.key)
INSERT OVERWRITE TABLE dest_j2 SELECT src1.key, src3.value;

SELECT sum(hash(dest_j2.key,dest_j2.value)) FROM dest_j2;
