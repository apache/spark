set hive.auto.convert.join = true;

CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

explain
FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src2.value where src1.ds = '2008-04-08' and src1.hr = '12';

FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src2.value where src1.ds = '2008-04-08' and src1.hr = '12';



SELECT sum(hash(dest1.key,dest1.value)) FROM dest1;
