set hive.auto.convert.join = true;
explain
SELECT sum(hash(src5.src1_value)) FROM (SELECT src3.*, src4.value as src4_value, src4.key as src4_key FROM src src4 JOIN (SELECT src2.*, src1.key as src1_key, src1.value as src1_value FROM src src1 JOIN src src2 ON src1.key = src2.key) src3 ON src3.src1_key = src4.key) src5;

SELECT sum(hash(src5.src1_value)) FROM (SELECT src3.*, src4.value as src4_value, src4.key as src4_key FROM src src4 JOIN (SELECT src2.*, src1.key as src1_key, src1.value as src1_value FROM src src1 JOIN src src2 ON src1.key = src2.key) src3 ON src3.src1_key = src4.key) src5;
