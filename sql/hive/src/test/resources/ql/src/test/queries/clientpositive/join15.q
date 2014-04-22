EXPLAIN
SELECT * FROM src src1 JOIN src src2 ON (src1.key = src2.key) SORT BY src1.key, src1.value, src2.key, src2.value;

SELECT * FROM src src1 JOIN src src2 ON (src1.key = src2.key) SORT BY src1.key, src1.value, src2.key, src2.value;
