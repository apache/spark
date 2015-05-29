set hive.auto.convert.join = false;
--HIVE-2101 mapjoin sometimes gives wrong results if there is a filter in the on condition

SELECT * FROM src1
  RIGHT OUTER JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key > 10)
  JOIN src src3 ON (src2.key = src3.key AND src3.key < 300)
  SORT BY src1.key, src2.key, src3.key;

explain
SELECT /*+ mapjoin(src1, src2) */ * FROM src1
  RIGHT OUTER JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key > 10)
  JOIN src src3 ON (src2.key = src3.key AND src3.key < 300)
  SORT BY src1.key, src2.key, src3.key;

SELECT /*+ mapjoin(src1, src2) */ * FROM src1
  RIGHT OUTER JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key > 10)
  JOIN src src3 ON (src2.key = src3.key AND src3.key < 300)
  SORT BY src1.key, src2.key, src3.key;

set hive.auto.convert.join = true;

explain
SELECT * FROM src1
  RIGHT OUTER JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key > 10)
  JOIN src src3 ON (src2.key = src3.key AND src3.key < 300)
  SORT BY src1.key, src2.key, src3.key;

SELECT * FROM src1
  RIGHT OUTER JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key > 10)
  JOIN src src3 ON (src2.key = src3.key AND src3.key < 300)
  SORT BY src1.key, src2.key, src3.key;
