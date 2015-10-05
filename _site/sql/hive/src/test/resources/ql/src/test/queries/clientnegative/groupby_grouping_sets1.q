CREATE TABLE T1(a STRING, b STRING, c STRING);

-- Check for empty grouping set
SELECT * FROM T1 GROUP BY a GROUPING SETS (());

