CREATE TABLE T1(a STRING, b STRING, c STRING);

-- Check for mupltiple empty grouping sets
SELECT * FROM T1 GROUP BY b GROUPING SETS ((), (), ());
