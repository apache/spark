CREATE TABLE dest_j1(key STRING, value STRING, val2 INT) STORED AS TEXTFILE;

-- Mapjoin followed by union is not supported.
-- The same query would work without the hint
-- Note that there is a positive test with the same name in clientpositive
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest_j1
SELECT /*+ MAPJOIN(x) */ x.key, x.value, subq1.cnt
FROM 
( SELECT x.key as key, count(1) as cnt from src x where x.key < 20 group by x.key
     UNION ALL
  SELECT x1.key as key, count(1) as cnt from src x1 where x1.key > 100 group by x1.key
) subq1
JOIN src1 x ON (x.key = subq1.key);




