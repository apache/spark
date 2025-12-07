CREATE TABLE t1(col1 TIMESTAMP, col2 STRING);
CREATE TABLE t2(col1 INT, col2 STRING);

-- Duplicates in UNION under CTE
WITH cte AS (
    SELECT col1, col1 FROM t1
    UNION
    SELECT col1, col1 FROM t1
)
SELECT col1 FROM cte;

WITH cte AS (
    SELECT col2, from_utc_timestamp(col1, 'unknown'), col2 FROM t1
    UNION ALL
    SELECT col2, from_utc_timestamp(col1, 'unknown'), col2 FROM t1
)
SELECT * FROM cte;

-- UNION with outer references in a subquery
SELECT col1 FROM t3 WHERE (col1, col1) IN (SELECT col1, col1 UNION SELECT col1, col1);

-- SetOperationLike output deduplication across different branches should be done only in context of UNION
SELECT col1, TRIM(col2), col1 FROM t2 UNION SELECT col1, col2, col1 FROM t2;
SELECT col1, TRIM(col2), col1 FROM t2 MINUS SELECT col1, col2, col1 FROM t2;
SELECT col1, LTRIM(col2), col1 FROM t2 MINUS SELECT col1, col2, col1 FROM t2;
SELECT col1, RTRIM(col2), col1 FROM t2 EXCEPT SELECT col1, col2, col1 FROM t2;
SELECT col1, LOWER(col2), col1 FROM t2 INTERSECT SELECT col1, col2, col1 FROM t2;

DROP TABLE t1;
DROP TABLE t2;
