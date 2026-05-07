--ONLY_IF spark

CREATE TABLE t1(col1 INT, col2 STRING);
CREATE TABLE t2(col1 INT);
CREATE TABLE t3(col1 INT, col2 STRING);

-- Subquery expressions validation is done at the end of operator resolution
SELECT *
FROM (
    SELECT (
        SELECT t1.col1
        FROM t1
        WHERE t3.col1 = t1.col2
        LIMIT 1
    )
    FROM t3
    GROUP BY (
        SELECT t1.col1
        FROM t1
        WHERE t3.col1 = t1.col2
        LIMIT 1
    )
);
SELECT *
FROM (
    SELECT 1 IN (
        SELECT t1.col1
        FROM t1
        WHERE t3.col1 = t1.col2
        LIMIT 1
    )
    FROM t3
    GROUP BY 1 IN (
        SELECT t1.col1
        FROM t1
        WHERE t3.col1 = t1.col2
        LIMIT 1
    )
);
SELECT *
FROM (
    SELECT EXISTS (
        SELECT t1.col1
        FROM t1
        WHERE t3.col1 = t1.col2
        LIMIT 1
    )
    FROM t3
    GROUP BY EXISTS (
        SELECT t1.col1
        FROM t1
        WHERE t3.col1 = t1.col2
        LIMIT 1
    )
);

SELECT col1 IN (SELECT col1 FROM t2)
FROM t2
GROUP BY col1 IN (SELECT col1 FROM t2), col1
ORDER BY col1 IN (SELECT col1 FROM t2);

SELECT col1 FROM t2 GROUP BY col1 IN (SELECT col1 FROM t2), col1 ORDER BY col1 IN (SELECT col1 FROM t2);

SELECT col1 FROM t2 GROUP BY col1 ORDER BY 1 IN (SELECT 1);

SELECT col1 AS a, a + 1 FROM t2 GROUP BY col1 ORDER BY 1 IN (SELECT 1);

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
