-- Subquery expressions validation is done at the end of operator resolution

CREATE TEMPORARY VIEW t2_subq(col1 INT, col2 STRING) AS VALUES (1, 'a'), (2, 'b');
CREATE TEMPORARY VIEW t3_subq(col1 INT) AS VALUES (1), (2), (3);
CREATE TEMPORARY VIEW t4_subq(col1 INT, col2 STRING) AS VALUES (1, 'x'), (2, 'y');

SELECT *
FROM (
    SELECT (
        SELECT t2_subq.col1
        FROM t2_subq
        WHERE t4_subq.col1 = t2_subq.col2
        LIMIT 1
    )
    FROM t4_subq
    GROUP BY (
        SELECT t2_subq.col1
        FROM t2_subq
        WHERE t4_subq.col1 = t2_subq.col2
        LIMIT 1
    )
);

SELECT *
FROM (
    SELECT 1 IN (
        SELECT t2_subq.col1
        FROM t2_subq
        WHERE t4_subq.col1 = t2_subq.col2
        LIMIT 1
    )
    FROM t4_subq
    GROUP BY 1 IN (
        SELECT t2_subq.col1
        FROM t2_subq
        WHERE t4_subq.col1 = t2_subq.col2
        LIMIT 1
    )
);

SELECT *
FROM (
    SELECT EXISTS (
        SELECT t2_subq.col1
        FROM t2_subq
        WHERE t4_subq.col1 = t2_subq.col2
        LIMIT 1
    )
    FROM t4_subq
    GROUP BY EXISTS (
        SELECT t2_subq.col1
        FROM t2_subq
        WHERE t4_subq.col1 = t2_subq.col2
        LIMIT 1
    )
);

SELECT col1 IN (SELECT col1 FROM t3_subq)
FROM t3_subq
GROUP BY col1 IN (SELECT col1 FROM t3_subq), col1
ORDER BY col1 IN (SELECT col1 FROM t3_subq);

SELECT col1 FROM t3_subq GROUP BY col1 IN (SELECT col1 FROM t3_subq), col1 ORDER BY col1 IN (SELECT col1 FROM t3_subq);
SELECT col1 FROM t3_subq GROUP BY col1 ORDER BY 1 IN (SELECT 1);
SELECT col1 AS a, a + 1 FROM t3_subq GROUP BY col1 ORDER BY 1 IN (SELECT 1);

-- Nested subqueries with redundant aliases in the innermost subquery.
-- Tests whether we run PlanRewriter rules on the nested subqueries recursively.
SELECT (
    SELECT (
        SELECT LOWER('str':col)
    )
);

SELECT (
    SELECT 'a' IN (
        SELECT LOWER('str':col)
    )
);

SELECT (
    SELECT EXISTS (
        SELECT LCASE('str':col)
    )
);

DROP VIEW t2_subq;
DROP VIEW t3_subq;
DROP VIEW t4_subq;
