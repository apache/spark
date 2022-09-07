--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- SELECT_HAVING
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/select_having.sql
--
-- This test file was converted from inputs/postgreSQL/select_having.sql
-- TODO: We should add UDFs in GROUP BY clause when [SPARK-28386] and [SPARK-26741] is resolved.

-- load test data
CREATE TABLE test_having (a int, b int, c string, d string) USING parquet;
INSERT INTO test_having VALUES (0, 1, 'XXXX', 'A');
INSERT INTO test_having VALUES (1, 2, 'AAAA', 'b');
INSERT INTO test_having VALUES (2, 2, 'AAAA', 'c');
INSERT INTO test_having VALUES (3, 3, 'BBBB', 'D');
INSERT INTO test_having VALUES (4, 3, 'BBBB', 'e');
INSERT INTO test_having VALUES (5, 3, 'bbbb', 'F');
INSERT INTO test_having VALUES (6, 4, 'cccc', 'g');
INSERT INTO test_having VALUES (7, 4, 'cccc', 'h');
INSERT INTO test_having VALUES (8, 4, 'CCCC', 'I');
INSERT INTO test_having VALUES (9, 4, 'CCCC', 'j');

SELECT udf(b), udf(c) FROM test_having
	GROUP BY b, c HAVING udf(count(*)) = 1 ORDER BY udf(b), udf(c);

-- HAVING is effectively equivalent to WHERE in this case
SELECT udf(b), udf(c) FROM test_having
	GROUP BY b, c HAVING udf(b) = 3 ORDER BY udf(b), udf(c);

-- [SPARK-28386] Cannot resolve ORDER BY columns with GROUP BY and HAVING
-- SELECT lower(c), count(c) FROM test_having
-- 	GROUP BY lower(c) HAVING count(*) > 2 OR min(a) = max(a)
-- 	ORDER BY lower(c);

SELECT udf(c), max(udf(a)) FROM test_having
	GROUP BY c HAVING udf(count(*)) > 2 OR udf(min(a)) = udf(max(a))
	ORDER BY c;

-- test degenerate cases involving HAVING without GROUP BY
-- Per SQL spec, these should generate 0 or 1 row, even without aggregates

SELECT udf(udf(min(udf(a)))), udf(udf(max(udf(a)))) FROM test_having HAVING udf(udf(min(udf(a)))) = udf(udf(max(udf(a))));
SELECT udf(min(udf(a))), udf(udf(max(a))) FROM test_having HAVING udf(min(a)) < udf(max(udf(a)));

-- errors: ungrouped column references
SELECT udf(a) FROM test_having HAVING udf(min(a)) < udf(max(a));
SELECT 1 AS one FROM test_having HAVING udf(a) > 1;

-- the really degenerate case: need not scan table at all
SELECT 1 AS one FROM test_having HAVING udf(udf(1) > udf(2));
SELECT 1 AS one FROM test_having HAVING udf(udf(1) < udf(2));

-- [SPARK-33008] Spark SQL throws an exception
-- and just to prove that we aren't scanning the table:
SELECT 1 AS one FROM test_having WHERE 1/udf(a) = 1 HAVING 1 < 2;

DROP TABLE test_having;
