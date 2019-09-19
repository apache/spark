--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
--
-- SELECT_IMPLICIT
-- Test cases for queries with ordering terms missing from the target list.
-- This used to be called "junkfilter.sql".
-- The parser uses the term "resjunk" to handle these cases.
-- - thomas 1998-07-09
-- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/select_implicit.sql
--

-- load test data
CREATE TABLE test_missing_target (a int, b int, c string, d string) using parquet;
INSERT INTO test_missing_target VALUES (0, 1, 'XXXX', 'A');
INSERT INTO test_missing_target VALUES (1, 2, 'ABAB', 'b');
INSERT INTO test_missing_target VALUES (2, 2, 'ABAB', 'c');
INSERT INTO test_missing_target VALUES (3, 3, 'BBBB', 'D');
INSERT INTO test_missing_target VALUES (4, 3, 'BBBB', 'e');
INSERT INTO test_missing_target VALUES (5, 3, 'bbbb', 'F');
INSERT INTO test_missing_target VALUES (6, 4, 'cccc', 'g');
INSERT INTO test_missing_target VALUES (7, 4, 'cccc', 'h');
INSERT INTO test_missing_target VALUES (8, 4, 'CCCC', 'I');
INSERT INTO test_missing_target VALUES (9, 4, 'CCCC', 'j');


--   w/ existing GROUP BY target
SELECT c, count(*) FROM test_missing_target GROUP BY test_missing_target.c ORDER BY c;

--   w/o existing GROUP BY target using a relation name in GROUP BY clause
SELECT count(*) FROM test_missing_target GROUP BY test_missing_target.c ORDER BY c;

--   w/o existing GROUP BY target and w/o existing a different ORDER BY target
--   failure expected
SELECT count(*) FROM test_missing_target GROUP BY a ORDER BY b;

--   w/o existing GROUP BY target and w/o existing same ORDER BY target
SELECT count(*) FROM test_missing_target GROUP BY b ORDER BY b;

--   w/ existing GROUP BY target using a relation name in target
SELECT test_missing_target.b, count(*)
  FROM test_missing_target GROUP BY b ORDER BY b;

--   w/o existing GROUP BY target
SELECT c FROM test_missing_target ORDER BY a;

--   w/o existing ORDER BY target
SELECT count(*) FROM test_missing_target GROUP BY b ORDER BY b desc;

--   group using reference number
SELECT count(*) FROM test_missing_target ORDER BY 1 desc;

--   order using reference number
SELECT c, count(*) FROM test_missing_target GROUP BY 1 ORDER BY 1;

--   group using reference number out of range
--   failure expected
SELECT c, count(*) FROM test_missing_target GROUP BY 3;

--   group w/o existing GROUP BY and ORDER BY target under ambiguous condition
--   failure expected
SELECT count(*) FROM test_missing_target x, test_missing_target y
	WHERE x.a = y.a
	GROUP BY b ORDER BY b;

--   order w/ target under ambiguous condition
--   failure NOT expected
SELECT a, a FROM test_missing_target
	ORDER BY a;

--   order expression w/ target under ambiguous condition
--   failure NOT expected
SELECT a/2, a/2 FROM test_missing_target
	ORDER BY a/2;

--   group expression w/ target under ambiguous condition
--   failure NOT expected
SELECT a/2, a/2 FROM test_missing_target
	GROUP BY a/2 ORDER BY a/2;

--   group w/ existing GROUP BY target under ambiguous condition
SELECT x.b, count(*) FROM test_missing_target x, test_missing_target y
	WHERE x.a = y.a
	GROUP BY x.b ORDER BY x.b;

--   group w/o existing GROUP BY target under ambiguous condition
SELECT count(*) FROM test_missing_target x, test_missing_target y
	WHERE x.a = y.a
	GROUP BY x.b ORDER BY x.b;

-- [SPARK-28329] SELECT INTO syntax
--   group w/o existing GROUP BY target under ambiguous condition
--   into a table
-- SELECT count(*) INTO TABLE test_missing_target2
-- FROM test_missing_target x, test_missing_target y
-- 	WHERE x.a = y.a
-- 	GROUP BY x.b ORDER BY x.b;
-- SELECT * FROM test_missing_target2;


--  Functions and expressions

--   w/ existing GROUP BY target
SELECT a%2, count(b) FROM test_missing_target
GROUP BY test_missing_target.a%2
ORDER BY test_missing_target.a%2;

--   w/o existing GROUP BY target using a relation name in GROUP BY clause
SELECT count(c) FROM test_missing_target
GROUP BY lower(test_missing_target.c)
ORDER BY lower(test_missing_target.c);

--   w/o existing GROUP BY target and w/o existing a different ORDER BY target
--   failure expected
SELECT count(a) FROM test_missing_target GROUP BY a ORDER BY b;

--   w/o existing GROUP BY target and w/o existing same ORDER BY target
SELECT count(b) FROM test_missing_target GROUP BY b/2 ORDER BY b/2;

--   w/ existing GROUP BY target using a relation name in target
SELECT lower(test_missing_target.c), count(c)
  FROM test_missing_target GROUP BY lower(c) ORDER BY lower(c);

--   w/o existing GROUP BY target
SELECT a FROM test_missing_target ORDER BY upper(d);

--   w/o existing ORDER BY target
SELECT count(b) FROM test_missing_target
	GROUP BY (b + 1) / 2 ORDER BY (b + 1) / 2 desc;

--   group w/o existing GROUP BY and ORDER BY target under ambiguous condition
--   failure expected
SELECT count(x.a) FROM test_missing_target x, test_missing_target y
	WHERE x.a = y.a
	GROUP BY b/2 ORDER BY b/2;

--   group w/ existing GROUP BY target under ambiguous condition
SELECT x.b/2, count(x.b) FROM test_missing_target x, test_missing_target y
	WHERE x.a = y.a
	GROUP BY x.b/2 ORDER BY x.b/2;

--   group w/o existing GROUP BY target under ambiguous condition
--   failure expected due to ambiguous b in count(b)
SELECT count(b) FROM test_missing_target x, test_missing_target y
	WHERE x.a = y.a
	GROUP BY x.b/2;

-- [SPARK-28329] SELECT INTO syntax
--   group w/o existing GROUP BY target under ambiguous condition
--   into a table
-- SELECT count(x.b) INTO TABLE test_missing_target3
-- FROM test_missing_target x, test_missing_target y
-- 	WHERE x.a = y.a
-- 	GROUP BY x.b/2 ORDER BY x.b/2;
-- SELECT * FROM test_missing_target3;

--   Cleanup
DROP TABLE test_missing_target;
-- DROP TABLE test_missing_target2;
-- DROP TABLE test_missing_target3;
