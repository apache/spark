-- test cases for SQL User Defined Functions

-- 1. CREATE FUNCTION
-- 1.1 Parameter
-- 1.1.a A scalar function with various numbers of parameter
-- Expect success
CREATE FUNCTION foo1a0() RETURNS INT RETURN 1;
-- Expect: 1
SELECT foo1a0();
-- Expect failure
SELECT foo1a0(1);

CREATE FUNCTION foo1a1(a INT) RETURNS INT RETURN 1;
-- Expect: 1
SELECT foo1a1(1);
-- Expect failure
SELECT foo1a1(1, 2);

CREATE FUNCTION foo1a2(a INT, b INT, c INT, d INT) RETURNS INT RETURN 1;
-- Expect: 1
SELECT foo1a2(1, 2, 3, 4);

-------------------------------
-- 2. Scalar SQL UDF
-- 2.1 deterministic simple expressions
CREATE FUNCTION foo2_1a(a INT) RETURNS INT RETURN a;
SELECT foo2_1a(5);

CREATE FUNCTION foo2_1b(a INT, b INT) RETURNS INT RETURN a + b;
SELECT foo2_1b(5, 6);

CREATE FUNCTION foo2_1c(a INT, b INT) RETURNS INT RETURN 10 * (a + b) + 100 * (a -b);
SELECT foo2_1c(5, 6);

CREATE FUNCTION foo2_1d(a INT, b INT) RETURNS INT RETURN ABS(a) - LENGTH(CAST(b AS VARCHAR(10)));
SELECT foo2_1d(-5, 6);

-- 2.2 deterministic complex expression with subqueries
-- 2.2.1 Nested Scalar subqueries
CREATE FUNCTION foo2_2a(a INT) RETURNS INT RETURN SELECT a;
SELECT foo2_2a(5);

CREATE FUNCTION foo2_2b(a INT) RETURNS INT RETURN 1 + (SELECT a);
SELECT foo2_2b(5);

-- Expect error: deep correlation is not yet supported
CREATE FUNCTION foo2_2c(a INT) RETURNS INT RETURN 1 + (SELECT (SELECT a));
-- SELECT foo2_2c(5);

-- Expect error: deep correlation is not yet supported
CREATE FUNCTION foo2_2d(a INT) RETURNS INT RETURN 1 + (SELECT (SELECT (SELECT (SELECT a))));
-- SELECT foo2_2d(5);

-- 2.2.2 Set operations
-- Expect error: correlated scalar subquery must be aggregated.
CREATE FUNCTION foo2_2e(a INT) RETURNS INT RETURN
SELECT a FROM (VALUES 1) AS V(c1) WHERE c1 = 2
UNION ALL
SELECT a + 1 FROM (VALUES 1) AS V(c1);
-- SELECT foo2_2e(5);

-- Expect error: correlated scalar subquery must be aggregated.
CREATE FUNCTION foo2_2f(a INT) RETURNS INT RETURN
SELECT a FROM (VALUES 1) AS V(c1)
EXCEPT
SELECT a + 1 FROM (VALUES 1) AS V(a);
-- SELECT foo2_2f(5);

-- Expect error: correlated scalar subquery must be aggregated.
CREATE FUNCTION foo2_2g(a INT) RETURNS INT RETURN
SELECT a FROM (VALUES 1) AS V(c1)
INTERSECT
SELECT a FROM (VALUES 1) AS V(a);
-- SELECT foo2_2g(5);

-- Prepare by dropping views or tables if they already exist.
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS tm;
DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS V1;
DROP TABLE IF EXISTS V2;
DROP VIEW IF EXISTS t1;
DROP VIEW IF EXISTS t2;
DROP VIEW IF EXISTS ts;
DROP VIEW IF EXISTS tm;
DROP VIEW IF EXISTS ta;
DROP VIEW IF EXISTS V1;
DROP VIEW IF EXISTS V2;

-- 2.3 Calling Scalar UDF from various places
CREATE FUNCTION foo2_3(a INT, b INT) RETURNS INT RETURN a + b;
CREATE VIEW V1(c1, c2) AS VALUES (1, 2), (3, 4), (5, 6);
CREATE VIEW V2(c1, c2) AS VALUES (-1, -2), (-3, -4), (-5, -6);

-- 2.3.1 Multiple times in the select list
SELECT foo2_3(c1, c2), foo2_3(c2, 1), foo2_3(c1, c2) - foo2_3(c2, c1 - 1) FROM V1 ORDER BY 1, 2, 3;

-- 2.3.2 In the WHERE clause
SELECT * FROM V1 WHERE foo2_3(c1, 0) = c1 AND foo2_3(c1, c2) < 8;

-- 2.3.3 Different places around an aggregate
SELECT foo2_3(SUM(c1), SUM(c2)), SUM(c1) + SUM(c2), SUM(foo2_3(c1, c2) + foo2_3(c2, c1) - foo2_3(c2, c1))
FROM V1;

-- 2.4 Scalar UDF with complex one row relation subquery
-- 2.4.1 higher order functions
CREATE FUNCTION foo2_4a(a ARRAY<STRING>) RETURNS STRING RETURN
SELECT array_sort(a, (i, j) -> rank[i] - rank[j])[0] FROM (SELECT MAP('a', 1, 'b', 2) rank);

SELECT foo2_4a(ARRAY('a', 'b'));

-- 2.4.2 built-in functions
CREATE FUNCTION foo2_4b(m MAP<STRING, STRING>, k STRING) RETURNS STRING RETURN
SELECT v || ' ' || v FROM (SELECT upper(m[k]) AS v);

SELECT foo2_4b(map('a', 'hello', 'b', 'world'), 'a');

-- Clean up
DROP VIEW V2;
DROP VIEW V1;
