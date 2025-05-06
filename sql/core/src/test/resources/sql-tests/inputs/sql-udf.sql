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

-- 1.1.b A table function with various numbers of arguments
CREATE FUNCTION foo1b0() RETURNS TABLE (c1 INT) RETURN SELECT 1;
-- Expect (1)
SELECT * FROM foo1b0();

CREATE FUNCTION foo1b1(a INT) RETURNS TABLE (c1 INT) RETURN SELECT 1;
-- Expect (1)
SELECT * FROM foo1b1(1);

CREATE FUNCTION foo1b2(a INT, b INT, c INT, d INT) RETURNS TABLE(c1 INT) RETURN SELECT 1;
-- Expect (1)
SELECT * FROM foo1b2(1, 2, 3, 4);

-- 1.1.c Duplicate argument names
-- Expect failure
CREATE FUNCTION foo1c1(duplicate INT, DUPLICATE INT) RETURNS INT RETURN 1;

-- Expect failure
CREATE FUNCTION foo1c2(a INT, b INT, thisisaduplicate INT, c INT, d INT, e INT, f INT, thisIsaDuplicate INT, g INT)
    RETURNS TABLE (a INT) RETURN SELECT 1;

-- 1.1.d DEFAULT parameters
-- A NULL default
CREATE OR REPLACE FUNCTION foo1d1(a INT DEFAULT NULL) RETURNS INT RETURN a;

-- Expect 5, NULL
SELECT foo1d1(5), foo1d1();

-- A literal default
CREATE OR REPLACE FUNCTION foo1d1(a INT DEFAULT 10) RETURNS INT RETURN a;

-- Expect 5, 10
SELECT foo1d1(5), foo1d1();

-- A constant expression
CREATE OR REPLACE FUNCTION foo1d1(a INT DEFAULT length(substr(current_database(), 1, 1))) RETURNS INT RETURN a;

-- Expect 5, 1
SELECT foo1d1(5), foo1d1();

-- An expression that needs a cast
CREATE OR REPLACE FUNCTION foo1d1(a INT DEFAULT '5' || length(substr(current_database(), 1, 1)))
  RETURNS INT RETURN a;

-- Expect 5, 51
SELECT foo1d1(5), foo1d1();

-- A non deterministic default
CREATE OR REPLACE FUNCTION foo1d1(a INT DEFAULT RAND()::INT) RETURNS INT RETURN a;

-- Expect 5, 0
SELECT foo1d1(5), foo1d1();

-- Cannot cast
-- Expect error
CREATE OR REPLACE FUNCTION foo1d1(a INT DEFAULT array(55, 17))
  RETURNS INT RETURN a;

-- A subquery
CREATE OR REPLACE FUNCTION foo1d1(a INT DEFAULT (SELECT max(c1) FROM VALUES (1) AS T(c1)))
  RETURNS INT RETURN a;

-- Multiple parameters
CREATE OR REPLACE FUNCTION foo1d2(a INT, b INT DEFAULT 7, c INT DEFAULT 8, d INT DEFAULT 9 COMMENT 'test')
  RETURNS STRING RETURN a || ' ' || b || ' ' || c || ' ' || d;

-- Expect: (1 2 3 4), (1 2 3 9), (1 2 8 9), (1 7 8 9)
SELECT foo1d2(1, 2, 3, 4), foo1d2(1, 2, 3), foo1d2(1, 2), foo1d2(1);

-- Expect error a has no default
SELECT foo1d2();

-- Expect error, too many parameters
SELECT foo1d2(1, 2, 3, 4, 5);

-- Sparse default, expect error
CREATE OR REPLACE FUNCTION foo1d2(a INT DEFAULT 5, b INT , c INT DEFAULT 8, d INT DEFAULT 9 COMMENT 'test')
  RETURNS STRING RETURN a || ' ' || b || ' ' || c || ' ' || d;

CREATE OR REPLACE FUNCTION foo1d2(a INT, b INT DEFAULT 7, c INT DEFAULT 8, d INT COMMENT 'test')
  RETURNS STRING RETURN a || ' ' || b || ' ' || c || ' ' || d;

-- Temporary function
CREATE OR REPLACE TEMPORARY FUNCTION foo1d3(a INT DEFAULT 7 COMMENT 'hello') RETURNS INT RETURN a;

-- Expect 5, 7
SELECT foo1d3(5), foo1d3();

-- Dependent default
-- Expect error
CREATE OR REPLACE FUNCTION foo1d4(a INT, b INT DEFAULT a) RETURNS INT RETURN a + b;

-- Defaults with SQL UDF
CREATE OR REPLACE FUNCTION foo1d4(a INT, b INT DEFAULT 3) RETURNS INT RETURN a + b;

CREATE OR REPLACE FUNCTION foo1d5(a INT, b INT DEFAULT foo1d4(6)) RETURNS INT RETURN a + b;

-- Expect 19, 12
SELECT foo1d5(10), foo1d5(10, 2);

-- Function invocation with default in SQL UDF
CREATE OR REPLACE FUNCTION foo1d5(a INT, b INT) RETURNS INT RETURN a + foo1d4(b);

-- Expect 15
SELECT foo1d5(10, 2);

-- DEFAULT in table function
CREATE OR REPLACE FUNCTION foo1d6(a INT, b INT DEFAULT 7) RETURNS TABLE(a INT, b INT) RETURN SELECT a, b;

-- Expect (5, 7)
SELECT * FROM foo1d6(5);

-- Expect (5, 2)
SELECT * FROM foo1d6(5, 2);

-- 1.1.e NOT NULL
-- Expect failure
CREATE FUNCTION foo1e1(x INT NOT NULL, y INT) RETURNS INT RETURN 1;
CREATE FUNCTION foo1e2(x INT, y INT NOT NULL) RETURNS TABLE (x INT) RETURN SELECT 1;
CREATE FUNCTION foo1e3(x INT, y INT) RETURNS TABLE (x INT NOT NULL) RETURN SELECT 1;

-- 1.1.f GENERATED ALWAYS AS
-- Expect failure
CREATE FUNCTION foo1f1(x INT, y INT GENERATED ALWAYS AS (x + 10)) RETURNS INT RETURN y + 1;
CREATE FUNCTION foo1f2(id BIGINT GENERATED ALWAYS AS IDENTITY) RETURNS BIGINT RETURN id + 1;

-- 1.1.g Constraint
-- Expect failure
CREATE FUNCTION foo1g1(x INT, y INT UNIQUE) RETURNS INT RETURN y + 1;
CREATE FUNCTION foo1g2(id BIGINT CHECK (true)) RETURNS BIGINT RETURN id + 1;

-- 1.2 Returns Columns
-- 1.2.a A table function with various numbers of returns columns
-- Expect error: Cannot have an empty RETURNS
CREATE FUNCTION foo2a0() RETURNS TABLE() RETURN SELECT 1;

CREATE FUNCTION foo2a2() RETURNS TABLE(c1 INT, c2 INT) RETURN SELECT 1, 2;
-- Expect (1, 2)
SELECT * FROM foo2a2();

CREATE FUNCTION foo2a4() RETURNS TABLE(c1 INT, c2 INT, c3 INT, c4 INT) RETURN SELECT 1, 2, 3, 4;
-- Expect (1, 2, 3, 4)
SELECT * FROM foo2a2();

-- 1.2.b Duplicates in RETURNS clause
-- Expect failure
CREATE FUNCTION foo2b1() RETURNS TABLE(DuPLiCatE INT, duplicate INT) RETURN SELECT 1, 2;

-- Expect failure
CREATE FUNCTION foo2b2() RETURNS TABLE(a INT, b INT, duplicate INT, c INT, d INT, e INT, DUPLICATE INT)
RETURN SELECT 1, 2, 3, 4, 5, 6, 7;

-- 1.2.c No DEFAULT allowed in RETURNS
CREATE FUNCTION foo2c1() RETURNS TABLE(c1 INT DEFAULT 5) RETURN SELECT 1, 2;

-- 1.3 Mismatched RETURN
-- Expect Failure
CREATE FUNCTION foo31() RETURNS INT RETURN (SELECT 1, 2);

CREATE FUNCTION foo32() RETURNS TABLE(a INT) RETURN SELECT 1, 2;

CREATE FUNCTION foo33() RETURNS TABLE(a INT, b INT) RETURN SELECT 1;

-- 1.4 Table function returns expression and vice versa
CREATE FUNCTION foo41() RETURNS INT RETURN SELECT 1;
-- Expect failure
CREATE FUNCTION foo42() RETURNS TABLE(a INT) RETURN 1;

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

-- 3. Misc
CREATE VIEW t1(c1, c2) AS VALUES (0, 1), (0, 2), (1, 2);
CREATE VIEW t2(c1, c2) AS VALUES (0, 2), (0, 3);

-- 4. SQL table functions
CREATE FUNCTION foo4_0() RETURNS TABLE (x INT) RETURN SELECT 1;
CREATE FUNCTION foo4_1(x INT) RETURNS TABLE (a INT) RETURN SELECT x;
CREATE FUNCTION foo4_2(x INT) RETURNS TABLE (a INT) RETURN SELECT c2 FROM t2 WHERE c1 = x;
CREATE FUNCTION foo4_3(x INT) RETURNS TABLE (a INT, cnt INT) RETURN SELECT c1, COUNT(*) FROM t2 WHERE c1 = x GROUP BY c1;

-- 4.1 SQL table function with literals
SELECT * FROM foo4_0();
SELECT * FROM foo4_1(1);
SELECT * FROM foo4_2(2);
SELECT * FROM foo4_3(0);
-- with non-deterministic inputs
SELECT * FROM foo4_1(rand(0) * 0);
-- named arguments
SELECT * FROM foo4_1(x => 1);

-- 4.2 SQL table function with lateral references
SELECT * FROM t1, LATERAL foo4_1(c1);
SELECT * FROM t1, LATERAL foo4_2(c1);
SELECT * FROM t1 JOIN LATERAL foo4_2(c1) ON t1.c2 = foo4_2.a;
SELECT * FROM t1, LATERAL foo4_3(c1);
SELECT * FROM t1, LATERAL (SELECT cnt FROM foo4_3(c1));
SELECT * FROM t1, LATERAL foo4_1(c1 + rand(0) * 0);

-- 4.3 multiple SQL table functions
SELECT * FROM t1 JOIN foo4_1(1) AS foo4_1(x) ON t1.c1 = foo4_1.x;
SELECT * FROM t1, LATERAL foo4_1(c1), LATERAL foo4_2(foo4_1.a + c1);

-- 4.4 table functions inside scalar subquery
SELECT (SELECT MAX(a) FROM foo4_1(c1)) FROM t1;
SELECT (SELECT MAX(a) FROM foo4_1(c1) WHERE a = c2) FROM t1;
SELECT (SELECT MAX(cnt) FROM foo4_3(c1)) FROM t1;

-- Clean up
DROP VIEW t1;
DROP VIEW t2;
