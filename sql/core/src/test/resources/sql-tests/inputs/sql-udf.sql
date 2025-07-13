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

-- 1.5 Scalar function returns subquery with more than one row or no rows

-- 1.5.a More than one row
CREATE FUNCTION foo51() RETURNS INT RETURN (SELECT a FROM VALUES(1), (2) AS T(a));
SELECT foo51();

-- 1.5.b No Rows
CREATE FUNCTION foo52() RETURNS INT RETURN (SELECT 1 FROM VALUES(1) WHERE 1 = 0);
-- Expect Success: NULL
SELECT foo52();

-- 1.6 Difficult identifiers
-- 1.6.a Space in the schema name
-- UNSUPPORTED BY CREATE SCHEMA
-- CREATE SCHEMA `a b`;

-- CREATE FUNCTION `a b`.foo6a() RETURNS INT RETURN 1;
-- SELECT `a b`.foo6a();

-- DROP FUNCTION `a b`.foo6a;
-- DROP SCHEMA `a b`;

-- 1.6.b Space in a function name
-- Default Hive configuration does not allow function name with space
-- CREATE FUNCTION `foo 6 b`() RETURNS INT RETURN 1;
-- SELECT `foo 6 b`();
-- DROP FUNCTION `foo 6 b`;

-- 1.6.c Spaces in parameter names
CREATE FUNCTION foo6c(` a` INT, a INT, `a b` INT) RETURNS INT RETURN 1;
SELECT foo6c(1, 2, 3);

-- 1.6.d Spaces in RETURNS column list
CREATE FUNCTION foo6d() RETURNS TABLE(` a` INT, a INT, `a b` INT) RETURN SELECT 1, 2, 3;
SELECT * FROM foo6d();

-- 1.7 Parameter resolution
CREATE FUNCTION foo7a(a STRING, b STRING, c STRING) RETURNS STRING RETURN
SELECT 'Foo.a: ' || a ||  ' Foo.a: ' || foo7a.a
       || ' T.b: ' ||  b || ' Foo.b: ' || foo7a.b
       || ' T.c: ' || c || ' T.c: ' || t.c FROM VALUES('t.b', 't.c') AS T(b, c);

SELECT foo7a('Foo.a', 'Foo.b', 'Foo.c');

CREATE FUNCTION foo7at(a STRING, b STRING, c STRING) RETURNS TABLE (a STRING, b STRING, c STRING, d STRING, e STRING) RETURN
SELECT CONCAT('Foo.a: ', a), CONCAT('Foo.b: ', foo7at.b), CONCAT('T.b: ', b),
       CONCAT('Foo.c: ', foo7at.c), CONCAT('T.c: ', c)
FROM VALUES ('t.b', 't.c') AS T(b, c);
SELECT * FROM foo7at('Foo.a', 'Foo.b', 'Foo.c');

-- 1.8 Comments
-- Need to verify comments in non-sql tests

-- 1.9 Test all data types
-- Boolean
CREATE FUNCTION foo9a(a BOOLEAN) RETURNS BOOLEAN RETURN NOT a;
SELECT foo9a(true);

-- Expect error
SELECT foo9a(5);
SELECT foo9a('Nonsense');

-- Byte
CREATE FUNCTION foo9b(a BYTE) RETURNS BYTE RETURN CAST(a AS SHORT) + 1;
SELECT foo9b(126);
SELECT foo9b(127);
SELECT foo9b(128);

-- Short
CREATE FUNCTION foo9c(a SHORT) RETURNS SHORT RETURN CAST(a AS INTEGER) + 1;
SELECT foo9c(32766);
SELECT foo9c(32767);
SELECT foo9c(32768);

-- Integer
CREATE FUNCTION foo9d(a INTEGER) RETURNS INTEGER RETURN CAST(a AS BIGINT) + 1;
SELECT foo9d(2147483647 - 1);
SELECT foo9d(2147483647);
SELECT foo9d(2147483647 + 1);

-- Bigint
CREATE FUNCTION foo9e(a BIGINT) RETURNS BIGINT RETURN CAST(a AS DECIMAL(20, 0)) + 1;
SELECT foo9e(9223372036854775807 - 1);
SELECT foo9e(9223372036854775807);
SELECT foo9e(9223372036854775807.0 + 1);

-- DECIMAL
CREATE FUNCTION foo9f(a DECIMAL( 5, 2 )) RETURNS DECIMAL (5, 2) RETURN CAST(a AS DECIMAL(6, 2)) + 1;
SELECT foo9f(999 - 1);
SELECT foo9f(999);
SELECT foo9f(999 + 1);

-- FLOAT
CREATE FUNCTION foo9g(a FLOAT, b String) RETURNS FLOAT RETURN b || CAST(a AS String);
SELECT foo9g(123.23, '7');
SELECT foo9g('hello', '7');
SELECT foo9g(123.23, 'q');

-- DOUBLE
CREATE FUNCTION foo9h(a DOUBLE, b String) RETURNS DOUBLE RETURN b || CAST(a AS String);
SELECT foo9h(123.23, '7');
SELECT foo9h('hello', '7');
SELECT foo9h(123.23, 'q');

-- VARCHAR
-- Expect failure: char/varchar type can only be used in the table schema.
CREATE FUNCTION foo9i(a VARCHAR(10), b VARCHAR(10)) RETURNS VARCHAR(12) RETURN a || b;
-- SELECT foo9i('1234567890', '');
-- SELECT foo9i('12345678901', '');
-- SELECT foo9i('1234567890', '1');

-- STRING
CREATE FUNCTION foo9j(a STRING, b STRING) RETURNS STRING RETURN a || b;
SELECT foo9j('1234567890', '12');
SELECT foo9j(12345678901, '12');

-- DATE
CREATE FUNCTION foo9l(a DATE, b INTERVAL) RETURNS DATE RETURN a + b;
SELECT foo9l(DATE '2020-02-02', INTERVAL '1' YEAR);
SELECT foo9l('2020-02-02', INTERVAL '1' YEAR);
SELECT foo9l(DATE '-7', INTERVAL '1' YEAR);
SELECT foo9l(DATE '2020-02-02', INTERVAL '9999999' YEAR);

-- TIMESTAMP
CREATE FUNCTION foo9m(a TIMESTAMP, b INTERVAL) RETURNS TIMESTAMP RETURN a + b;
SELECT foo9m(TIMESTAMP'2020-02-02 12:15:16.123', INTERVAL '1' YEAR);
SELECT foo9m('2020-02-02 12:15:16.123', INTERVAL '1' YEAR);
SELECT foo9m(TIMESTAMP'2020-02-02 12:15:16.123', INTERVAL '999999' YEAR);

-- ARRAY
CREATE FUNCTION foo9n(a ARRAY<INTEGER>) RETURNS ARRAY<INTEGER> RETURN a;
SELECT foo9n(ARRAY(1, 2, 3));
SELECT foo9n(from_json('[1, 2, 3]', 'array<int>'));

-- MAP
CREATE FUNCTION foo9o(a MAP<STRING, INTEGER>) RETURNS MAP<STRING, INTEGER> RETURN a;
SELECT foo9o(MAP('hello', 1, 'world', 2));
SELECT foo9o(from_json('{"hello":1, "world":2}', 'map<string,int>'));

-- STRUCT
CREATE FUNCTION foo9p(a STRUCT<a1: INTEGER, a2: STRING>) RETURNS STRUCT<a1: INTEGER, a2: STRING> RETURN a;
SELECT foo9p(STRUCT(1, 'hello'));
SELECT foo9p(from_json('{1:"hello"}', 'struct<a1:int, a2:string>'));

-- ARRAY of STRUCT
CREATE FUNCTION foo9q(a ARRAY<STRUCT<a1: INT, a2: STRING>>) RETURNS ARRAY<STRUCT<a1: INT, a2: STRING>> RETURN a;
SELECT foo9q(ARRAY(STRUCT(1, 'hello'), STRUCT(2, 'world')));
SELECT foo9q(ARRAY(NAMED_STRUCT('x', 1, 'y', 'hello'), NAMED_STRUCT('x', 2, 'y', 'world')));
SELECT foo9q(from_json('[{1:"hello"}, {2:"world"}]', 'array<struct<a1:int,a2:string>>'));

-- ARRAY of MAP
CREATE FUNCTION foo9r(a ARRAY<MAP<STRING, INT>>) RETURNS ARRAY<MAP<STRING, INT>> RETURN a;
SELECT foo9r(ARRAY(MAP('hello', 1), MAP('world', 2)));
SELECT foo9r(from_json('[{"hello":1}, {"world":2}]', 'array<map<string,int>>'));

-- 1.10 Proper name resolution when referencing another function
CREATE OR REPLACE FUNCTION foo1_10(a INT) RETURNS INT RETURN a + 2;
CREATE OR REPLACE FUNCTION bar1_10(b INT) RETURNS STRING RETURN foo1_10(TRY_CAST(b AS STRING));
SELECT bar1_10(3);

-- 1.11 Optional return types (type inference)
-- 1.11.a Scalar UDF without RETURNS clause - return type inferred from body
-- Simple literal return
CREATE OR REPLACE FUNCTION foo1_11a() RETURN 42;
-- Expect: 42
SELECT foo1_11a();

-- String literal return
CREATE OR REPLACE FUNCTION foo1_11b() RETURN 'hello world';
-- Expect: 'hello world'
SELECT foo1_11b();

-- Expression return - should infer INT
CREATE OR REPLACE FUNCTION foo1_11c(a INT, b INT) RETURN a + b;
-- Expect: 8
SELECT foo1_11c(3, 5);

-- Expression return - should infer DOUBLE
CREATE OR REPLACE FUNCTION foo1_11d(a DOUBLE, b INT) RETURN a * b + 1.5;
-- Expect: 16.5
SELECT foo1_11d(3.0, 5);

-- Boolean expression return
CREATE OR REPLACE FUNCTION foo1_11e(a INT) RETURN a > 10;
-- Expect: true, false
SELECT foo1_11e(15), foo1_11e(5);

-- Date arithmetic return
CREATE OR REPLACE FUNCTION foo1_11f(d DATE) RETURN d + INTERVAL '1' DAY;
-- Expect: 2024-01-02
SELECT foo1_11f(DATE '2024-01-01');

-- Array return
CREATE OR REPLACE FUNCTION foo1_11g(n INT) RETURN ARRAY(1, 2, n);
-- Expect: [1, 2, 5]
SELECT foo1_11g(5);

-- Struct return
CREATE OR REPLACE FUNCTION foo1_11h(a INT, b STRING) RETURN STRUCT(a, b);
-- Expect: {1, 'test'}
SELECT foo1_11h(1, 'test');

-- Subquery return - scalar
CREATE OR REPLACE FUNCTION foo1_11i(x INT) RETURN (SELECT x * 2);
-- Expect: 10
SELECT foo1_11i(5);

-- Function call return
CREATE OR REPLACE FUNCTION foo1_11j(s STRING) RETURN UPPER(s);
-- Expect: 'HELLO'
SELECT foo1_11j('hello');

-- Complex expression with multiple types
CREATE OR REPLACE FUNCTION foo1_11k(a INT, b STRING) RETURN CONCAT(CAST(a AS STRING), '_', b);
-- Expect: '123_test'
SELECT foo1_11k(123, 'test');

-- 1.11.b Table UDF without TABLE schema - schema inferred from body
-- Simple SELECT with literals
CREATE OR REPLACE FUNCTION foo1_11l() RETURNS TABLE RETURN SELECT 1 as id, 'hello' as name;
-- Expect: (1, 'hello')
SELECT * FROM foo1_11l();

-- SELECT with expressions
CREATE OR REPLACE FUNCTION foo1_11m(a INT, b STRING) RETURNS TABLE RETURN SELECT a * 2 as doubled, UPPER(b) as upper_name;
-- Expect: (10, 'WORLD')
SELECT * FROM foo1_11m(5, 'world');

-- SELECT with complex data types
CREATE OR REPLACE FUNCTION foo1_11n(arr ARRAY<INT>) RETURNS TABLE RETURN SELECT size(arr) as array_size, arr[0] as first_element;
-- Expect: (3, 1)
SELECT * FROM foo1_11n(ARRAY(1, 2, 3));

-- SELECT with struct columns
CREATE OR REPLACE FUNCTION foo1_11o(id INT, name STRING) RETURNS TABLE RETURN SELECT STRUCT(id, name) as person_info, id + 100 as modified_id;
-- Expect: ({1, 'Alice'}, 101)
SELECT * FROM foo1_11o(1, 'Alice');

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
CREATE VIEW ts(x) AS VALUES NAMED_STRUCT('a', 1, 'b', 2);
CREATE VIEW tm(x) AS VALUES MAP('a', 1, 'b', 2);
CREATE VIEW ta(x) AS VALUES ARRAY(1, 2, 3);

-- 3.1 deterministic functions
CREATE FUNCTION foo3_1a(a DOUBLE, b DOUBLE) RETURNS DOUBLE RETURN a * b;
CREATE FUNCTION foo3_1b(x INT) RETURNS INT RETURN x;
CREATE FUNCTION foo3_1c(x INT) RETURNS INT RETURN SELECT x;
CREATE FUNCTION foo3_1d(x INT) RETURNS INT RETURN (SELECT SUM(c2) FROM t2 WHERE c1 = x);
CREATE FUNCTION foo3_1e() RETURNS INT RETURN foo3_1d(0);
-- Function body is a uncorrelated scalar subquery.
CREATE FUNCTION foo3_1f() RETURNS INT RETURN SELECT SUM(c2) FROM t2 WHERE c1 = 0;
CREATE FUNCTION foo3_1g(x INT) RETURNS INT RETURN SELECT (SELECT x);

-- 3.1.1 scalar function in various operators
-- in project
SELECT a, b, foo3_1a(a + 1, b + 1) FROM t1 AS t(a, b);
SELECT x, foo3_1c(x) FROM t1 AS t(x, y);
SELECT c1, foo3_1d(c1) FROM t1;

-- in project, with nested SQL functions
SELECT c1, foo3_1a(foo3_1b(c1), foo3_1b(c1)) FROM t1;
SELECT c1, foo3_1d(foo3_1c(foo3_1b(c1))) FROM t1;
SELECT c1, foo3_1a(foo3_1c(foo3_1b(c1)), foo3_1d(foo3_1b(c1))) FROM t1;
SELECT foo3_1c(foo3_1e()) FROM t1;

-- in aggregate
SELECT foo3_1a(MAX(c1), MAX(c2)) FROM t1;
SELECT foo3_1a(MAX(c1), c2) FROM t1 GROUP BY c2;
SELECT foo3_1a(c1, c2) FROM t1 GROUP BY c1, c2;
SELECT MAX(foo3_1a(c1, c2)) FROM t1 GROUP BY c1, c2;
SELECT MAX(c1) + foo3_1b(MAX(c1)) FROM t1 GROUP BY c2;
SELECT c1, SUM(foo3_1c(c2)) FROM t1 GROUP BY c1;
SELECT c1, SUM(foo3_1d(c2)) FROM t1 GROUP BY c1;
SELECT foo3_1c(c1), foo3_1d(c1) FROM t1 GROUP BY c1;

-- in aggregate, with non-deterministic input
SELECT foo3_1a(SUM(c1), rand(0) * 0) FROM t1;
SELECT foo3_1a(SUM(c1) + rand(0) * 0, SUM(c2)) FROM t1;
SELECT foo3_1b(SUM(c1) + rand(0) * 0) FROM t1;
SELECT foo3_1b(SUM(1) + rand(0) * 0) FROM t1 GROUP BY c2;
SELECT foo3_1c(SUM(c2) + rand(0) * 0) FROM t1 GROUP by c1;

-- in aggregate, with nested SQL functions
SELECT foo3_1b(foo3_1b(MAX(c2))) FROM t1;
SELECT foo3_1b(MAX(foo3_1b(c2))) FROM t1;
SELECT foo3_1a(foo3_1b(c1), MAX(c2)) FROM t1 GROUP BY c1;

-- in aggregate, with grouping expressions
SELECT c1, foo3_1b(c1) FROM t1 GROUP BY c1;
SELECT c1, foo3_1b(c1 + 1) FROM t1 GROUP BY c1;
SELECT c1, foo3_1b(c1 + rand(0) * 0) FROM t1 GROUP BY c1;
SELECT c1, foo3_1a(c1, MIN(c2)) FROM t1 GROUP BY c1;
SELECT c1, foo3_1a(c1 + 1, MIN(c2 + 1)) FROM t1 GROUP BY c1;
SELECT c1, c2, foo3_1a(c1, c2) FROM t1 GROUP BY c1, c2;
SELECT c1, c2, foo3_1a(1, 2) FROM t1 GROUP BY c1, c2;
SELECT c1 + c2, foo3_1b(c1 + c2 + 1) FROM t1 GROUP BY c1 + c2;
SELECT COUNT(*) + foo3_1b(c1) + foo3_1b(SUM(c2)) + SUM(foo3_1b(c2)) FROM t1 GROUP BY c1;

-- in aggregate, with having expressions
SELECT c1, COUNT(*), foo3_1b(SUM(c2)) FROM t1 GROUP BY c1 HAVING COUNT(*) > 0;
SELECT c1, COUNT(*), foo3_1b(SUM(c2)) FROM t1 GROUP BY c1 HAVING foo3_1b(SUM(c2)) > 0;
-- Expect failure
SELECT c1, COUNT(*), foo3_1b(SUM(c2)) FROM t1 GROUP BY c1 HAVING SUM(foo3_1b(c2)) > 0;

-- in aggregate, with sql function in group by columns
SELECT foo3_1b(c1), MIN(c2) FROM t1 GROUP BY 1;
SELECT foo3_1a(c1 + rand(0) * 0, c2) FROM t1 GROUP BY 1;
SELECT c1, c2, foo3_1a(c1, c2) FROM t1 GROUP BY c1, c2, 3;

-- in aggregate, with scalar subquery
SELECT c1, (SELECT c1), (SELECT foo3_1b(c1)), SUM(c2) FROM t1 GROUP BY 1, 2, 3;
SELECT c1, SUM(c2) + foo3_1a(MIN(c2), MAX(c2)) + (SELECT SUM(c2)) FROM t1 GROUP BY c1;
SELECT foo3_1b(SUM(c1)) + (SELECT foo3_1b(SUM(c1))) FROM t1;

-- in aggregate, with invalid aggregate expressions
SELECT SUM(foo3_1b(SUM(c1))) FROM t1;
SELECT foo3_1b(SUM(c1)) + (SELECT SUM(SUM(c1))) FROM t1;
SELECT foo3_1b(SUM(c1) + SUM(SUM(c1))) FROM t1;
SELECT foo3_1b(SUM(c1 + rand(0) * 0)) FROM t1;
SELECT SUM(foo3_1b(c1) + rand(0) * 0) FROM t1;

-- in aggregate, with non-deterministic function input inside aggregate expression
SELECT SUM(foo3_1b(c1 + rand(0) * 0)) FROM t1;

-- in aggregate, with nested SQL functions
SELECT foo3_1b(SUM(c1) + foo3_1b(SUM(c1))) FROM t1;
SELECT foo3_1b(SUM(c2) + foo3_1b(SUM(c1))) AS foo FROM t1 HAVING foo > 0;
SELECT c1, COUNT(*), foo3_1b(SUM(c2) + foo3_1b(SUM(c2))) FROM t1 GROUP BY c1 HAVING COUNT(*) > 0;

-- in aggregate, with invalid group by
SELECT foo3_1a(c1, MAX(c2)) FROM t1 GROUP BY c1, 1;

-- in CTE
WITH cte AS (SELECT foo3_1a(c1, c2) FROM t1)
SELECT * FROM cte;

-- in GROUP BY
SELECT SUM(c2) FROM t1 GROUP BY foo3_1b(c1);
SELECT foo3_1b(c1), SUM(c2) FROM t1 GROUP BY 1;
SELECT foo3_1b(c1), c2, GROUPING(foo3_1b(c1)), SUM(c1) FROM t1 GROUP BY ROLLUP(foo3_1b(c1), c2);

-- in HAVING
SELECT c1, SUM(c2) FROM t1 GROUP BY c1 HAVING foo3_1b(SUM(c2)) > 1;
SELECT c1, SUM(c2) FROM t1 GROUP BY CUBE(c1) HAVING foo3_1b(GROUPING(c1)) = 0;

-- in join
SELECT * FROM t1 JOIN t2 ON foo3_1a(t1.c1, t2.c2) >= 2;
SELECT * FROM t1 JOIN t2 ON foo3_1b(t1.c2) = foo3_1b(t2.c2);
SELECT * FROM t1 JOIN t2 ON foo3_1b(t1.c1 + t2.c1 + 2) > 2;
SELECT * FROM t1 JOIN t2 ON foo3_1a(foo3_1b(t1.c1), t2.c2) >= 2;
-- in join with non-correlated scalar subquery
SELECT * FROM t1 JOIN t2 ON foo3_1f() > 0;
-- expect error: non-deterministic expressions cannot be used in Join
SELECT * FROM t1 JOIN t2 ON foo3_1b(t1.c1 + rand(0) * 0) > 1;
-- this works because the analyzer interprets the function body of 'SELECT x' as just 'x' now
SELECT * FROM t1 JOIN t2 ON foo3_1c(t1.c1) = 2;
-- expect error: correlated scalar subquery cannot be used in Join
SELECT * FROM t1 JOIN t2 ON foo3_1g(t1.c1) = 2;

-- in sort: unsupported
SELECT * FROM t1 ORDER BY foo3_1b(c1);

-- in limit: unsupported
SELECT * FROM t1 LIMIT foo3_1b(1);

-- in generate: unsupported
SELECT * FROM ta LATERAL VIEW EXPLODE(ARRAY(foo3_1b(x[0]), foo3_1b(x[1]))) AS t;

-- 3.1.2 scalar function with various function inputs
-- with non-deterministic expressions
SELECT CASE WHEN foo3_1b(rand(0) * 0 < 1 THEN 1 ELSE -1 END;

-- with outer references
SELECT (SELECT SUM(c2) FROM t2 WHERE c1 = foo3_1b(t1.c1)) FROM t1;

-- with uncorrelated scalar subquery
SELECT foo3_1b((SELECT SUM(c1) FROM t1));
SELECT foo3_1a(c1, (SELECT MIN(c1) FROM t1)) FROM t1;

-- with correlated scalar subquery
SELECT foo3_1b((SELECT SUM(c1))) FROM t1;
SELECT foo3_1b((SELECT SUM(c1) FROM t1 WHERE c2 = t2.c2)) FROM t2;
SELECT c2, AVG(foo3_1b((SELECT COUNT(*) FROM t1 WHERE c2 = t2.c2))) OVER (PARTITION BY c1) AS r FROM t2;

-- 3.1.3 scalar function with complex data type
CREATE FUNCTION foo3_1x(x STRUCT<a: INT, b: INT>) RETURNS INT RETURN x.a + x.b;
CREATE FUNCTION foo3_1y(x ARRAY<INT>) RETURNS INT RETURN aggregate(x, BIGINT(0), (x, y) -> x + y);

-- with struct type
SELECT foo3_1a(x.a, x.b) FROM ts;
SELECT foo3_1x(x) FROM ts;

-- with map type
SELECT foo3_1a(x['a'], x['b']) FROM tm;

-- with array type
SELECT foo3_1a(x[0], x[1]) FROM ta;
SELECT foo3_1y(x) FROM ta;

-- 3.2 Scalar function with complex function body
-- 3.2.a Non-deterministic expression
CREATE FUNCTION foo3_2a() RETURNS INT RETURN FLOOR(RAND() * 6) + 1;

SELECT CASE WHEN foo3_2a() > 6 THEN FALSE ELSE TRUE END;
-- Expect error: non-deterministic expressions cannot be used in Join
SELECT * FROM t1 JOIN t2 ON foo3_2a() = 1;

-- 3.2.b IN subqueries
CREATE FUNCTION foo3_2b1(x INT) RETURNS BOOLEAN RETURN x IN (SELECT 1);
SELECT * FROM t1 WHERE foo3_2b1(c1);

CREATE FUNCTION foo3_2b2(x INT) RETURNS INT RETURN IF(x IN (SELECT 1), 1, 0);
SELECT * FROM t1 WHERE foo3_2b2(c1) = 0;
SELECT foo3_2b2(c1) FROM t1;

CREATE FUNCTION foo3_2b3(x INT) RETURNS BOOLEAN RETURN x IN (SELECT c1 FROM t2);
SELECT * FROM t1 WHERE foo3_2b3(c1);

CREATE FUNCTION foo3_2b4(x INT) RETURNS BOOLEAN RETURN x NOT IN (SELECT c2 FROM t2 WHERE x = c1);
SELECT * FROM t1 WHERE foo3_2b4(c1);

-- Expect error
CREATE FUNCTION foo3_2b5(x INT) RETURNS BOOLEAN RETURN SUM(1) + IF(x IN (SELECT 1), 1, 0);
CREATE FUNCTION foo3_2b5(x INT) RETURNS BOOLEAN RETURN y IN (SELECT 1);
CREATE FUNCTION foo3_2b5(x INT) RETURNS BOOLEAN RETURN x IN (SELECT x WHERE x = 1);

-- 3.2.c EXISTS subqueries
CREATE FUNCTION foo3_2c1(x INT) RETURNS BOOLEAN RETURN EXISTS(SELECT 1);
SELECT * FROM t1 WHERE foo3_2c1(c1);

CREATE FUNCTION foo3_2c2(x INT) RETURNS BOOLEAN RETURN NOT EXISTS(SELECT * FROM t2 WHERE c1 = x);
SELECT * FROM t1 WHERE foo3_2c2(c1);

-- 3.2.d with nested subquery: not supported
CREATE FUNCTION foo3_2d1(x INT) RETURNS INT RETURN SELECT (SELECT x);
CREATE FUNCTION foo3_2d2(x INT) RETURNS INT RETURN SELECT (SELECT 1 WHERE EXISTS (SELECT * FROM t2 WHERE c1 = x));

-- 3.2.e CTEs
CREATE FUNCTION foo3_2e1(
    occurrences ARRAY<STRUCT<start_time: TIMESTAMP, occurrence_id: STRING>>,
    instance_start_time TIMESTAMP
) RETURNS STRING RETURN
WITH t AS (
    SELECT transform(occurrences, x -> named_struct(
        'diff', abs(unix_millis(x.start_time) - unix_millis(instance_start_time)),
        'id', x.occurrence_id
    )) AS diffs
)
SELECT CASE WHEN occurrences IS NULL OR size(occurrences) = 0
       THEN NULL
       ELSE sort_array(diffs)[0].id END AS id
FROM t;

SELECT foo3_2e1(
    ARRAY(STRUCT('2022-01-01 10:11:12', '1'), STRUCT('2022-01-01 10:11:15', '2')),
    '2022-01-01');

-- 3.3 Create and invoke function with different SQL configurations
SET spark.sql.ansi.enabled=true;
CREATE FUNCTION foo3_3a(x INT) RETURNS DOUBLE RETURN 1 / x;
CREATE FUNCTION foo3_3at(x INT) RETURNS TABLE (a DOUBLE) RETURN SELECT 1 / x;
CREATE TEMPORARY FUNCTION foo3_3b(x INT) RETURNS DOUBLE RETURN 1 / x;
SET spark.sql.ansi.enabled=false;
-- Expect ArithmeticException
SELECT foo3_3a(0);
SELECT foo3_3b(0);
SELECT * FROM foo3_3at(0);
-- Replace the functions with different configs.
CREATE OR REPLACE FUNCTION foo3_3a(x INT) RETURNS DOUBLE RETURN 1 / x;
CREATE OR REPLACE FUNCTION foo3_3at(x INT) RETURNS TABLE (a DOUBLE) RETURN SELECT 1 / x;
CREATE OR REPLACE TEMPORARY FUNCTION foo3_3b(x INT) RETURNS DOUBLE RETURN 1 / x;
-- Expect null
SELECT foo3_3a(0);
SELECT foo3_3b(0);
SELECT * FROM foo3_3at(0);

-- Cast inside the UDF should respect the captured SQL configurations
-- Explicit cast
CREATE FUNCTION foo3_3c() RETURNS INT RETURN CAST('a' AS INT);
CREATE FUNCTION foo3_3ct() RETURNS TABLE (a INT) RETURN SELECT CAST('a' AS INT);
-- Implicit cast
CREATE FUNCTION foo3_3d() RETURNS INT RETURN 'a' + 1;
CREATE FUNCTION foo3_3dt() RETURNS TABLE (a INT) RETURN SELECT 'a' + 1;
-- Expect null
SELECT foo3_3c();
SELECT foo3_3d();
SELECT * FROM foo3_3ct();
SELECT * FROM foo3_3dt();
SET spark.sql.ansi.enabled=true;
-- Expect null
SELECT foo3_3c();
SELECT foo3_3d();
SELECT * FROM foo3_3ct();
SELECT * FROM foo3_3dt();
RESET spark.sql.ansi.enabled;

-- 3.14 Invalid usage of SQL scalar/table functions in query clauses.
CREATE FUNCTION foo3_14a() RETURNS INT RETURN 1;
CREATE FUNCTION foo3_14b() RETURNS TABLE (a INT) RETURN SELECT 1;
-- Expect error
SELECT * FROM foo3_14a();
SELECT foo3_14b();

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

DROP FUNCTION IF EXISTS foo1a0;
DROP FUNCTION IF EXISTS foo1a1;
DROP FUNCTION IF EXISTS foo1a2;
DROP FUNCTION IF EXISTS foo1b0;
DROP FUNCTION IF EXISTS foo1b1;
DROP FUNCTION IF EXISTS foo1b2;
DROP FUNCTION IF EXISTS foo1c1;
DROP FUNCTION IF EXISTS foo1c2;
DROP FUNCTION IF EXISTS foo1d1;
DROP FUNCTION IF EXISTS foo1d2;
DROP FUNCTION IF EXISTS foo1d4;
DROP FUNCTION IF EXISTS foo1d5;
DROP FUNCTION IF EXISTS foo1d6;
DROP FUNCTION IF EXISTS foo1e1;
DROP FUNCTION IF EXISTS foo1e2;
DROP FUNCTION IF EXISTS foo1e3;
DROP FUNCTION IF EXISTS foo1f1;
DROP FUNCTION IF EXISTS foo1f2;
DROP FUNCTION IF EXISTS foo1g1;
DROP FUNCTION IF EXISTS foo1g2;
DROP FUNCTION IF EXISTS foo2a0;
DROP FUNCTION IF EXISTS foo2a2;
DROP FUNCTION IF EXISTS foo2a4;
DROP FUNCTION IF EXISTS foo2b1;
DROP FUNCTION IF EXISTS foo2b2;
DROP FUNCTION IF EXISTS foo2c1;
DROP FUNCTION IF EXISTS foo31;
DROP FUNCTION IF EXISTS foo32;
DROP FUNCTION IF EXISTS foo33;
DROP FUNCTION IF EXISTS foo41;
DROP FUNCTION IF EXISTS foo42;
DROP FUNCTION IF EXISTS foo51;
DROP FUNCTION IF EXISTS foo52;
DROP FUNCTION IF EXISTS foo6c;
DROP FUNCTION IF EXISTS foo6d;
DROP FUNCTION IF EXISTS foo7a;
DROP FUNCTION IF EXISTS foo7at;
DROP FUNCTION IF EXISTS foo9a;
DROP FUNCTION IF EXISTS foo9b;
DROP FUNCTION IF EXISTS foo9c;
DROP FUNCTION IF EXISTS foo9d;
DROP FUNCTION IF EXISTS foo9e;
DROP FUNCTION IF EXISTS foo9f;
DROP FUNCTION IF EXISTS foo9g;
DROP FUNCTION IF EXISTS foo9h;
DROP FUNCTION IF EXISTS foo9i;
DROP FUNCTION IF EXISTS foo9j;
DROP FUNCTION IF EXISTS foo9l;
DROP FUNCTION IF EXISTS foo9m;
DROP FUNCTION IF EXISTS foo9n;
DROP FUNCTION IF EXISTS foo9o;
DROP FUNCTION IF EXISTS foo9p;
DROP FUNCTION IF EXISTS foo9q;
DROP FUNCTION IF EXISTS foo9r;
DROP FUNCTION IF EXISTS foo1_10;
DROP FUNCTION IF EXISTS bar1_10;
DROP FUNCTION IF EXISTS foo1_11a;
DROP FUNCTION IF EXISTS foo1_11b;
DROP FUNCTION IF EXISTS foo1_11c;
DROP FUNCTION IF EXISTS foo1_11d;
DROP FUNCTION IF EXISTS foo1_11e;
DROP FUNCTION IF EXISTS foo1_11f;
DROP FUNCTION IF EXISTS foo1_11g;
DROP FUNCTION IF EXISTS foo1_11h;
DROP FUNCTION IF EXISTS foo1_11i;
DROP FUNCTION IF EXISTS foo1_11j;
DROP FUNCTION IF EXISTS foo1_11k;
DROP FUNCTION IF EXISTS foo1_11l;
DROP FUNCTION IF EXISTS foo1_11m;
DROP FUNCTION IF EXISTS foo1_11n;
DROP FUNCTION IF EXISTS foo1_11o;
DROP FUNCTION IF EXISTS foo2_1a;
DROP FUNCTION IF EXISTS foo2_1b;
DROP FUNCTION IF EXISTS foo2_1c;
DROP FUNCTION IF EXISTS foo2_1d;
DROP FUNCTION IF EXISTS foo2_2a;
DROP FUNCTION IF EXISTS foo2_2b;
DROP FUNCTION IF EXISTS foo2_2c;
DROP FUNCTION IF EXISTS foo2_2d;
DROP FUNCTION IF EXISTS foo2_2e;
DROP FUNCTION IF EXISTS foo2_2f;
DROP FUNCTION IF EXISTS foo2_2g;
DROP FUNCTION IF EXISTS foo2_3;
DROP FUNCTION IF EXISTS foo2_4a;
DROP FUNCTION IF EXISTS foo2_4b;
DROP FUNCTION IF EXISTS foo3_1a;
DROP FUNCTION IF EXISTS foo3_1b;
DROP FUNCTION IF EXISTS foo3_1c;
DROP FUNCTION IF EXISTS foo3_1d;
DROP FUNCTION IF EXISTS foo3_1e;
DROP FUNCTION IF EXISTS foo3_1f;
DROP FUNCTION IF EXISTS foo3_1g;
DROP FUNCTION IF EXISTS foo3_1x;
DROP FUNCTION IF EXISTS foo3_1y;
DROP FUNCTION IF EXISTS foo3_2a;
DROP FUNCTION IF EXISTS foo3_2b1;
DROP FUNCTION IF EXISTS foo3_2b2;
DROP FUNCTION IF EXISTS foo3_2b3;
DROP FUNCTION IF EXISTS foo3_2b4;
DROP FUNCTION IF EXISTS foo3_2b5;
DROP FUNCTION IF EXISTS foo3_2c1;
DROP FUNCTION IF EXISTS foo3_2c2;
DROP FUNCTION IF EXISTS foo3_2d1;
DROP FUNCTION IF EXISTS foo3_2d2;
DROP FUNCTION IF EXISTS foo3_2e1;
DROP FUNCTION IF EXISTS foo3_3a;
DROP FUNCTION IF EXISTS foo3_3at;
DROP FUNCTION IF EXISTS foo3_14a;
DROP FUNCTION IF EXISTS foo3_14b;
DROP FUNCTION IF EXISTS foo3_3c;
DROP FUNCTION IF EXISTS foo3_3ct;
DROP FUNCTION IF EXISTS foo3_3d;
DROP FUNCTION IF EXISTS foo3_3dt;
DROP FUNCTION IF EXISTS foo4_0;
DROP FUNCTION IF EXISTS foo4_1;
DROP FUNCTION IF EXISTS foo4_2;
DROP FUNCTION IF EXISTS foo4_3;
