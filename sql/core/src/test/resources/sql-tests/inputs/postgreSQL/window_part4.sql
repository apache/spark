-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
-- Window Functions Testing
-- https://github.com/postgres/postgres/blob/REL_12_STABLE/src/test/regress/sql/window.sql#L913-L1278

-- Test window operator with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

-- Spark doesn't handle UDFs in SQL
-- test user-defined window function with named args and default args
-- CREATE FUNCTION nth_value_def(val anyelement, n integer = 1) RETURNS anyelement
--   LANGUAGE internal WINDOW IMMUTABLE STRICT AS 'window_nth_value';

-- Spark doesn't handle UDFs in SQL
-- SELECT nth_value_def(n := 2, val := ten) OVER (PARTITION BY four), ten, four
--   FROM (SELECT * FROM tenk1 WHERE unique2 < 10 ORDER BY four, ten) s;

-- Spark doesn't handle UDFs in SQL
-- SELECT nth_value_def(ten) OVER (PARTITION BY four), ten, four
--   FROM (SELECT * FROM tenk1 WHERE unique2 < 10 ORDER BY four, ten) s;

--
-- Test the basic moving-aggregate machinery
--

-- create aggregates that record the series of transform calls (these are
-- intentionally not true inverses)

-- Spark doesn't handle UDFs in SQL
-- CREATE FUNCTION logging_sfunc_nonstrict(text, anyelement) RETURNS text AS
-- $$ SELECT COALESCE($1, '') || '*' || quote_nullable($2) $$
-- LANGUAGE SQL IMMUTABLE;

-- Spark doesn't handle UDFs in SQL
-- CREATE FUNCTION logging_msfunc_nonstrict(text, anyelement) RETURNS text AS
-- $$ SELECT COALESCE($1, '') || '+' || quote_nullable($2) $$
-- LANGUAGE SQL IMMUTABLE;

-- Spark doesn't handle UDFs in SQL
-- CREATE FUNCTION logging_minvfunc_nonstrict(text, anyelement) RETURNS text AS
-- $$ SELECT $1 || '-' || quote_nullable($2) $$
-- LANGUAGE SQL IMMUTABLE;

-- Spark doesn't handle UDFs in SQL
-- CREATE AGGREGATE logging_agg_nonstrict (anyelement)
-- (
-- 	stype = text,
-- 	sfunc = logging_sfunc_nonstrict,
-- 	mstype = text,
-- 	msfunc = logging_msfunc_nonstrict,
-- 	minvfunc = logging_minvfunc_nonstrict
-- );

-- Spark doesn't handle UDFs in SQL
-- CREATE AGGREGATE logging_agg_nonstrict_initcond (anyelement)
-- (
-- 	stype = text,
-- 	sfunc = logging_sfunc_nonstrict,
-- 	mstype = text,
-- 	msfunc = logging_msfunc_nonstrict,
-- 	minvfunc = logging_minvfunc_nonstrict,
-- 	initcond = 'I',
-- 	minitcond = 'MI'
-- );

-- Spark doesn't handle UDFs in SQL
-- CREATE FUNCTION logging_sfunc_strict(text, anyelement) RETURNS text AS
-- $$ SELECT $1 || '*' || quote_nullable($2) $$
-- LANGUAGE SQL STRICT IMMUTABLE;

-- Spark doesn't handle UDFs in SQL
-- CREATE FUNCTION logging_msfunc_strict(text, anyelement) RETURNS text AS
-- $$ SELECT $1 || '+' || quote_nullable($2) $$
-- LANGUAGE SQL STRICT IMMUTABLE;

-- Spark doesn't handle UDFs in SQL
-- CREATE FUNCTION logging_minvfunc_strict(text, anyelement) RETURNS text AS
-- $$ SELECT $1 || '-' || quote_nullable($2) $$
-- LANGUAGE SQL STRICT IMMUTABLE;

-- Spark doesn't handle UDFs in SQL
-- CREATE AGGREGATE logging_agg_strict (text)
-- (
-- 	stype = text,
-- 	sfunc = logging_sfunc_strict,
-- 	mstype = text,
-- 	msfunc = logging_msfunc_strict,
-- 	minvfunc = logging_minvfunc_strict
-- );

-- Spark doesn't handle UDFs in SQL
-- CREATE AGGREGATE logging_agg_strict_initcond (anyelement)
-- (
-- 	stype = text,
-- 	sfunc = logging_sfunc_strict,
-- 	mstype = text,
-- 	msfunc = logging_msfunc_strict,
-- 	minvfunc = logging_minvfunc_strict,
-- 	initcond = 'I',
-- 	minitcond = 'MI'
-- );

-- Spark doesn't handle UDFs in SQL
-- test strict and non-strict cases
-- SELECT
-- 	p::text || ',' || i::text || ':' || COALESCE(v::text, 'NULL') AS row,
-- 	logging_agg_nonstrict(v) over wnd as nstrict,
-- 	logging_agg_nonstrict_initcond(v) over wnd as nstrict_init,
-- 	logging_agg_strict(v::text) over wnd as strict,
-- 	logging_agg_strict_initcond(v) over wnd as strict_init
-- FROM (VALUES
-- 	(1, 1, NULL),
-- 	(1, 2, 'a'),
-- 	(1, 3, 'b'),
-- 	(1, 4, NULL),
-- 	(1, 5, NULL),
-- 	(1, 6, 'c'),
-- 	(2, 1, NULL),
-- 	(2, 2, 'x'),
-- 	(3, 1, 'z')
-- ) AS t(p, i, v)
-- WINDOW wnd AS (PARTITION BY P ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
-- ORDER BY p, i;

-- Spark doesn't handle UDFs in SQL
-- and again, but with filter
-- SELECT
-- 	p::text || ',' || i::text || ':' ||
-- 		CASE WHEN f THEN COALESCE(v::text, 'NULL') ELSE '-' END as row,
-- 	logging_agg_nonstrict(v) filter(where f) over wnd as nstrict_filt,
-- 	logging_agg_nonstrict_initcond(v) filter(where f) over wnd as nstrict_init_filt,
-- 	logging_agg_strict(v::text) filter(where f) over wnd as strict_filt,
-- 	logging_agg_strict_initcond(v) filter(where f) over wnd as strict_init_filt
-- FROM (VALUES
-- 	(1, 1, true,  NULL),
-- 	(1, 2, false, 'a'),
-- 	(1, 3, true,  'b'),
-- 	(1, 4, false, NULL),
-- 	(1, 5, false, NULL),
-- 	(1, 6, false, 'c'),
-- 	(2, 1, false, NULL),
-- 	(2, 2, true,  'x'),
-- 	(3, 1, true,  'z')
-- ) AS t(p, i, f, v)
-- WINDOW wnd AS (PARTITION BY p ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
-- ORDER BY p, i;

-- Spark doesn't handle UDFs in SQL
-- test that volatile arguments disable moving-aggregate mode
-- SELECT
-- 	i::text || ':' || COALESCE(v::text, 'NULL') as row,
-- 	logging_agg_strict(v::text)
-- 		over wnd as inverse,
-- 	logging_agg_strict(v::text || CASE WHEN random() < 0 then '?' ELSE '' END)
-- 		over wnd as noinverse
-- FROM (VALUES
-- 	(1, 'a'),
-- 	(2, 'b'),
-- 	(3, 'c')
-- ) AS t(i, v)
-- WINDOW wnd AS (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
-- ORDER BY i;

-- Spark doesn't handle UDFs in SQL
-- SELECT
-- 	i::text || ':' || COALESCE(v::text, 'NULL') as row,
-- 	logging_agg_strict(v::text) filter(where true)
-- 		over wnd as inverse,
-- 	logging_agg_strict(v::text) filter(where random() >= 0)
-- 		over wnd as noinverse
-- FROM (VALUES
-- 	(1, 'a'),
-- 	(2, 'b'),
-- 	(3, 'c')
-- ) AS t(i, v)
-- WINDOW wnd AS (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
-- ORDER BY i;

-- Spark doesn't handle UDFs in SQL
-- test that non-overlapping windows don't use inverse transitions
-- SELECT
-- 	logging_agg_strict(v::text) OVER wnd
-- FROM (VALUES
-- 	(1, 'a'),
-- 	(2, 'b'),
-- 	(3, 'c')
-- ) AS t(i, v)
-- WINDOW wnd AS (ORDER BY i ROWS BETWEEN CURRENT ROW AND CURRENT ROW)
-- ORDER BY i;

-- Spark doesn't handle UDFs in SQL
-- test that returning NULL from the inverse transition functions
-- restarts the aggregation from scratch. The second aggregate is supposed
-- to test cases where only some aggregates restart, the third one checks
-- that one aggregate restarting doesn't cause others to restart.

-- Spark doesn't handle UDFs in SQL
-- CREATE FUNCTION sum_int_randrestart_minvfunc(int4, int4) RETURNS int4 AS
-- $$ SELECT CASE WHEN random() < 0.2 THEN NULL ELSE $1 - $2 END $$
-- LANGUAGE SQL STRICT;

-- Spark doesn't handle UDFs in SQL
-- CREATE AGGREGATE sum_int_randomrestart (int4)
-- (
-- 	stype = int4,
-- 	sfunc = int4pl,
-- 	mstype = int4,
-- 	msfunc = int4pl,
-- 	minvfunc = sum_int_randrestart_minvfunc
-- );

-- Spark doesn't handle UDFs in SQL
-- WITH
-- vs AS (
-- 	SELECT i, (random() * 100)::int4 AS v
-- 	FROM generate_series(1, 100) AS i
-- ),
-- sum_following AS (
-- 	SELECT i, SUM(v) OVER
-- 		(ORDER BY i DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s
-- 	FROM vs
-- )
-- SELECT DISTINCT
-- 	sum_following.s = sum_int_randomrestart(v) OVER fwd AS eq1,
-- 	-sum_following.s = sum_int_randomrestart(-v) OVER fwd AS eq2,
-- 	100*3+(vs.i-1)*3 = length(logging_agg_nonstrict(''::text) OVER fwd) AS eq3
-- FROM vs
-- JOIN sum_following ON sum_following.i = vs.i
-- WINDOW fwd AS (
-- 	ORDER BY vs.i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
-- );

--
-- Test various built-in aggregates that have moving-aggregate support
--

-- test inverse transition functions handle NULLs properly
SELECT i,AVG(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,AVG(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,AVG(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,AVG(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1.5),(2,2.5),(3,NULL),(4,NULL)) t(i,v);

-- [SPARK-28602] Spark does not recognize 'interval' type as 'numeric'
-- SELECT i,AVG(v::interval) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
--   FROM (VALUES(1,'1 sec'),(2,'2 sec'),(3,NULL),(4,NULL)) t(i,v);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

-- The cast syntax is present in PgSQL for legacy reasons and Spark will not recognize a money field
-- SELECT i,SUM(v::money) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
--   FROM (VALUES(1,'1.10'),(2,'2.20'),(3,NULL),(4,NULL)) t(i,v);

-- [SPARK-28602] Spark does not recognize 'interval' type as 'numeric'
-- SELECT i,SUM(cast(v as interval)) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
--   FROM (VALUES(1,'1 sec'),(2,'2 sec'),(3,NULL),(4,NULL)) t(i,v);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1.1),(2,2.2),(3,NULL),(4,NULL)) t(i,v);

SELECT SUM(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1.01),(2,2),(3,3)) v(i,n);

SELECT i,COUNT(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,COUNT(*) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT VAR_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VAR_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VAR_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VAR_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VAR_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VAR_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VAR_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VAR_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VARIANCE(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VARIANCE(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VARIANCE(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT VARIANCE(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT STDDEV_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV_POP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

-- For the following queries Spark result differs from PgSQL:
-- Spark handles division by zero as 'NaN' instead of 'NULL', which is the PgSQL behaviour
SELECT STDDEV_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV_SAMP(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,NULL),(2,600),(3,470),(4,170),(5,430),(6,300)) r(i,n);

SELECT STDDEV(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(0,NULL),(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT STDDEV(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(0,NULL),(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT STDDEV(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(0,NULL),(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

SELECT STDDEV(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(0,NULL),(1,600),(2,470),(3,170),(4,430),(5,300)) r(i,n);

-- test that inverse transition functions work with various frame options
SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND CURRENT ROW)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

SELECT i,SUM(v) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,3),(4,4)) t(i,v);

-- [SPARK-29638] Spark handles 'NaN' as 0 in sums
-- ensure aggregate over numeric properly recovers from NaN values
SELECT a, b,
       SUM(b) OVER(ORDER BY A ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
FROM (VALUES(1,1),(2,2),(3,(cast('nan' as int))),(4,3),(5,4)) t(a,b);

-- It might be tempting for someone to add an inverse trans function for
-- float and double precision. This should not be done as it can give incorrect
-- results. This test should fail if anyone ever does this without thinking too
-- hard about it.
-- [SPARK-28516] adds `to_char`
-- SELECT to_char(SUM(n) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING),'999999999999999999999D9')
--   FROM (VALUES(1,1e20),(2,1)) n(i,n);

-- [SPARK-27880] Implement boolean aggregates(BOOL_AND, BOOL_OR and EVERY)
-- SELECT i, b, bool_and(b) OVER w, bool_or(b) OVER w
--   FROM (VALUES (1,true), (2,true), (3,false), (4,false), (5,true)) v(i,b)
--   WINDOW w AS (ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING);

-- Tests for problems with failure to walk or mutate expressions
-- within window frame clauses.

-- [SPARK-37612] Support window frame ORDER BY i ROWS BETWEEN (('foo' < 'foobar')::integer) PRECEDING AND CURRENT ROW
-- test walker (fails with collation error if expressions are not walked)
-- SELECT array_agg(i) OVER w
--   FROM range(1,6) i
-- WINDOW w AS (ORDER BY i ROWS BETWEEN (('foo' < 'foobar')::integer) PRECEDING AND CURRENT ROW);

-- Spark doesn't handle UDFs in SQL
-- test mutator (fails when inlined if expressions are not mutated)
-- CREATE FUNCTION pg_temp.f(group_size BIGINT) RETURNS SETOF integer[]
-- AS $$
--     SELECT array_agg(s) OVER w
--       FROM generate_series(1,5) s
--     WINDOW w AS (ORDER BY s ROWS BETWEEN CURRENT ROW AND GROUP_SIZE FOLLOWING)
-- $$ LANGUAGE SQL STABLE;
