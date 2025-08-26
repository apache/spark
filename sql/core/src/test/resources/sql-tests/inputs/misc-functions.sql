-- test for misc functions

-- typeof
select typeof(null);
select typeof(true);
select typeof(1Y), typeof(1S), typeof(1), typeof(1L);
select typeof(cast(1.0 as float)), typeof(1.0D), typeof(1.2);
select typeof(date '1986-05-23'),  typeof(timestamp '1986-05-23'), typeof(interval '23 days');
select typeof(x'ABCD'), typeof('SPARK');
select typeof(array(1, 2)), typeof(map(1, 2)), typeof(named_struct('a', 1, 'b', 'spark'));

-- Spark-32793: Rewrite AssertTrue with RaiseError
SELECT assert_true(true), assert_true(boolean(1));
SELECT assert_true(false);
SELECT assert_true(boolean(0));
SELECT assert_true(null);
SELECT assert_true(boolean(null));
SELECT assert_true(false, 'custom error message');

CREATE TEMPORARY VIEW tbl_misc AS SELECT * FROM (VALUES (1), (8), (2)) AS T(v);
SELECT raise_error('error message');
SELECT if(v > 5, raise_error('too big: ' || v), v + 1) FROM tbl_misc;

-- Too many parameters
SELECT raise_error('error message', Map());

-- Too many parameters
SELECT raise_error('error message', 'some args');

-- Too few parameters
SELECT raise_error();

-- Passing null as message
SELECT raise_error(NULL);

-- Passing non-string type
SELECT raise_error(1);

-- Check legacy config disables printing of [USER_RAISED_EXCEPTION]
SET spark.sql.legacy.raiseErrorWithoutErrorClass=true;
SELECT assert_true(false);
SELECT raise_error('hello');
SET spark.sql.legacy.raiseErrorWithoutErrorClass=false;

-- Implicit alias of assert expression
SELECT assert_true(col1 <= col2) FROM VALUES ('2025-03-01', '2025-03-10');
