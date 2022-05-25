-- SPARK-23179: SQL ANSI 2011 states that in case of overflow during arithmetic operations,
-- an exception should be thrown instead of returning NULL.
-- This is what most of the SQL DBs do (e.g. SQLServer, DB2).

-- tests for decimals handling in operations
create table decimals_test(id int, a decimal(38,18), b decimal(38,18)) using parquet;

insert into decimals_test values(1, 100.0, 999.0), (2, 12345.123, 12345.123),
  (3, 0.1234567891011, 1234.1), (4, 123456789123456789.0, 1.123456789123456789);

-- test operations between decimals and constants
select id, a*10, b/10 from decimals_test order by id;

-- test operations on constants
select 10.3 * 3.0;
select 10.3000 * 3.0;
select 10.30000 * 30.0;
select 10.300000000000000000 * 3.000000000000000000;
select 10.300000000000000000 * 3.0000000000000000000;

-- arithmetic operations causing an overflow throw exception
select (5e36BD + 0.1) + 5e36BD;
select (-4e36BD - 0.1) - 7e36BD;
select 12345678901234567890.0 * 12345678901234567890.0;
select 1e35BD / 0.1;

-- arithmetic operations causing a precision loss throw exception
select 123456789123456789.1234567890 * 1.123456789123456789;
select 123456789123456789.1234567890 * 1.123456789123456789;
select 12345678912345.123456789123 / 0.000000012345678;

select 1.0123456789012345678901234567890123456e36BD / 0.1;
select 1.0123456789012345678901234567890123456e35BD / 1.0;
select 1.0123456789012345678901234567890123456e34BD / 1.0;
select 1.0123456789012345678901234567890123456e33BD / 1.0;
select 1.0123456789012345678901234567890123456e32BD / 1.0;
select 1.0123456789012345678901234567890123456e31BD / 1.0;
select 1.0123456789012345678901234567890123456e31BD / 0.1;
select 1.0123456789012345678901234567890123456e31BD / 10.0;

drop table decimals_test;
