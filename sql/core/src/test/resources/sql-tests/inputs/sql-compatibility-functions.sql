-- A test suite for functions added for compatibility with other databases such as Oracle, MSSQL.
-- These functions are typically implemented using the trait RuntimeReplaceable.

SELECT ifnull(null, 'x'), ifnull('y', 'x'), ifnull(null, null);
SELECT nullif('x', 'x'), nullif('x', 'y');
SELECT nvl(null, 'x'), nvl('y', 'x'), nvl(null, null);
SELECT nvl2(null, 'x', 'y'), nvl2('n', 'x', 'y'), nvl2(null, null, null);

-- type coercion
SELECT ifnull(1, 2.1d), ifnull(null, 2.1d);
SELECT nullif(1, 2.1d), nullif(1, 1.0d);
SELECT nvl(1, 2.1d), nvl(null, 2.1d);
SELECT nvl2(null, 1, 2.1d), nvl2('n', 1, 2.1d);

-- explain for these functions; use range to avoid constant folding
explain extended
select ifnull(id, 'x'), nullif(id, 'x'), nvl(id, 'x'), nvl2(id, 'x', 'y')
from range(2);

-- SPARK-16730 cast alias functions for Hive compatibility
SELECT boolean(1), tinyint(1), smallint(1), int(1), bigint(1);
SELECT float(1), double(1), decimal(1);
SELECT date("2014-04-04"), timestamp(date("2014-04-04"));
-- error handling: only one argument
SELECT string(1, 2);
