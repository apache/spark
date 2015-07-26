set hive.fetch.task.conversion=more;

use default;
-- Test format_number() UDF

DESCRIBE FUNCTION format_number;
DESCRIBE FUNCTION EXTENDED format_number;

EXPLAIN
SELECT format_number(12332.123456, 4),
    format_number(12332.1,4),
    format_number(12332.2,0) FROM src tablesample (1 rows);

SELECT format_number(12332.123456, 4),
    format_number(12332.1,4),
    format_number(12332.2,0)
FROM src tablesample (1 rows);

-- positive numbers
SELECT format_number(0.123456789, 12),
    format_number(12345678.123456789, 5),
    format_number(1234567.123456789, 7),
    format_number(123456.123456789, 0)
FROM src tablesample (1 rows);

-- negative numbers
SELECT format_number(-123456.123456789, 0),
    format_number(-1234567.123456789, 2),
    format_number(-0.123456789, 15),
    format_number(-12345.123456789, 4)
FROM src tablesample (1 rows);

-- zeros
SELECT format_number(0.0, 4),
    format_number(0.000000, 1),
    format_number(000.0000, 1),
    format_number(00000.0000, 1),
    format_number(-00.0, 4)
FROM src tablesample (1 rows);

-- integers
SELECT format_number(0, 0),
    format_number(1, 4),
    format_number(12, 2),
    format_number(123, 5),
    format_number(1234, 7)
FROM src tablesample (1 rows);

-- long and double boundary
-- 9223372036854775807 is LONG_MAX
-- -9223372036854775807 is one more than LONG_MIN,
-- due to HIVE-2733, put it here to check LONG_MIN boundary
-- 4.9E-324 and 1.7976931348623157E308 are Double.MIN_VALUE and Double.MAX_VALUE
-- check them for Double boundary
SELECT format_number(-9223372036854775807, 10),
    format_number(9223372036854775807, 20),
    format_number(4.9E-324, 324),
    format_number(1.7976931348623157E308, 308)
FROM src tablesample (1 rows);
