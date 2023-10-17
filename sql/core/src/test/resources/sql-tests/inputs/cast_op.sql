-- cast string representing a valid fractional number to integral should truncate the number
SELECT '1.23' :: int;
SELECT '1.23' :: long;
SELECT '-4.56' :: int;
SELECT '-4.56' :: long;

-- cast string which are not numbers to numeric types
SELECT 'abc' :: int;
SELECT 'abc' :: long;
SELECT 'abc' :: float;
SELECT 'abc' :: double;

-- cast string representing a very large number to integral should return null
SELECT '1234567890123' :: int;
SELECT '12345678901234567890123' :: long;

-- cast empty string to integral should return null
SELECT '' :: int;
SELECT '' :: long;
SELECT '' :: float;
SELECT '' :: double;

-- cast null to integral should return null
SELECT NULL :: int;
SELECT NULL :: long;

-- cast invalid decimal string to numeric types
SELECT '123.a' :: int;
SELECT '123.a' :: long;
SELECT '123.a' :: float;
SELECT '123.a' :: double;

-- '-2147483648' is the smallest int value
SELECT '-2147483648' :: int;
SELECT '-2147483649' :: int;

-- '2147483647' is the largest int value
SELECT '2147483647' :: int;
SELECT '2147483648' :: int;

-- '-9223372036854775808' is the smallest long value
SELECT '-9223372036854775808' :: long;
SELECT '-9223372036854775809' :: long;

-- '9223372036854775807' is the largest long value
SELECT '9223372036854775807' :: long;
SELECT '9223372036854775808' :: long;

-- cast string to its binary representation
SELECT HEX('abc' :: binary);

-- cast integral values to their corresponding binary representation
SELECT HEX((123 :: byte) :: binary);
SELECT HEX((-123 :: byte) :: binary);
SELECT HEX(123S :: binary);
SELECT HEX(-123S :: binary);
SELECT HEX(123 :: binary);
SELECT HEX(-123 :: binary);
SELECT HEX(123L :: binary);
SELECT HEX(-123L :: binary);

DESC FUNCTION boolean;
DESC FUNCTION EXTENDED boolean;
-- TODO: migrate all cast tests here.

-- cast string to interval and interval to string
SELECT 'interval 3 month 1 hour' :: interval;
SELECT "interval '3-1' year to month" :: interval year to month;
SELECT "interval '3 00:00:01' day to second" :: interval day to second;
SELECT interval 3 month 1 hour :: string;
SELECT interval 3 year 1 month :: string;
SELECT interval 3 day 1 second :: string;

-- trim string before cast to numeric
select ' 1' :: tinyint;
select ' 1\t' :: tinyint;
select ' 1' :: smallint;
select ' 1' :: INT;
select ' 1' :: bigint;
select ' 1' :: float;
select ' 1 ' :: DOUBLE;
select '1.0 ' :: DEC;
select '1中文' :: tinyint;
select '1中文' :: smallint;
select '1中文' :: INT;
select '中文1' :: bigint;
select '1中文' :: bigint;

-- trim string before cast to boolean
select '\t\t true \n\r ' :: boolean;
select '\t\n false \t\r' :: boolean;
select '\t\n xyz \t\r' :: boolean;

select '23.45' :: decimal(4, 2);
select '123.45' :: decimal(4, 2);
select 'xyz' :: decimal(4, 2);

select '2022-01-01' :: date;
select 'a' :: date;
select '2022-01-01 00:00:00' :: timestamp;
select 'a' :: timestamp;
select '2022-01-01 00:00:00' :: timestamp_ntz;
select 'a' :: timestamp_ntz;

select ('inf' :: double) :: timestamp;
select ('inf' :: float) :: timestamp;

-- cast ANSI intervals to integrals
select interval '1' year :: tinyint;
select interval '-10-2' year to month :: smallint;
select interval '1000' month :: int;
select interval -'10.123456' second :: tinyint;
select interval '23:59:59' hour to second :: smallint;
select interval -'1 02:03:04.123' day to second :: int;
select interval '10' day :: bigint;

select interval '-1000' month :: tinyint;
select interval '1000000' second :: smallint;

-- cast integrals to ANSI intervals
select 1Y :: interval year;
select -122S :: interval year to month;
select (ym :: interval year to month) from values(-122S) as t(ym);
select 1000 :: interval month;
select -10L :: interval second;
select 100Y :: interval hour to second;
select (dt :: interval hour to second) from values(100Y) as t(dt);
select -1000S :: interval day to second;
select 10 :: interval day;

select 2147483647 :: interval year;
select -9223372036854775808L :: interval day;

-- cast ANSI intervals to decimals
select interval '-1' year :: decimal(10, 0);
select interval '1.000001' second :: decimal(10, 6);
select interval '08:11:10.001' hour to second :: decimal(10, 4);
select interval '1 01:02:03.1' day to second :: decimal(8, 1);
select interval '10.123' second :: decimal(4, 2);
select interval '10.005' second :: decimal(4, 2);
select interval '10.123' second :: decimal(5, 2);
select interval '10.123' second :: decimal(1, 0);

-- cast decimals to ANSI intervals
select 10.123456BD :: interval day to second;
select 80.654321BD :: interval hour to minute;
select -10.123456BD :: interval year to month;
select 10.654321BD :: interval month;
