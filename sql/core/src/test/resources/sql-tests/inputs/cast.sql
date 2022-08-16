-- cast string representing a valid fractional number to integral should truncate the number
SELECT CAST('1.23' AS int);
SELECT CAST('1.23' AS long);
SELECT CAST('-4.56' AS int);
SELECT CAST('-4.56' AS long);

-- cast string which are not numbers to numeric types
SELECT CAST('abc' AS int);
SELECT CAST('abc' AS long);
SELECT CAST('abc' AS float);
SELECT CAST('abc' AS double);

-- cast string representing a very large number to integral should return null
SELECT CAST('1234567890123' AS int);
SELECT CAST('12345678901234567890123' AS long);

-- cast empty string to integral should return null
SELECT CAST('' AS int);
SELECT CAST('' AS long);
SELECT CAST('' AS float);
SELECT CAST('' AS double);

-- cast null to integral should return null
SELECT CAST(NULL AS int);
SELECT CAST(NULL AS long);

-- cast invalid decimal string to numeric types
SELECT CAST('123.a' AS int);
SELECT CAST('123.a' AS long);
SELECT CAST('123.a' AS float);
SELECT CAST('123.a' AS double);

-- '-2147483648' is the smallest int value
SELECT CAST('-2147483648' AS int);
SELECT CAST('-2147483649' AS int);

-- '2147483647' is the largest int value
SELECT CAST('2147483647' AS int);
SELECT CAST('2147483648' AS int);

-- '-9223372036854775808' is the smallest long value
SELECT CAST('-9223372036854775808' AS long);
SELECT CAST('-9223372036854775809' AS long);

-- '9223372036854775807' is the largest long value
SELECT CAST('9223372036854775807' AS long);
SELECT CAST('9223372036854775808' AS long);

-- cast string to its binary representation
SELECT HEX(CAST('abc' AS binary));

-- cast integral values to their corresponding binary representation
SELECT HEX(CAST(CAST(123 AS byte) AS binary));
SELECT HEX(CAST(CAST(-123 AS byte) AS binary));
SELECT HEX(CAST(123S AS binary));
SELECT HEX(CAST(-123S AS binary));
SELECT HEX(CAST(123 AS binary));
SELECT HEX(CAST(-123 AS binary));
SELECT HEX(CAST(123L AS binary));
SELECT HEX(CAST(-123L AS binary));

DESC FUNCTION boolean;
DESC FUNCTION EXTENDED boolean;
-- TODO: migrate all cast tests here.

-- cast string to interval and interval to string
SELECT CAST('interval 3 month 1 hour' AS interval);
SELECT CAST("interval '3-1' year to month" AS interval year to month);
SELECT CAST("interval '3 00:00:01' day to second" AS interval day to second);
SELECT CAST(interval 3 month 1 hour AS string);
SELECT CAST(interval 3 year 1 month AS string);
SELECT CAST(interval 3 day 1 second AS string);

-- trim string before cast to numeric
select cast(' 1' as tinyint);
select cast(' 1\t' as tinyint);
select cast(' 1' as smallint);
select cast(' 1' as INT);
select cast(' 1' as bigint);
select cast(' 1' as float);
select cast(' 1 ' as DOUBLE);
select cast('1.0 ' as DEC);
select cast('1中文' as tinyint);
select cast('1中文' as smallint);
select cast('1中文' as INT);
select cast('中文1' as bigint);
select cast('1中文' as bigint);

-- trim string before cast to boolean
select cast('\t\t true \n\r ' as boolean);
select cast('\t\n false \t\r' as boolean);
select cast('\t\n xyz \t\r' as boolean);

select cast('23.45' as decimal(4, 2));
select cast('123.45' as decimal(4, 2));
select cast('xyz' as decimal(4, 2));

select cast('2022-01-01' as date);
select cast('a' as date);
select cast('2022-01-01 00:00:00' as timestamp);
select cast('a' as timestamp);
select cast('2022-01-01 00:00:00' as timestamp_ntz);
select cast('a' as timestamp_ntz);

select cast(cast('inf' as double) as timestamp);
select cast(cast('inf' as float) as timestamp);

-- cast ANSI intervals to integrals
select cast(interval '1' year as tinyint);
select cast(interval '-10-2' year to month as smallint);
select cast(interval '1000' month as int);
select cast(interval -'10.123456' second as tinyint);
select cast(interval '23:59:59' hour to second as smallint);
select cast(interval -'1 02:03:04.123' day to second as int);
select cast(interval '10' day as bigint);

select cast(interval '-1000' month as tinyint);
select cast(interval '1000000' second as smallint);

-- cast integrals to ANSI intervals
select cast(1Y as interval year);
select cast(-122S as interval year to month);
select cast(ym as interval year to month) from values(-122S) as t(ym);
select cast(1000 as interval month);
select cast(-10L as interval second);
select cast(100Y as interval hour to second);
select cast(dt as interval hour to second) from values(100Y) as t(dt);
select cast(-1000S as interval day to second);
select cast(10 as interval day);

select cast(2147483647 as interval year);
select cast(-9223372036854775808L as interval day);

-- cast ANSI intervals to decimals
select cast(interval '-1' year as decimal(10, 0));
select cast(interval '1.000001' second as decimal(10, 6));
select cast(interval '08:11:10.001' hour to second as decimal(10, 4));
select cast(interval '1 01:02:03.1' day to second as decimal(8, 1));
select cast(interval '10.123' second as decimal(4, 2));
select cast(interval '10.005' second as decimal(4, 2));
select cast(interval '10.123' second as decimal(5, 2));
select cast(interval '10.123' second as decimal(1, 0));

-- cast decimals to ANSI intervals
select cast(10.123456BD as interval day to second);
select cast(80.654321BD as interval hour to minute);
select cast(-10.123456BD as interval year to month);
select cast(10.654321BD as interval month);
