-- cast string representing a valid fractional number to integral should truncate the number
SELECT CAST('1.23' AS int);
SELECT CAST('1.23' AS long);
SELECT CAST('-4.56' AS int);
SELECT CAST('-4.56' AS long);

-- cast string which are not numbers to integral should return null
SELECT CAST('abc' AS int);
SELECT CAST('abc' AS long);

-- cast string representing a very large number to integral should return null
SELECT CAST('1234567890123' AS int);
SELECT CAST('12345678901234567890123' AS long);

-- cast empty string to integral should return null
SELECT CAST('' AS int);
SELECT CAST('' AS long);

-- cast null to integral should return null
SELECT CAST(NULL AS int);
SELECT CAST(NULL AS long);

-- cast invalid decimal string to integral should return null
SELECT CAST('123.a' AS int);
SELECT CAST('123.a' AS long);

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
SELECT CAST(interval 3 month 1 hour AS string);

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
