-- TRY_CAST string representing a valid fractional number to integral should truncate the number
SELECT TRY_CAST('1.23' AS int);
SELECT TRY_CAST('1.23' AS long);
SELECT TRY_CAST('-4.56' AS int);
SELECT TRY_CAST('-4.56' AS long);

-- TRY_CAST string which are not numbers to integral should return null
SELECT TRY_CAST('abc' AS int);
SELECT TRY_CAST('abc' AS long);

-- TRY_CAST empty string to integral should return null
SELECT TRY_CAST('' AS int);
SELECT TRY_CAST('' AS long);

-- TRY_CAST null to integral should return null
SELECT TRY_CAST(NULL AS int);
SELECT TRY_CAST(NULL AS long);

-- TRY_CAST invalid decimal string to integral should return null
SELECT TRY_CAST('123.a' AS int);
SELECT TRY_CAST('123.a' AS long);

-- '-2147483648' is the smallest int value
SELECT TRY_CAST('-2147483648' AS int);
SELECT TRY_CAST('-2147483649' AS int);

-- '2147483647' is the largest int value
SELECT TRY_CAST('2147483647' AS int);
SELECT TRY_CAST('2147483648' AS int);

-- '-9223372036854775808' is the smallest long value
SELECT TRY_CAST('-9223372036854775808' AS long);
SELECT TRY_CAST('-9223372036854775809' AS long);

-- '9223372036854775807' is the largest long value
SELECT TRY_CAST('9223372036854775807' AS long);
SELECT TRY_CAST('9223372036854775808' AS long);

-- TRY_CAST string to interval and interval to string
SELECT TRY_CAST('interval 3 month 1 hour' AS interval);
SELECT TRY_CAST('abc' AS interval);

-- TRY_CAST string to boolean
select TRY_CAST('true' as boolean);
select TRY_CAST('false' as boolean);
select TRY_CAST('abc' as boolean);

-- TRY_CAST string to date
SELECT TRY_CAST("2021-01-01" AS date);
SELECT TRY_CAST("2021-101-01" AS date);

-- TRY_CAST string to timestamp
SELECT TRY_CAST("2021-01-01 00:00:00" AS timestamp);
SELECT TRY_CAST("2021-101-01 00:00:00" AS timestamp);