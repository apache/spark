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

-- TODO: migrate all cast tests here.
