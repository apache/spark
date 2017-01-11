-- cast string representing a valid fractional number to integral should truncate the number
SELECT CAST('1.23' AS int);
SELECT CAST('1.23' AS long);

-- cast string which are not numbers to integral should return null
SELECT CAST('abc' AS int);
SELECT CAST('abc' AS long);

-- cast string representing a very large number to integral should return null
SELECT CAST('1234567890123' AS int);
SELECT CAST('12345678901234567890123' AS long);

-- TODO: migrate all cast tests here.
