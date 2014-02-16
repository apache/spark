DESCRIBE FUNCTION conv;
DESCRIBE FUNCTION EXTENDED conv;

-- conv must work on both strings and integers up to 64-bit precision

-- Some simple conversions to test different bases
SELECT
  conv('4521', 10, 36),
  conv('22', 10, 10),
  conv('110011', 2, 16),
  conv('facebook', 36, 16)
FROM src LIMIT 1;

-- Test negative numbers. If to_base is positive, the number should be handled
-- as a two's complement (64-bit)
SELECT
  conv('-641', 10, -10),
  conv('1011', 2, -16),
  conv('-1', 10, 16),
  conv('-15', 10, 16)
FROM src LIMIT 1;

-- Test overflow. If a number is two large, the result should be -1 (if signed)
-- or MAX_LONG (if unsigned)
SELECT
  conv('9223372036854775807', 36, 16),
  conv('9223372036854775807', 36, -16),
  conv('-9223372036854775807', 36, 16),
  conv('-9223372036854775807', 36, -16)
FROM src LIMIT 1;

-- Test with invalid input. If one of the bases is invalid, the result should
-- be NULL. If there is an invalid digit in the number, the longest valid
-- prefix should be converted.
SELECT
  conv('123455', 3, 10),
  conv('131', 1, 5),
  conv('515', 5, 100),
  conv('10', -2, 2)
FROM src LIMIT 1;

-- Perform the same tests with number arguments.

SELECT
  conv(4521, 10, 36),
  conv(22, 10, 10),
  conv(110011, 2, 16)
FROM src LIMIT 1;

SELECT
  conv(-641, 10, -10),
  conv(1011, 2, -16),
  conv(-1, 10, 16),
  conv(-15, 10, 16)
FROM src LIMIT 1;

SELECT
  conv(9223372036854775807, 36, 16),
  conv(9223372036854775807, 36, -16),
  conv(-9223372036854775807, 36, 16),
  conv(-9223372036854775807, 36, -16)
FROM src LIMIT 1;

SELECT
  conv(123455, 3, 10),
  conv(131, 1, 5),
  conv(515, 5, 100),
  conv('10', -2, 2)
FROM src LIMIT 1;

-- Make sure that state is properly reset.

SELECT conv(key, 10, 16),
       conv(key, 16, 10)
FROM src LIMIT 3;
