-- Round with Byte input
SELECT round(25y, 1);
SELECT round(25y, 0);
SELECT round(25y, -1);
SELECT round(25y, -2);
SELECT round(25y, -3);
SELECT round(127y, -1);
SELECT round(-128y, -1);

-- Round with short integer input
SELECT round(525s, 1);
SELECT round(525s, 0);
SELECT round(525s, -1);
SELECT round(525s, -2);
SELECT round(525s, -3);
SELECT round(32767s, -1);
SELECT round(-32768s, -1);

-- Round with integer input
SELECT round(525, 1);
SELECT round(525, 0);
SELECT round(525, -1);
SELECT round(525, -2);
SELECT round(525, -3);
SELECT round(2147483647, -1);
SELECT round(-2147483647, -1);

-- Round with big integer input
SELECT round(525L, 1);
SELECT round(525L, 0);
SELECT round(525L, -1);
SELECT round(525L, -2);
SELECT round(525L, -3);
SELECT round(9223372036854775807L, -1);
SELECT round(-9223372036854775808L, -1);

-- Bround with byte input
SELECT bround(25y, 1);
SELECT bround(25y, 0);
SELECT bround(25y, -1);
SELECT bround(25y, -2);
SELECT bround(25y, -3);
SELECT bround(127y, -1);
SELECT bround(-128y, -1);

-- Bround with Short input
SELECT bround(525s, 1);
SELECT bround(525s, 0);
SELECT bround(525s, -1);
SELECT bround(525s, -2);
SELECT bround(525s, -3);
SELECT bround(32767s, -1);
SELECT bround(-32768s, -1);

-- Bround with integer input
SELECT bround(525, 1);
SELECT bround(525, 0);
SELECT bround(525, -1);
SELECT bround(525, -2);
SELECT bround(525, -3);
SELECT bround(2147483647, -1);
SELECT bround(-2147483647, -1);

-- Bround with big integer input
SELECT bround(525L, 1);
SELECT bround(525L, 0);
SELECT bround(525L, -1);
SELECT bround(525L, -2);
SELECT bround(525L, -3);
SELECT bround(9223372036854775807L, -1);
SELECT bround(-9223372036854775808L, -1);

-- Conv
SELECT conv('100', 2, 10);
SELECT conv(-10, 16, -10);
SELECT conv('9223372036854775808', 10, 16);
SELECT conv('92233720368547758070', 10, 16);
SELECT conv('9223372036854775807', 36, 10);
SELECT conv('-9223372036854775807', 36, 10);

SELECT BIN(0);
SELECT BIN(25);
SELECT BIN(25L);
SELECT BIN(25.5);

SELECT POSITIVE(0Y);
SELECT POSITIVE(25);
SELECT POSITIVE(-25L);
SELECT POSITIVE(25.5);
SELECT POSITIVE("25.5");
SELECT POSITIVE("invalid");
SELECT POSITIVE(null);
