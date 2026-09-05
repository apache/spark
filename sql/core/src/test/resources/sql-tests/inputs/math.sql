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

-- Truncate with Byte input
SELECT truncate(25y, 1);
SELECT truncate(25y, 0);
SELECT truncate(25y, -1);
SELECT truncate(25y, -2);
SELECT truncate(25y, -3);
-- Truncate with negative Byte input: truncation is toward zero, unlike floor.
SELECT truncate(-25y, 1);
SELECT truncate(-25y, 0);
SELECT truncate(-25y, -1);
SELECT truncate(-25y, -2);
SELECT truncate(-25y, -3);
-- Truncate never overflows, unlike round: truncate(-128y, -1) is -120, not an overflow.
SELECT truncate(127y, -1);
SELECT truncate(-128y, -1);

-- Truncate with short integer input
SELECT truncate(525s, 1);
SELECT truncate(525s, 0);
SELECT truncate(525s, -1);
SELECT truncate(525s, -2);
SELECT truncate(525s, -3);
-- Truncate with negative short integer input: truncation is toward zero, unlike floor.
SELECT truncate(-525s, 1);
SELECT truncate(-525s, 0);
SELECT truncate(-525s, -1);
SELECT truncate(-525s, -2);
SELECT truncate(-525s, -3);

-- Truncate with integer input
SELECT truncate(525, 1);
SELECT truncate(525, 0);
SELECT truncate(525, -1);
SELECT truncate(525, -2);
SELECT truncate(525, -3);
-- Truncate with negative integer input: truncation is toward zero, unlike floor.
SELECT truncate(-525, 1);
SELECT truncate(-525, 0);
SELECT truncate(-525, -1);
SELECT truncate(-525, -2);
SELECT truncate(-525, -3);

-- Truncate with big integer input
SELECT truncate(525L, 1);
SELECT truncate(525L, 0);
SELECT truncate(525L, -1);
SELECT truncate(525L, -2);
SELECT truncate(525L, -3);
-- Truncate with negative big integer input: truncation is toward zero, unlike floor.
SELECT truncate(-525L, 1);
SELECT truncate(-525L, 0);
SELECT truncate(-525L, -1);
SELECT truncate(-525L, -2);
SELECT truncate(-525L, -3);

-- Truncate with the scale argument omitted; it defaults to 0.
SELECT truncate(1234.5678);

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

-- Gcd
SELECT gcd(24, 36);
SELECT gcd(17, 5);
-- The result is never negative, whichever inputs are.
SELECT gcd(-24, 36);
SELECT gcd(24, -36);
SELECT gcd(-24, -36);
-- Zero is divisible by everything, so it acts as the identity.
SELECT gcd(0, 0);
SELECT gcd(0, -7);
SELECT gcd(7, 0);
SELECT gcd(null, 5);
SELECT gcd(5, null);

-- Lcm
SELECT lcm(4, 6);
SELECT lcm(17, 5);
-- The result is never negative, whichever inputs are.
SELECT lcm(-4, 6);
SELECT lcm(4, -6);
SELECT lcm(-4, -6);
-- Zero has no non-zero multiple in common with anything.
SELECT lcm(0, 0);
SELECT lcm(0, 5);
SELECT lcm(5, 0);
SELECT lcm(null, 5);
SELECT lcm(5, null);
-- Dividing by the gcd before multiplying keeps a representable result in range: the naive product
-- 4611686018427387904 * 2 would overflow, while the least common multiple does not.
SELECT lcm(4611686018427387904L, 2L);
-- Long.MaxValue and Long.MaxValue - 2 are both odd and differ by 2, so they are coprime and their
-- least common multiple is not representable.
SELECT lcm(9223372036854775807L, 9223372036854775805L);
