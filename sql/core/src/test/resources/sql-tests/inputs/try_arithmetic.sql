-- Numeric + Numeric
SELECT try_add(1, 1);
SELECT try_add(2147483647, 1);
SELECT try_add(2147483647, decimal(1));
SELECT try_add(2147483647, "1");
SELECT try_add(-2147483648, -1);
SELECT try_add(9223372036854775807L, 1);
SELECT try_add(-9223372036854775808L, -1);
SELECT try_add(1, (2147483647 + 1));
SELECT try_add(1L, (9223372036854775807L + 1L));
SELECT try_add(1, 1.0 / 0.0);
SELECT try_add(sum(1), 1) GROUP BY ALL;

-- Date + Integer
SELECT try_add(date'2021-01-01', 1);
SELECT try_add(1, date'2021-01-01');

-- Date + Interval
SELECT try_add(date'2021-01-01', interval 2 year);
SELECT try_add(date'2021-01-01', interval 2 second);
SELECT try_add(interval 2 year, date'2021-01-01');
SELECT try_add(interval 2 second, date'2021-01-01');

-- Timestamp + Interval
SELECT try_add(timestamp_ltz'2021-01-01 00:00:00', interval 2 year);
SELECT try_add(timestamp_ntz'2021-01-01 00:00:00', interval 2 second);
SELECT try_add(interval 2 year, timestamp_ltz'2021-01-01 00:00:00');
SELECT try_add(interval 2 second, timestamp_ntz'2021-01-01 00:00:00');

-- Time + Interval
SELECT try_add(time'08:00:00', interval 1 hour);
SELECT try_add(interval 1 hour, time'08:00:00');
SELECT try_add(time'23:59:59.999999', interval 1 hour);
SELECT try_add(time'00:00:00', interval -1 second);
SELECT try_add(time'08:30:00', null);
SELECT try_add(null, interval 1 hour);

-- Interval + Interval
SELECT try_add(interval 2 year, interval 2 year);
SELECT try_add(interval 2 second, interval 2 second);
SELECT try_add(interval 2 year, interval 2 second);
SELECT try_add(interval 2147483647 month, interval 2 month);
SELECT try_add(interval 106751991 day, interval 3 day);

-- Numeric / Numeric
SELECT try_divide(1, 0.5);
SELECT try_divide(1, 0);
SELECT try_divide(0, 0);
SELECT try_divide(1, (2147483647 + 1));
SELECT try_divide(1L, (9223372036854775807L + 1L));
SELECT try_divide(1, 1.0 / 0.0);
SELECT try_divide(1, decimal(0));
SELECT try_divide(1, "0");
SELECT try_divide(sum(1), 1) GROUP BY ALL;

-- Interval / Numeric
SELECT try_divide(interval 2 year, 2);
SELECT try_divide(interval 2 second, 2);
SELECT try_divide(interval 2 year, 0);
SELECT try_divide(interval 2 second, 0);
SELECT try_divide(interval 2147483647 month, 0.5);
SELECT try_divide(interval 106751991 day, 0.5);

-- Numeric - Numeric
SELECT try_subtract(1, 1);
SELECT try_subtract(2147483647, -1);
SELECT try_subtract(2147483647, decimal(-1));
SELECT try_subtract(2147483647, "-1");
SELECT try_subtract(-2147483648, 1);
SELECT try_subtract(9223372036854775807L, -1);
SELECT try_subtract(-9223372036854775808L, 1);
SELECT try_subtract(1, (2147483647 + 1));
SELECT try_subtract(1L, (9223372036854775807L + 1L));
SELECT try_subtract(1, 1.0 / 0.0);
SELECT try_subtract(sum(1), 1) GROUP BY ALL;

-- Interval - Interval
SELECT try_subtract(interval 2 year, interval 3 year);
SELECT try_subtract(interval 3 second, interval 2 second);
SELECT try_subtract(interval 2147483647 month, interval -2 month);
SELECT try_subtract(interval 106751991 day, interval -3 day);

-- Time - Interval
SELECT try_subtract(time'10:00:00', interval 1 hour);
SELECT try_subtract(time'00:30:00', interval 1 hour);
SELECT try_subtract(time'00:00:00', interval 1 second);
SELECT try_subtract(time'10:00:00', null);

-- Time - Time
SELECT try_subtract(time'10:00:00', time'08:00:00');
SELECT try_subtract(time'08:00:00.123456', time'10:00:00');
SELECT try_subtract(null, time'08:00:00');

-- Numeric * Numeric
SELECT try_multiply(2, 3);
SELECT try_multiply(2147483647, -2);
SELECT try_multiply(2147483647, decimal(-2));
SELECT try_multiply(2147483647, "-2");
SELECT try_multiply(-2147483648, 2);
SELECT try_multiply(9223372036854775807L, 2);
SELECT try_multiply(-9223372036854775808L, -2);
SELECT try_multiply(1, (2147483647 + 1));
SELECT try_multiply(1L, (9223372036854775807L + 1L));
SELECT try_multiply(1, 1.0 / 0.0);
SELECT try_multiply(sum(1), 1) GROUP BY ALL;

-- Interval * Numeric
SELECT try_multiply(interval 2 year, 2);
SELECT try_multiply(interval 2 second, 2);
SELECT try_multiply(interval 2 year, 0);
SELECT try_multiply(interval 2 second, 0);
SELECT try_multiply(interval 2147483647 month, 2);
SELECT try_multiply(interval 106751991 day, 2);

-- try_add and try_multiply chains over non-foldable inputs keep their written grouping, both at
-- the top level and nested in regular arithmetic.
CREATE TABLE try_arithmetic_chains(a INT, b INT) USING parquet;
INSERT INTO try_arithmetic_chains VALUES (2147483647, 1), (-10, 0), (2147483643, 0), (1073741824, 2);
SELECT try_add(try_add(a, -5), b) FROM try_arithmetic_chains WHERE a = 2147483647;
SELECT try_add(try_add(a, -5), b) + 1 FROM try_arithmetic_chains WHERE a = 2147483647;
SELECT try_add(try_add(a, 2147483647), 1) FROM try_arithmetic_chains WHERE a = -10;
SELECT -5 + try_add(try_add(a, 2147483647), 1) FROM try_arithmetic_chains WHERE a = -10;
SELECT try_add(try_add(a, 5), -3) FROM try_arithmetic_chains WHERE a = 2147483643;
SELECT try_multiply(try_multiply(a, -1), b) FROM try_arithmetic_chains WHERE a = 1073741824;
SELECT try_multiply(try_multiply(a, -1), b) * 3 FROM try_arithmetic_chains WHERE a = 1073741824;
-- Literals that move away from zero fold exactly, also above a step with two column operands.
SELECT try_add(try_add(a, 1), 2) FROM try_arithmetic_chains WHERE a = 2147483643;
SELECT try_add(try_add(a, 2), 3) FROM try_arithmetic_chains WHERE a = 2147483643;
SELECT try_multiply(try_multiply(a, 2), 3) FROM try_arithmetic_chains WHERE a = -10;
SELECT try_multiply(try_multiply(a, 2), 2) FROM try_arithmetic_chains WHERE a = 1073741824;
SELECT a, b, try_add(try_add(try_add(a, b), 2), 3) FROM try_arithmetic_chains ORDER BY a;
SELECT a, b, try_multiply(try_multiply(try_multiply(a, b), 2), 3) FROM try_arithmetic_chains ORDER BY a;
-- Differently grouped TRY chains are different expressions.
SELECT try_add(try_add(a, -5), b), try_add(try_add(a, b), -5) FROM try_arithmetic_chains WHERE a = 2147483647;
SELECT try_multiply(try_multiply(a, -1), b), try_multiply(try_multiply(a, b), -1) FROM try_arithmetic_chains WHERE a = 1073741824;
DROP TABLE try_arithmetic_chains;
