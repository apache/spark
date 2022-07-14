-- Numeric + Numeric
SELECT try_add(1, 1);
SELECT try_add(2147483647, 1);
SELECT try_add(-2147483648, -1);
SELECT try_add(9223372036854775807L, 1);
SELECT try_add(-9223372036854775808L, -1);

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
SELECT try_subtract(-2147483648, 1);
SELECT try_subtract(9223372036854775807L, -1);
SELECT try_subtract(-9223372036854775808L, 1);

-- Interval - Interval
SELECT try_subtract(interval 2 year, interval 3 year);
SELECT try_subtract(interval 3 second, interval 2 second);
SELECT try_subtract(interval 2147483647 month, interval -2 month);
SELECT try_subtract(interval 106751991 day, interval -3 day);

-- Numeric * Numeric
SELECT try_multiply(2, 3);
SELECT try_multiply(2147483647, -2);
SELECT try_multiply(-2147483648, 2);
SELECT try_multiply(9223372036854775807L, 2);
SELECT try_multiply(-9223372036854775808L, -2);

-- Interval * Numeric
SELECT try_multiply(interval 2 year, 2);
SELECT try_multiply(interval 2 second, 2);
SELECT try_multiply(interval 2 year, 0);
SELECT try_multiply(interval 2 second, 0);
SELECT try_multiply(interval 2147483647 month, 2);
SELECT try_multiply(interval 106751991 day, 2);
