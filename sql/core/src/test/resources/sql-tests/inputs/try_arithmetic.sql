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

-- Absolute value and negation of numerics
SELECT try_abs(-1), try_negative(1);
SELECT try_abs(-128Y), try_negative(-128Y);
SELECT try_abs(-32768S), try_negative(-32768S);
SELECT try_abs(-2147483648), try_negative(-2147483648);
SELECT try_abs(-9223372036854775808L), try_negative(-9223372036854775808L);
SELECT try_abs(-2147483647), try_negative(2147483647);
SELECT try_abs(-1.5D), try_negative(1.5D);
SELECT try_abs(-12.34BD), try_negative(-12.34BD);
SELECT try_abs(-99999999999999999999999999999999999999BD), try_negative(99999999999999999999999999999999999999BD);
SELECT try_abs(NULL), try_negative(NULL);
SELECT try_abs("-1"), try_negative("1");
SELECT try_abs(c), try_negative(c) FROM VALUES ('-1.5'), ('abc'), (NULL) AS t(c);
SELECT try_abs(c), try_negative(c) FROM VALUES (-2147483648), (-1), (2147483647), (NULL) AS t(c);
SELECT try_abs(c), try_negative(c) FROM VALUES (-9223372036854775808L), (1L) AS t(c);
SELECT try_abs(2147483647 + 1);
SELECT try_negative(2147483647 + 1);

-- Absolute value and negation of intervals
SELECT try_abs(interval -2 year), try_negative(interval 2 second);
SELECT try_abs(INTERVAL '-178956970-8' YEAR TO MONTH);
SELECT try_negative(INTERVAL '-178956970-8' YEAR TO MONTH);
SELECT try_abs(INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND);
SELECT try_negative(INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND);
SELECT try_abs(c), try_negative(c)
FROM VALUES (INTERVAL '-178956970-8' YEAR TO MONTH), (INTERVAL '-1-1' YEAR TO MONTH) AS t(c);
SELECT try_negative(make_interval(0, 1, 0, 2)), try_negative(make_interval(-178956970, -8));

-- Invalid input types
SELECT try_abs(true);
SELECT try_negative(true);
SELECT try_abs(make_interval(0, 1));
