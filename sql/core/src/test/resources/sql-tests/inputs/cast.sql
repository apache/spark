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

DESC FUNCTION boolean;
DESC FUNCTION EXTENDED boolean;

-- cast null to calendar interval should return null
SELECT CAST(NULL as calendarinterval);
SELECT CALENDARINTERVAL(NULL);

-- cast invalid strings to calendar interval should return null
SELECT CAST('interval 10' as calendarinterval);
SELECT CAST('interval 100 nanoseconds' as calendarinterval);
SELECT CAST('interval 1 second 10 years -10 months 1 minute' as calendarinterval);
SELECT CAST('interval 60 hours + 1 minute' as calendarinterval);
SELECT CAST('interval 1 day +5 minutes' as calendarinterval);

-- cast valid strings to calendar interval should return calendar interval
SELECT CAST('interval 5 minutes' as calendarinterval);
SELECT CAST('interval 10 hours' as calendarinterval);
SELECT CAST('interval 1 second' as calendarinterval);
SELECT CAST('interval 3 years -3 month 7 week 123 microseconds' as calendarinterval);
SELECT CAST('interval 100 years 15 months -24 weeks 66 seconds' as calendarinterval);

-- casting invalid strings to calendar interval using the function should return null
SELECT CALENDARINTERVAL('interval 2 months 1 year 3 weeks 4 days 5 minutes 6 seconds');
SELECT CALENDARINTERVAL('interval 1 seconds + 10 milliseconds');
SELECT CALENDARINTERVAL('interval 1 day -1 day');
SELECT CALENDARINTERVAL('interval 10 microseconds _ 1 microsecond');
SELECT CALENDARINTERVAL('5 weeks 5 days 23 hours');

-- casting to calendar interval using the function should return calendar interval
SELECT CALENDARINTERVAL('interval 1 year 2 months 3 weeks 4 days 5 minutes 6 seconds');
SELECT CALENDARINTERVAL('interval 10 hours -5 minutes');
SELECT CALENDARINTERVAL('interval 15 second 1000 milliseconds');
SELECT CALENDARINTERVAL('interval 10 hours      5 minutes');
SELECT CALENDARINTERVAL('interval 1 minute -60 seconds');

DESC FUNCTION calendarinterval;
-- TODO: migrate all cast tests here.
