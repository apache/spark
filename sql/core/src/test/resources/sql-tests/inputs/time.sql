-- time literals, functions and operations

create temporary view timediff_view as select time'01:02:03' time_start, time'04:05:06' time_end, 'SECOND' unit;

create temporary view time_view as select '11:53:26.038344' time_str, 'HH:mm:ss.SSSSSS' fmt_str;

create temporary view trunc_time_view as select time'11:53:26.038344' time_val, 'MINUTE' unit;

select time '16:39:45\t';

select to_time(null), to_time('01:02:03'), to_time('23-59-59.999999', 'HH-mm-ss.SSSSSS');
select to_time(time_str, fmt_str) from time_view;

-- missing fields in `to_time`
select to_time("11", "HH");
-- invalid: there is no 13 hours
select to_time("13-60", "HH-mm");

select try_to_time(null), try_to_time('00:12:00'), try_to_time('01:02:03', 'HH:mm:ss');
select try_to_time(1);
select try_to_time('12:48:31 abc');
select try_to_time('10:11:12.', 'HH:mm:ss.SSSSSS');
select try_to_time("02-69", "HH-mm");
select try_to_time('11:12:13', 'HH:mm:ss', 'SSSSSS');

select make_time(1, 18, 19.87);
-- invalid cases
select make_time(null, 18, 19.87);
select make_time(1, null, 19.87);
select make_time(1, 18, null);
select make_time(-1, 18, 19.87);
select make_time(1, 60, 19.87);
select make_time(1, 18, 60.0);
select make_time(1, 18, 9999999999.999999);
select make_time(1, 18, -999999999.999999);
-- Full seconds overflows to a valid seconds integer when converted from long to int
select make_time(1, 18, 4294967297.999999);

select second(to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select minute(to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));
select hour(to_time('23-59-58.987654', 'HH-mm-ss.SSSSSS'));

select extract(HOUR from time'00:59:00.987654');
select extract(H from time'23:59:00.987654');
select extract(HOURS from time'00:00:00');
select extract(HR from time'23:59:59.999999');
select extract(HRS from time'23:59:58.987654');

select extract(MINUTE from time'00:59:00.987654');
select extract(M from time'23:59:00.987654');
select extract(M from time'23:00:59.987654');
select extract(MIN from time'00:00:00');
select extract(MINS from time'23:59:59.999999');
select extract(MINUTES from time'23:59:58.987654');

select extract(SECOND from time'00:59:00.987654');
select extract(SECOND from time'00:0:00.000001');
select extract(SECOND from time'00:0:00.100000');
select extract(SECOND from time'00:0:00.100001');
select extract(S from time'23:59:00.987654');
select extract(SEC from time'00:00:00');
select extract(SEC from time'00:00:00.000000');
select extract(SECONDS from time'23:59:59.999999');
select extract(SECS from time'23:59:58.987654');

-- test with precisions
select extract(SECOND FROM cast('09:08:01.987654' as time(0)));
select extract(SECOND FROM cast('09:08:01.987654' as time(1)));
select extract(SECOND FROM cast('09:08:01.987654' as time(2)));
select extract(SECOND FROM cast('09:08:01.987654' as time(3)));
select extract(SECOND FROM cast('09:08:01.987654' as time(4)));
select extract(SECOND FROM cast('09:08:01.987654' as time(5)));
select extract(SECOND FROM cast('09:08:01.987654' as time(6)));

-- valid time literals
SELECT TIME'0:0:0';
SELECT TIME'01:02:03';
SELECT TIME'12:34:56';
SELECT TIME'23:59:59';
SELECT TIME'23:59:59.9';
SELECT TIME'23:59:59.99';
SELECT TIME'23:59:59.999';
SELECT TIME'23:59:59.9999';
SELECT TIME'23:59:59.99999';
SELECT TIME'23:59:59.999999';
SELECT TIME'01:02:03 AM';
SELECT TIME'01:02:03 am';
SELECT TIME'01:02:03 Am';
SELECT TIME'01:02:03 PM';
SELECT TIME'01:02:03 pm';
SELECT TIME'01:02:03 pM';

-- invalid time literals
SELECT TIME'00:00:60';
SELECT TIME'00:60:00';
SELECT TIME'24:00:00';
SELECT TIME'00:00:00 AM';
SELECT TIME'00:00:00 PM';
SELECT TIME'12:00:60 AM';
SELECT TIME'12:00:60 PM';
SELECT TIME'12:60:00 AM';
SELECT TIME'12:60:00 PM';
SELECT TIME'13:00:00 AM';
SELECT TIME'13:00:00 PM';
SELECT TIME'24:00:00 AM';
SELECT TIME'24:00:00 PM';

-- cast string to time
SELECT cast("12:34:56" as time);
SELECT cast("12:34:56.789" as time(3));
SELECT cast("12:34:56.789" as time(6));
SELECT cast("12:34:56.789012" as time without time zone);

-- cast time to time
SELECT cast(cast('12:00' as time(0)) as time(2));
SELECT cast(('23:59:59.001001' :: time(6)) as time(4));
SELECT cast(time'11:59:59.999999' as time without time zone);

-- SPARK-51554: time truncation.
SELECT time_trunc('HOUR', time'12:34:56');
SELECT time_trunc('MINUTE', time'12:34:56');
SELECT time_trunc('SECOND', time'12:34:56');
SELECT time_trunc('MILLISECOND', time'12:34:56');
SELECT time_trunc('MICROSECOND', time'12:34:56');

-- SPARK-51554: time truncation with various time precisions.
SELECT time_trunc('HOUR', time'12:34:56.1');
SELECT time_trunc('MINUTE', time'12:34:56.1');
SELECT time_trunc('SECOND', time'12:34:56.1');
SELECT time_trunc('MILLISECOND', time'12:34:56.1');
SELECT time_trunc('MICROSECOND', time'12:34:56.1');
SELECT time_trunc('HOUR', time'12:34:56.123456');
SELECT time_trunc('MINUTE', time'12:34:56.123456');
SELECT time_trunc('SECOND', time'12:34:56.123456');
SELECT time_trunc('MILLISECOND', time'12:34:56.123456');
SELECT time_trunc('MICROSECOND', time'12:34:56.123456');
SELECT time_trunc('HOUR', time'12:34:56.123456789');
SELECT time_trunc('MINUTE', time'12:34:56.123456789');
SELECT time_trunc('SECOND', time'12:34:56.123456789');
SELECT time_trunc('MILLISECOND', time'12:34:56.123456789');
SELECT time_trunc('MICROSECOND', time'12:34:56.123456789');

-- SPARK-51554: time truncation with various unit cases.
SELECT time_trunc('hour', time'12:34:56');
SELECT time_trunc('MiNuTe', time'12:34:56');
SELECT time_trunc('sEcOnD', time'12:34:56');
SELECT time_trunc('Millisecond', time'12:34:56');
SELECT time_trunc('microseconD', time'12:34:56');

-- SPARK-51554: time truncation with zero time.
SELECT time_trunc('HOUR', time'00:00:00');
SELECT time_trunc('MINUTE', time'00:00:00');
SELECT time_trunc('SECOND', time'00:00:00');
SELECT time_trunc('MILLISECOND', time'00:00:00');
SELECT time_trunc('MICROSECOND', time'00:00:00');
-- SPARK-51554: time truncation with small time.
SELECT time_trunc('HOUR', time'00:00:00.000000001');
SELECT time_trunc('MINUTE', time'00:00:00.000000001');
SELECT time_trunc('SECOND', time'00:00:00.000000001');
SELECT time_trunc('MILLISECOND', time'00:00:00.000000001');
SELECT time_trunc('MICROSECOND', time'00:00:00.000000001');
-- SPARK-51554: time truncation with max time.
SELECT time_trunc('HOUR', time'23:59:59.999999999');
SELECT time_trunc('MINUTE', time'23:59:59.999999999');
SELECT time_trunc('SECOND', time'23:59:59.999999999');
SELECT time_trunc('MILLISECOND', time'23:59:59.999999999');
SELECT time_trunc('MICROSECOND', time'23:59:59.999999999');

-- SPARK-51554: time truncation with invalid unit.
SELECT time_trunc('', time'12:34:56');
SELECT time_trunc(' ', time'12:34:56');
SELECT time_trunc('MS', time'12:34:56');
SELECT time_trunc('DAY', time'12:34:56');
SELECT time_trunc('WEEK', time'12:34:56');
SELECT time_trunc('ABCD', time'12:34:56');
SELECT time_trunc('QUARTER', time'12:34:56');
SELECT time_trunc('INVALID', time'12:34:56');
SELECT time_trunc('INVALID_UNIT', time'12:34:56');

-- SPARK-51554: time truncation with null inputs.
SELECT time_trunc('HOUR', NULL);
SELECT time_trunc(NULL, time'12:34:56');
SELECT time_trunc(NULL, NULL);

-- SPARK-51554: time truncation with table columns.
SELECT time_trunc('HOUR', time_val) FROM trunc_time_view;
SELECT time_trunc(unit, time'12:34:56') FROM trunc_time_view;
SELECT time_trunc(unit, time_val) FROM trunc_time_view;

-- SPARK-51562: test time function (i.e. alias for casting to time type).
SELECT time("12:34:56");
SELECT time("12:34:56.789");
SELECT time("12:34:56.789012");
SELECT time(cast('12:00' as time(0)));
SELECT time(('23:59:59.001001' :: time(6)));
SELECT time(time'11:59:59.999999');

-- +/- ANSI day-time intervals
SELECT '12:43:33.1234' :: TIME(4) + INTERVAL '01:04:05.56' HOUR TO SECOND;
SELECT TIME'08:30' + NULL;
SELECT NULL + TIME'08:30';
SELECT TIME'00:00:00.0101' + 1;
SELECT TIME'12:30' - INTERVAL '12:29:59.000001' HOUR TO SECOND;
SELECT '23:59:59.999999' :: TIME - INTERVAL '23:59:59.999999' HOUR TO SECOND;
SELECT '00:00:00.0001' :: TIME(4) - INTERVAL '0 00:00:00.0001' DAY TO SECOND;
SELECT '08:30' :: TIME(0) - INTERVAL '6' HOUR;
SELECT '10:00:01' :: TIME(1) - INTERVAL '1' MONTH;

-- SPARK-51555: time difference.
SELECT time_diff('HOUR', time'00:00:00', time'12:34:56');
SELECT time_diff('MINUTE', time'00:00:00', time'12:34:56');
SELECT time_diff('SECOND', time'00:00:00', time'12:34:56');
SELECT time_diff('MILLISECOND', time'00:00:00', time'12:34:56');
SELECT time_diff('MICROSECOND', time'00:00:00', time'12:34:56');

-- SPARK-51555: positive and negative time difference.
SELECT time_diff('HOUR', time'01:02:03', time'12:34:56');
SELECT time_diff('MINUTE', time'01:02:03', time'12:34:56');
SELECT time_diff('SECOND', time'01:02:03', time'12:34:56');
SELECT time_diff('HOUR', time'12:34:56', time'01:02:03');
SELECT time_diff('MINUTE', time'12:34:56', time'01:02:03');
SELECT time_diff('SECOND', time'12:34:56', time'01:02:03');

-- SPARK-51555: time difference with various time precisions.
SELECT time_diff('HOUR', time'00:00:00', time'12:34:56.1');
SELECT time_diff('MINUTE', time'00:00:00', time'12:34:56.1');
SELECT time_diff('SECOND', time'00:00:00', time'12:34:56.1');
SELECT time_diff('MILLISECOND', time'00:00:00', time'12:34:56.1');
SELECT time_diff('MICROSECOND', time'00:00:00', time'12:34:56.1');
SELECT time_diff('HOUR', time'00:00:00', time'12:34:56.123456');
SELECT time_diff('MINUTE', time'00:00:00', time'12:34:56.123456');
SELECT time_diff('SECOND', time'00:00:00', time'12:34:56.123456');
SELECT time_diff('MILLISECOND', time'00:00:00', time'12:34:56.123456');
SELECT time_diff('MICROSECOND', time'00:00:00', time'12:34:56.123456');
SELECT time_diff('HOUR', time'00:00:00', time'12:34:56.123456789');
SELECT time_diff('MINUTE', time'00:00:00', time'12:34:56.123456789');
SELECT time_diff('SECOND', time'00:00:00', time'12:34:56.123456789');
SELECT time_diff('MILLISECOND', time'00:00:00', time'12:34:56.123456789');
SELECT time_diff('MICROSECOND', time'00:00:00', time'12:34:56.123456789');

-- SPARK-51555: time difference with various unit cases.
SELECT time_diff('hour', time'00:00:00', time'12:34:56');
SELECT time_diff('MiNuTe', time'00:00:00', time'12:34:56');
SELECT time_diff('sEcOnD', time'00:00:00', time'12:34:56');
SELECT time_diff('Millisecond', time'00:00:00', time'12:34:56');
SELECT time_diff('microseconD', time'00:00:00', time'12:34:56');

-- SPARK-51555: time difference with zero time.
SELECT time_diff('HOUR', time'00:00:00', time'00:00:00');
SELECT time_diff('MINUTE', time'00:00:00', time'00:00:00');
SELECT time_diff('SECOND', time'00:00:00', time'00:00:00');
SELECT time_diff('MILLISECOND', time'00:00:00', time'00:00:00');
SELECT time_diff('MICROSECOND', time'00:00:00', time'00:00:00');
-- SPARK-51555: time difference with small time.
SELECT time_diff('HOUR', time'00:00:00', time'00:00:00.000000001');
SELECT time_diff('MINUTE', time'00:00:00', time'00:00:00.000000001');
SELECT time_diff('SECOND', time'00:00:00', time'00:00:00.000000001');
SELECT time_diff('MILLISECOND', time'00:00:00', time'00:00:00.000000001');
SELECT time_diff('MICROSECOND', time'00:00:00', time'00:00:00.000000001');
-- SPARK-51555: time difference with max time.
SELECT time_diff('HOUR', time'00:00:00', time'23:59:59.999999999');
SELECT time_diff('MINUTE', time'00:00:00', time'23:59:59.999999999');
SELECT time_diff('SECOND', time'00:00:00', time'23:59:59.999999999');
SELECT time_diff('MILLISECOND', time'00:00:00', time'23:59:59.999999999');
SELECT time_diff('MICROSECOND', time'00:00:00', time'23:59:59.999999999');

-- SPARK-51555: time difference with invalid unit.
SELECT time_diff('', time'00:00:00', time'12:34:56');
SELECT time_diff(' ', time'00:00:00', time'12:34:56');
SELECT time_diff('MS', time'00:00:00', time'12:34:56');
SELECT time_diff('DAY', time'00:00:00', time'12:34:56');
SELECT time_diff('WEEK', time'00:00:00', time'12:34:56');
SELECT time_diff('ABCD', time'00:00:00', time'12:34:56');
SELECT time_diff('QUARTER', time'00:00:00', time'12:34:56');
SELECT time_diff('INVALID', time'00:00:00', time'12:34:56');
SELECT time_diff('INVALID_UNIT', time'00:00:00', time'12:34:56');

-- SPARK-51555: time difference with null inputs.
SELECT time_diff(NULL, time'00:00:00', time'12:34:56');
SELECT time_diff('MICROSECOND', NULL, time'12:34:56');
SELECT time_diff('MICROSECOND', time'00:00:00', NULL);
SELECT time_diff(NULL, NULL, time'12:34:56');
SELECT time_diff(NULL, time'00:00:00', NULL);
SELECT time_diff('MICROSECOND', NULL, NULL);
SELECT time_diff(NULL, NULL, NULL);

-- SPARK-51555: time difference with table columns.
SELECT time_diff('SECOND', time_start, time_end) FROM timediff_view;
SELECT time_diff(unit, time'01:02:03', time_end) FROM timediff_view;
SELECT time_diff(unit, time_start, time'04:05:06') FROM timediff_view;
SELECT time_diff('SECOND', time'01:02:03', time_end) FROM timediff_view;
SELECT time_diff('SECOND', time_start, time'04:05:06') FROM timediff_view;
SELECT time_diff(unit, time'01:02:03', time'04:05:06') FROM timediff_view;
SELECT time_diff(unit, time_start, time_end) FROM timediff_view;

-- Subtract times
SELECT TIME'12:30:41' - TIME'10:00';
SELECT TIME'08:30' - NULL;
SELECT NULL - TIME'10:32';
SELECT TIME'12:30:41.123' - TIMESTAMP'2025-07-11 10:00:01';
SELECT '12:30:41.123' - TIME'10:00:01';
SELECT '23:59:59.999999' :: TIME(6) - '00:00' :: TIME(0);
SELECT '00:00:00.1234' :: TIME(4) - TIME'23:59:59';

-- Numeric constructor and extractor functions for TIME type


-- time_from_seconds (valid: 0 to 86399.999999)
SELECT time_from_seconds(0);
SELECT time_from_seconds(43200);
SELECT time_from_seconds(52200.5);
SELECT time_from_seconds(86399.999999);
SELECT time_from_seconds(-1);           -- invalid: negative -> exception
SELECT time_from_seconds(86400);        -- invalid: >= 86400 -> exception
SELECT time_from_seconds(90000);        -- invalid: >= 86400 -> exception
SELECT time_from_seconds(NULL);

-- time_from_millis (valid: 0 to 86399999)
SELECT time_from_millis(0);
SELECT time_from_millis(43200);
SELECT time_from_millis(52200000);
SELECT time_from_millis(52200500);
SELECT time_from_millis(86399999);
SELECT time_from_millis(-1);            -- invalid: negative -> exception
SELECT time_from_millis(86400000);      -- invalid: >= 86400000 -> exception
SELECT time_from_millis(NULL);

-- time_from_micros (valid: 0 to 86399999999)
SELECT time_from_micros(0);
SELECT time_from_micros(43200);
SELECT time_from_micros(52200000000);
SELECT time_from_micros(52200500000);
SELECT time_from_micros(86399999999);
SELECT time_from_micros(-1);            -- invalid: negative -> exception
SELECT time_from_micros(86400000000);   -- invalid: >= 86400000000 -> exception
SELECT time_from_micros(NULL);

-- time_to_seconds
SELECT time_to_seconds(TIME'00:00:00');
SELECT time_to_seconds(TIME'12:00:00');
SELECT time_to_seconds(TIME'14:30:00.5');
SELECT time_to_seconds(TIME'23:59:59.999');
SELECT time_to_seconds(TIME'23:59:59.999999');
SELECT time_to_seconds(NULL);

-- time_to_millis
SELECT time_to_millis(TIME'00:00:00');
SELECT time_to_millis(TIME'14:30:00');
SELECT time_to_millis(TIME'14:30:00.5');
SELECT time_to_millis(TIME'23:59:59.999');
SELECT time_to_millis(TIME'23:59:59.999999');
SELECT time_to_millis(NULL);

-- time_to_micros
SELECT time_to_micros(TIME'00:00:00');
SELECT time_to_micros(TIME'14:30:00');
SELECT time_to_micros(TIME'14:30:00.5');
SELECT time_to_micros(TIME'23:59:59.999');
SELECT time_to_micros(TIME'23:59:59.999999');
SELECT time_to_micros(NULL);

-- Round trip tests
SELECT time_to_seconds(time_from_seconds(52200.5));
SELECT time_from_seconds(time_to_seconds(TIME'14:30:00.5'));
SELECT time_to_millis(time_from_millis(52200500));
SELECT time_from_millis(time_to_millis(TIME'14:30:00.5'));
SELECT time_to_micros(time_from_micros(52200500000));
SELECT time_from_micros(time_to_micros(TIME'14:30:00.5'));
