-- special date and timestamp values that are not allowed in the SQL standard
-- these tests are put in this separated file because they don't work in JDBC environment

-- date with year outside [0000-9999]
select date'999999-03-18', date'-0001-1-28', date'0015';
select make_date(999999, 3, 18), make_date(-1, 1, 28);

-- timestamp with year outside [0000-9999]
select timestamp'-1969-12-31 16:00:00', timestamp'-0015-03-18 16:00:00', timestamp'-000001', timestamp'99999-03-18T12:03:17';
select make_timestamp(-1969, 12, 31, 16, 0, 0.0), make_timestamp(-15, 3, 18, 16, 0, 0.0), make_timestamp(99999, 3, 18, 12, 3, 17.0);

-- next_day and last_day at the bounds of the DATE range: -5877641-06-23 is the minimum DATE and
-- +5881580-07-11 (a Friday) the maximum. A result past the maximum fails instead of wrapping around.
select next_day(date'-5877641-06-23', 'MO'), next_day(date'-5877641-06-23', 'FR');
select next_day(date'+5881580-07-04', 'FR');
select next_day(date'+5881580-07-11', 'MO');
select last_day(date'-5877641-06-23'), last_day(date'+5881580-06-30');
select last_day(date'+5881580-07-11');
-- trunc fails instead of wrapping around when the result is earlier than the minimum DATE.
select trunc(date'-5877641-06-29', 'WEEK'), trunc(date'-5877641-07-01', 'WEEK'), trunc(date'-5877641-07-01', 'MM'), trunc(date'-5877641-07-01', 'QUARTER'), trunc(date'-5877640-06-23', 'YEAR');
select trunc(date'-5877641-06-28', 'WEEK');
select trunc(date'-5877641-06-30', 'MONTH');
select trunc(date'-5877641-06-23', 'QUARTER');
select trunc(date'-5877641-12-31', 'YEAR');
