-- special date and timestamp values that are not allowed in the SQL standard
-- these tests are put in this separated file because they don't work in JDBC environment

-- date with year outside [0000-9999]
select date'999999-03-18', date'-0001-1-28', date'0015';
select make_date(999999, 3, 18), make_date(-1, 1, 28);

-- timestamp with year outside [0000-9999]
select timestamp'-1969-12-31 16:00:00', timestamp'-0015-03-18 16:00:00', timestamp'-000001', timestamp'99999-03-18T12:03:17';
select make_timestamp(-1969, 12, 31, 16, 0, 0.0), make_timestamp(-15, 3, 18, 16, 0, 0.0), make_timestamp(99999, 3, 18, 12, 3, 17.0);
