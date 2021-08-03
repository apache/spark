CREATE TEMPORARY VIEW t AS select '2011-05-06 07:08:09.1234567' as c, to_timestamp_ntz('2011-05-06 07:08:09.1234567') as ntz, interval 10 year 20 month as i, interval 30 day 40 hour 50 minute 6.7890 second as j;

select extract(year from c), extract(year from ntz), extract(year from i) from t;
select extract(y from c), extract(y from ntz), extract(y from i) from t;
select extract(years from c), extract(years from ntz), extract(years from i) from t;
select extract(yr from c), extract(yr from ntz), extract(yr from i) from t;
select extract(yrs from c), extract(yrs from ntz), extract(yrs from i) from t;

select extract(yearofweek from c), extract(yearofweek from ntz) from t;

select extract(quarter from c), extract(quarter from ntz) from t;
select extract(qtr from c), extract(qtr from ntz) from t;

select extract(month from c), extract(month from ntz), extract(month from i) from t;
select extract(mon from c), extract(mon from ntz), extract(mon from i) from t;
select extract(mons from c), extract(mons from ntz), extract(mons from i) from t;
select extract(months from c), extract(months from ntz), extract(months from i) from t;

select extract(week from c), extract(week from ntz) from t;
select extract(w from c), extract(w from ntz) from t;
select extract(weeks from c), extract(weeks from ntz) from t;

select extract(day from c), extract(day from ntz), extract(day from j) from t;
select extract(d from c), extract(d from ntz), extract(d from j) from t;
select extract(days from c), extract(days from ntz), extract(days from j) from t;

select extract(dayofweek from c), extract(dayofweek from ntz) from t;
select extract(dow from c), extract(dow from ntz) from t;

select extract(dayofweek_iso from c), extract(dayofweek_iso from ntz) from t;
select extract(dow_iso from c), extract(dow_iso from ntz) from t;

select extract(doy from c), extract(doy from ntz) from t;

select extract(hour from c), extract(hour from ntz), extract(hour from j) from t;
select extract(h from c), extract(h from ntz), extract(h from j) from t;
select extract(hours from c), extract(hours from ntz), extract(hours from j) from t;
select extract(hr from c), extract(hr from ntz), extract(hr from j) from t;
select extract(hrs from c), extract(hrs from ntz), extract(hrs from j) from t;

select extract(minute from c), extract(minute from ntz), extract(minute from j) from t;
select extract(m from c), extract(m from ntz), extract(m from j) from t;
select extract(min from c), extract(min from ntz), extract(min from j) from t;
select extract(mins from c), extract(mins from ntz), extract(mins from j) from t;
select extract(minutes from c), extract(minutes from ntz), extract(minutes from j) from t;

select extract(second from c), extract(second from ntz), extract(second from j) from t;
select extract(s from c), extract(s from ntz), extract(s from j) from t;
select extract(sec from c), extract(sec from ntz), extract(sec from j) from t;
select extract(seconds from c), extract(seconds from ntz), extract(seconds from j) from t;
select extract(secs from c), extract(secs from ntz), extract(secs from j) from t;

select extract(not_supported from c) from t;
select extract(not_supported from i) from t;
select extract(not_supported from j) from t;

select date_part('year', c), date_part('year', ntz), date_part('year', i) from t;
select date_part('y', c), date_part('y', ntz), date_part('y', i) from t;
select date_part('years', c), date_part('years', ntz), date_part('years', i) from t;
select date_part('yr', c), date_part('yr', ntz), date_part('yr', i) from t;
select date_part('yrs', c), date_part('yrs', ntz), date_part('yrs', i) from t;

select date_part('yearofweek', c), date_part('yearofweek', ntz) from t;

select date_part('quarter', c), date_part('quarter', ntz) from t;
select date_part('qtr', c), date_part('qtr', ntz) from t;

select date_part('month', c), date_part('month', ntz), date_part('month', i) from t;
select date_part('mon', c), date_part('mon', ntz), date_part('mon', i) from t;
select date_part('mons', c), date_part('mons', ntz), date_part('mons', i) from t;
select date_part('months', c), date_part('months', ntz), date_part('months', i) from t;

select date_part('week', c), date_part('week', ntz) from t;
select date_part('w', c), date_part('w', ntz) from t;
select date_part('weeks', c), date_part('weeks', ntz) from t;

select date_part('day', c), date_part('day', ntz), date_part('day', j) from t;
select date_part('d', c), date_part('d', ntz), date_part('d', j) from t;
select date_part('days', c), date_part('days', ntz), date_part('days', j) from t;

select date_part('dayofweek', c), date_part('dayofweek', ntz) from t;
select date_part('dow', c), date_part('dow', ntz) from t;

select date_part('dayofweek_iso', c), date_part('dayofweek_iso', ntz) from t;
select date_part('dow_iso', c), date_part('dow_iso', ntz) from t;

select date_part('doy', c), date_part('doy', ntz) from t;

select date_part('hour', c), date_part('hour', ntz), date_part('hour', j) from t;
select date_part('h', c), date_part('h', ntz), date_part('h', j) from t;
select date_part('hours', c), date_part('hours', ntz), date_part('hours', j) from t;
select date_part('hr', c), date_part('hr', ntz), date_part('hr', j) from t;
select date_part('hrs', c), date_part('hrs', ntz), date_part('hrs', j) from t;

select date_part('minute', c), date_part('minute', ntz), date_part('minute', j) from t;
select date_part('m', c), date_part('m', ntz), date_part('m', j) from t;
select date_part('min', c), date_part('min', ntz), date_part('min', j) from t;
select date_part('mins', c), date_part('mins', ntz), date_part('mins', j) from t;
select date_part('minutes', c), date_part('minutes', ntz), date_part('minutes', j) from t;

select date_part('second', c), date_part('second', ntz), date_part('second', j) from t;
select date_part('s', c), date_part('s', ntz), date_part('s', j) from t;
select date_part('sec', c), date_part('sec', ntz), date_part('sec', j) from t;
select date_part('seconds', c), date_part('seconds', ntz), date_part('seconds', j) from t;
select date_part('secs', c), date_part('secs', ntz), date_part('secs', j) from t;

select date_part('not_supported', c) from t;
select date_part(c, c) from t;
select date_part(null, c) from t;

select date_part(i, i) from t;
select date_part(null, i) from t;

-- In SPARK-31476, we've supported extract('field', source), too
select extract('year', c) from t;
select extract('quarter', c) from t;
select extract('month', c) from t;
select extract('week', c) from t;
select extract('day', c) from t;
select extract('days', c) from t;
select extract('dayofweek', c) from t;
select extract('dow', c) from t;
select extract('doy', c) from t;
select extract('hour', c) from t;
select extract('minute', c) from t;
select extract('second', c) from t;

select c - j from t;
select day(c - j) from t;
select extract(day from c - j) from t;
select extract(month from to_timestamp(c) - i) from t;
select extract(second from to_timestamp(c) - j) from t;

-- extract fields from year-month/day-time intervals
select extract(YEAR from interval '2-1' YEAR TO MONTH);
select date_part('YEAR', interval '2-1' YEAR TO MONTH);
select extract(YEAR from -interval '2-1' YEAR TO MONTH);
select extract(MONTH from interval '2-1' YEAR TO MONTH);
select date_part('MONTH', interval '2-1' YEAR TO MONTH);
select extract(MONTH from -interval '2-1' YEAR TO MONTH);
select date_part(NULL, interval '2-1' YEAR TO MONTH);

-- invalid
select extract(DAY from interval '2-1' YEAR TO MONTH);
select date_part('DAY', interval '2-1' YEAR TO MONTH);
select date_part('not_supported', interval '2-1' YEAR TO MONTH);

select extract(DAY from interval '123 12:34:56.789123123' DAY TO SECOND);
select date_part('DAY', interval '123 12:34:56.789123123' DAY TO SECOND);
select extract(DAY from -interval '123 12:34:56.789123123' DAY TO SECOND);
select extract(HOUR from interval '123 12:34:56.789123123' DAY TO SECOND);
select date_part('HOUR', interval '123 12:34:56.789123123' DAY TO SECOND);
select extract(HOUR from -interval '123 12:34:56.789123123' DAY TO SECOND);
select extract(MINUTE from interval '123 12:34:56.789123123' DAY TO SECOND);
select date_part('MINUTE', interval '123 12:34:56.789123123' DAY TO SECOND);
select extract(MINUTE from -interval '123 12:34:56.789123123' DAY TO SECOND);
select extract(SECOND from interval '123 12:34:56.789123123' DAY TO SECOND);
select date_part('SECOND', interval '123 12:34:56.789123123' DAY TO SECOND);
select extract(SECOND from -interval '123 12:34:56.789123123' DAY TO SECOND);
select date_part(NULL, interval '123 12:34:56.789123123' DAY TO SECOND);

select extract(MONTH from interval '123 12:34:56.789123123' DAY TO SECOND);
select date_part('not_supported', interval '123 12:34:56.789123123' DAY TO SECOND);
