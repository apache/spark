CREATE TEMPORARY VIEW t AS select '2011-05-06 07:08:09.1234567' as c, interval 10 year 20 month 30 day 40 hour 50 minute 6.7890 second as i;

select extract(year from c), extract(year from i) from t;
select extract(y from c), extract(y from i) from t;
select extract(years from c), extract(years from i) from t;
select extract(yr from c), extract(yr from i) from t;
select extract(yrs from c), extract(yrs from i) from t;

select extract(yearofweek from c) from t;

select extract(quarter from c) from t;
select extract(qtr from c) from t;

select extract(month from c), extract(month from i) from t;
select extract(mon from c), extract(mon from i) from t;
select extract(mons from c), extract(mons from i) from t;
select extract(months from c), extract(months from i) from t;

select extract(week from c) from t;
select extract(w from c) from t;
select extract(weeks from c) from t;

select extract(day from c), extract(day from i) from t;
select extract(d from c), extract(d from i) from t;
select extract(days from c), extract(days from i) from t;

select extract(dayofweek from c) from t;
select extract(dow from c) from t;

select extract(dayofweek_iso from c) from t;
select extract(dow_iso from c) from t;

select extract(doy from c) from t;

select extract(hour from c), extract(hour from i) from t;
select extract(h from c), extract(h from i) from t;
select extract(hours from c), extract(hours from i) from t;
select extract(hr from c), extract(hr from i) from t;
select extract(hrs from c), extract(hrs from i) from t;

select extract(minute from c), extract(minute from i) from t;
select extract(m from c), extract(m from i) from t;
select extract(min from c), extract(min from i) from t;
select extract(mins from c), extract(mins from i) from t;
select extract(minutes from c), extract(minutes from i) from t;

select extract(second from c), extract(second from i) from t;
select extract(s from c), extract(s from i) from t;
select extract(sec from c), extract(sec from i) from t;
select extract(seconds from c), extract(seconds from i) from t;
select extract(secs from c), extract(secs from i) from t;

select extract(not_supported from c) from t;
select extract(not_supported from i) from t;

select date_part('year', c), date_part('year', i) from t;
select date_part('y', c), date_part('y', i) from t;
select date_part('years', c), date_part('years', i) from t;
select date_part('yr', c), date_part('yr', i) from t;
select date_part('yrs', c), date_part('yrs', i) from t;

select date_part('yearofweek', c) from t;

select date_part('quarter', c) from t;
select date_part('qtr', c) from t;

select date_part('month', c), date_part('month', i) from t;
select date_part('mon', c), date_part('mon', i) from t;
select date_part('mons', c), date_part('mons', i) from t;
select date_part('months', c), date_part('months', i) from t;

select date_part('week', c) from t;
select date_part('w', c) from t;
select date_part('weeks', c) from t;

select date_part('day', c), date_part('day', i) from t;
select date_part('d', c), date_part('d', i) from t;
select date_part('days', c), date_part('days', i) from t;

select date_part('dayofweek', c) from t;
select date_part('dow', c) from t;

select date_part('dayofweek_iso', c) from t;
select date_part('dow_iso', c) from t;

select date_part('doy', c) from t;

select date_part('hour', c), date_part('hour', i) from t;
select date_part('h', c), date_part('h', i) from t;
select date_part('hours', c), date_part('hours', i) from t;
select date_part('hr', c), date_part('hr', i) from t;
select date_part('hrs', c), date_part('hrs', i) from t;

select date_part('minute', c), date_part('minute', i) from t;
select date_part('m', c), date_part('m', i) from t;
select date_part('min', c), date_part('min', i) from t;
select date_part('mins', c), date_part('mins', i) from t;
select date_part('minutes', c), date_part('minutes', i) from t;

select date_part('second', c), date_part('second', i) from t;
select date_part('s', c), date_part('s', i) from t;
select date_part('sec', c), date_part('sec', i) from t;
select date_part('seconds', c), date_part('seconds', i) from t;
select date_part('secs', c), date_part('secs', i) from t;

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

select c - i from t;
select year(c - i) from t;
select extract(year from c - i) from t;
select extract(month from to_timestamp(c) - i) from t;
