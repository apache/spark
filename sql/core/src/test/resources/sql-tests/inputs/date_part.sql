CREATE TEMPORARY VIEW t AS select '2011-05-06 07:08:09.1234567' as c;

select date_part('millennium', c) from t;
select date_part('millennia', c) from t;
select date_part('mil', c) from t;
select date_part('mils', c) from t;

select date_part('century', c) from t;
select date_part('centuries', c) from t;
select date_part('c', c) from t;
select date_part('cent', c) from t;

select date_part('decade', c) from t;
select date_part('decades', c) from t;
select date_part('dec', c) from t;
select date_part('decs', c) from t;

select date_part('year', c) from t;
select date_part('y', c) from t;
select date_part('years', c) from t;
select date_part('yr', c) from t;
select date_part('yrs', c) from t;

select date_part('quarter', c) from t;
select date_part('qtr', c) from t;

select date_part('month', c) from t;
select date_part('mon', c) from t;
select date_part('mons', c) from t;
select date_part('months', c) from t;

select date_part('week', c) from t;
select date_part('w', c) from t;
select date_part('weeks', c) from t;

select date_part('day', c) from t;
select date_part('d', c) from t;
select date_part('days', c) from t;

select date_part('dayofweek', c) from t;

select date_part('dow', c) from t;

select date_part('isodow', c) from t;

select date_part('doy', c) from t;

select date_part('hour', c) from t;
select date_part('h', c) from t;
select date_part('hours', c) from t;
select date_part('hr', c) from t;
select date_part('hrs', c) from t;

select date_part('minute', c) from t;
select date_part('m', c) from t;
select date_part('min', c) from t;
select date_part('mins', c) from t;
select date_part('minutes', c) from t;

select date_part('second', c) from t;
select date_part('s', c) from t;
select date_part('sec', c) from t;
select date_part('seconds', c) from t;
select date_part('secs', c) from t;

select date_part('not_supported', c) from t;

select date_part(c, c) from t;

select date_part(null, c) from t;

CREATE TEMPORARY VIEW t2 AS select interval 1010 year 9 month 8 day 7 hour 6 minute 5 second 4 millisecond 3 microsecond as c;

select date_part('millennium', c) from t2;
select date_part('millennia', c) from t2;
select date_part('mil', c) from t2;
select date_part('mils', c) from t2;

select date_part('century', c) from t2;
select date_part('centuries', c) from t2;
select date_part('c', c) from t2;
select date_part('cent', c) from t2;

select date_part('decade', c) from t2;
select date_part('decades', c) from t2;
select date_part('dec', c) from t2;
select date_part('decs', c) from t2;

select date_part('year', c) from t2;
select date_part('y', c) from t2;
select date_part('years', c) from t2;
select date_part('yr', c) from t2;
select date_part('yrs', c) from t2;

select date_part('quarter', c) from t2;
select date_part('qtr', c) from t2;

select date_part('month', c) from t2;
select date_part('mon', c) from t2;
select date_part('mons', c) from t2;
select date_part('months', c) from t2;

select date_part('day', c) from t2;
select date_part('d', c) from t2;
select date_part('days', c) from t2;

select date_part('hour', c) from t2;
select date_part('h', c) from t2;
select date_part('hours', c) from t2;
select date_part('hr', c) from t2;
select date_part('hrs', c) from t2;

select date_part('minute', c) from t2;
select date_part('m', c) from t2;
select date_part('min', c) from t2;
select date_part('mins', c) from t2;
select date_part('minutes', c) from t2;

select date_part('second', c) from t2;
select date_part('s', c) from t2;
select date_part('sec', c) from t2;
select date_part('seconds', c) from t2;
select date_part('secs', c) from t2;

select date_part('milliseconds', c) from t2;
select date_part('msec', c) from t2;
select date_part('msecs', c) from t2;
select date_part('millisecon', c) from t2;
select date_part('mseconds', c) from t2;
select date_part('ms', c) from t2;

select date_part('microseconds', c) from t2;
select date_part('usec', c) from t2;
select date_part('usecs', c) from t2;
select date_part('useconds', c) from t2;
select date_part('microsecon', c) from t2;
select date_part('us', c) from t2;

select date_part('epoch', c) from t2;

select date_part('not_supported', c) from t2;

select date_part(c, c) from t2;

select date_part(null, c) from t2;
