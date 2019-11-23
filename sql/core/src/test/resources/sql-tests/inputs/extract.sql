CREATE TEMPORARY VIEW t AS select '2011-05-06 07:08:09.1234567' as c;

select extract(millennium from c) from t;
select extract(millennia from c) from t;
select extract(mil from c) from t;
select extract(mils from c) from t;

select extract(century from c) from t;
select extract(centuries from c) from t;
select extract(c from c) from t;
select extract(cent from c) from t;

select extract(decade from c) from t;
select extract(decades from c) from t;
select extract(dec from c) from t;
select extract(decs from c) from t;

select extract(year from c) from t;
select extract(y from c) from t;
select extract(years from c) from t;
select extract(yr from c) from t;
select extract(yrs from c) from t;

select extract(isoyear from c) from t;

select extract(quarter from c) from t;
select extract(qtr from c) from t;

select extract(month from c) from t;
select extract(mon from c) from t;
select extract(mons from c) from t;
select extract(months from c) from t;

select extract(week from c) from t;
select extract(w from c) from t;
select extract(weeks from c) from t;

select extract(day from c) from t;
select extract(d from c) from t;
select extract(days from c) from t;

select extract(dayofweek from c) from t;

select extract(dow from c) from t;

select extract(isodow from c) from t;

select extract(doy from c) from t;

select extract(hour from c) from t;
select extract(h from c) from t;
select extract(hours from c) from t;
select extract(hr from c) from t;
select extract(hrs from c) from t;

select extract(minute from c) from t;
select extract(m from c) from t;
select extract(min from c) from t;
select extract(mins from c) from t;
select extract(minutes from c) from t;

select extract(second from c) from t;
select extract(s from c) from t;
select extract(sec from c) from t;
select extract(seconds from c) from t;
select extract(secs from c) from t;

select extract(milliseconds from c) from t;
select extract(msec from c) from t;
select extract(msecs from c) from t;
select extract(millisecon from c) from t;
select extract(mseconds from c) from t;
select extract(ms from c) from t;

select extract(microseconds from c) from t;
select extract(usec from c) from t;
select extract(usecs from c) from t;
select extract(useconds from c) from t;
select extract(microsecon from c) from t;
select extract(us from c) from t;

select extract(epoch from c) from t;

select extract(not_supported from c) from t;
