CREATE TEMPORARY VIEW t AS select '2011-05-06 07:08:09.1234567' as c;

select extract(year from c) from t;

select extract(quarter from c) from t;

select extract(month from c) from t;

select extract(week from c) from t;

select extract(day from c) from t;

select extract(dayofweek from c) from t;

select extract(hour from c) from t;

select extract(minute from c) from t;

select extract(second from c) from t;

select extract(not_supported from c) from t;
