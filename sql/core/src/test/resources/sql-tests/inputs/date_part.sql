CREATE TEMPORARY VIEW t AS select '2011-05-06 07:08:09.1234567' as c;

select date_part('millennium', c) from t;

select date_part('century', c) from t;

select date_part('decade', c) from t;

select date_part('year', c) from t;

select date_part('quarter', c) from t;

select date_part('month', c) from t;

select date_part('week', c) from t;

select date_part('day', c) from t;

select date_part('dayofweek', c) from t;

select date_part('dow', c) from t;

select date_part('isodow', c) from t;

select date_part('doy', c) from t;

select date_part('hour', c) from t;

select date_part('minute', c) from t;

select date_part('second', c) from t;

select date_part('not_supported', c) from t;

select date_part(c, c) from t;
