DESCRIBE FUNCTION weekofyear;
DESCRIBE FUNCTION EXTENDED weekofyear;

SELECT weekofyear('1980-01-01'), weekofyear('1980-01-06'), weekofyear('1980-01-07'), weekofyear('1980-12-31'),
weekofyear('1984-1-1'), weekofyear('2008-02-20 00:00:00'), weekofyear('1980-12-28 23:59:59'), weekofyear('1980-12-29 23:59:59')
FROM src LIMIT 1;
