-- This file tests special values such as NaN, Infinity and NULL.

SELECT udf(x) FROM (VALUES (1), (2), (NULL)) v(x);
SELECT udf(x) FROM (VALUES ('A'), ('B'), (NULL)) v(x);
SELECT udf(x) FROM (VALUES ('NaN'), ('1'), ('2')) v(x);
SELECT udf(x) FROM (VALUES ('Infinity'), ('1'), ('2')) v(x);
SELECT udf(x) FROM (VALUES ('-Infinity'), ('1'), ('2')) v(x);
SELECT udf(x) FROM (VALUES 0.00000001D, 0.00000002D, 0.00000003D) v(x);
SELECT array(1, 2, x), map('a', x), struct(x) FROM (VALUES (1), (2), (3)) v(x);
