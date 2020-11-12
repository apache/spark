-- This file tests special values such as NaN, Infinity and NULL.

SELECT udf(x) FROM (VALUES (1), (2), (NULL)) v(x);
SELECT udf(x) FROM (VALUES ('A'), ('B'), (NULL)) v(x);
SELECT udf(x) FROM (VALUES ('NaN'), ('1'), ('2')) v(x);
SELECT udf(x) FROM (VALUES ('Infinity'), ('1'), ('2')) v(x);
SELECT udf(x) FROM (VALUES ('-Infinity'), ('1'), ('2')) v(x);
SELECT udf(x) FROM (VALUES 0.00000001, 0.00000002, 0.00000003) v(x);
