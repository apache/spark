-- group by all
-- additional group by star test cases from Mosha
create temporary view stuff as select * from values
  (42, 9.75, 'hello world', '1970-08-07', '13.37', array(1,20,300)),
  (1337, 1.2345, 'oh no', '2000-01-01', '42.0', array(4000,50000,600000)),
  (42, 13.37, 'test', '1970-08-07', '1234567890', array(7000000,80000000,900000000))
  as stuff(i, f, s, t, d, a);

SELECT 100 * SUM(i) + SUM(f) / COUNT(s) AS f1, i AS f2 FROM stuff GROUP BY ALL ORDER BY f2;

SELECT i + 1 AS i1, COUNT(i - 2) ci, f / i AS fi, SUM(i + f) sif FROM stuff GROUP BY ALL ORDER BY 1, 3;

SELECT i AS i, COUNT(i) ci, f AS f, SUM(i + f) sif FROM stuff GROUP BY ALL ORDER BY 1, i, 2, ci, 3, f, 4, sif;

SELECT i + 1, f / i, substring(s, 2, 3), extract(year from t), octet_length(d), size(a) FROM stuff
GROUP BY ALL ORDER BY 1, 3, 4, 5, 6, 2;

-- unlike Mosha, I'm failing this case because IMO it is too implicit to automatically group by i.
SELECT i + SUM(f) FROM stuff GROUP BY ALL;

SELECT s AS s, COUNT(*) c FROM stuff GROUP BY ALL HAVING SUM(f) > 0 ORDER BY s;

SELECT SUM(i) si FROM stuff GROUP BY ALL HAVING si > 2;

SELECT SUM(i) si FROM stuff GROUP BY ALL HAVING si < 2;

-- negative test, i shouldn't propagate through the aggregate so the having should fail
SELECT SUM(i) si FROM stuff GROUP BY ALL HAVING i > 2;

-- negative test, i shouldn't propagate through the aggregate so the order by should fail
SELECT SUM(i) si FROM stuff GROUP BY ALL ORDER BY i DESC;

