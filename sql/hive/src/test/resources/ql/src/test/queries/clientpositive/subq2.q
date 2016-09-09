EXPLAIN
SELECT a.k, a.c
FROM (SELECT b.key as k, count(1) as c FROM src b GROUP BY b.key) a
WHERE a.k >= 90;

SELECT a.k, a.c
FROM (SELECT b.key as k, count(1) as c FROM src b GROUP BY b.key) a
WHERE a.k >= 90;
