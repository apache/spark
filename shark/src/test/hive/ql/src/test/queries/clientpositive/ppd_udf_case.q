set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

EXPLAIN
SELECT *
FROM srcpart a JOIN srcpart b
ON a.key = b.key
WHERE a.ds = '2008-04-08' AND
      b.ds = '2008-04-08' AND
      CASE a.key
        WHEN '27' THEN TRUE
        WHEN '38' THEN FALSE
        ELSE NULL
       END
ORDER BY a.key, a.value, a.ds, a.hr, b.key, b.value, b.ds, b.hr;

SELECT *
FROM srcpart a JOIN srcpart b
ON a.key = b.key
WHERE a.ds = '2008-04-08' AND
      b.ds = '2008-04-08' AND
      CASE a.key
        WHEN '27' THEN TRUE
        WHEN '38' THEN FALSE
        ELSE NULL
       END
ORDER BY a.key, a.value, a.ds, a.hr, b.key, b.value, b.ds, b.hr;

set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
SELECT *
FROM srcpart a JOIN srcpart b
ON a.key = b.key
WHERE a.ds = '2008-04-08' AND
      b.ds = '2008-04-08' AND
      CASE a.key
        WHEN '27' THEN TRUE
        WHEN '38' THEN FALSE
        ELSE NULL
       END
ORDER BY a.key, a.value, a.ds, a.hr, b.key, b.value, b.ds, b.hr;

SELECT *
FROM srcpart a JOIN srcpart b
ON a.key = b.key
WHERE a.ds = '2008-04-08' AND
      b.ds = '2008-04-08' AND
      CASE a.key
        WHEN '27' THEN TRUE
        WHEN '38' THEN FALSE
        ELSE NULL
       END
ORDER BY a.key, a.value, a.ds, a.hr, b.key, b.value, b.ds, b.hr;

