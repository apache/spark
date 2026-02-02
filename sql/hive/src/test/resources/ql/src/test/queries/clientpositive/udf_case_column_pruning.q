EXPLAIN
SELECT CASE a.key
        WHEN '1' THEN 2
        WHEN '3' THEN 4
        ELSE 5
       END as key
FROM src a JOIN src b
ON a.key = b.key
ORDER BY key LIMIT 10;

SELECT CASE a.key
        WHEN '1' THEN 2
        WHEN '3' THEN 4
        ELSE 5
       END as key
FROM src a JOIN src b
ON a.key = b.key
ORDER BY key LIMIT 10;
