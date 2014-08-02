SELECT CASE
        WHEN 1=1 THEN 2
        WHEN 3=5 THEN 4
        ELSE 5
       END,
       CASE
        WHEN 12=11 THEN 13
        WHEN 14=10 THEN 15
       END
FROM src LIMIT 1
