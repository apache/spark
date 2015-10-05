SELECT CASE 1
        WHEN 1 THEN 2
        WHEN 3 THEN 4
        ELSE 5
       END,
       CASE 11
        WHEN 12 THEN 13
        WHEN 14 THEN 15
       END
FROM src LIMIT 1
