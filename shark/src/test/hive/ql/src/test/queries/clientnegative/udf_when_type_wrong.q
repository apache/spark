SELECT CASE
        WHEN TRUE THEN 2
        WHEN '1' THEN 4
        ELSE 5
       END
FROM src LIMIT 1;
