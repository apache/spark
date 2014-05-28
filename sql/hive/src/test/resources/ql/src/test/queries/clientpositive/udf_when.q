DESCRIBE FUNCTION when;
DESCRIBE FUNCTION EXTENDED when;

EXPLAIN
SELECT CASE
        WHEN 1=1 THEN 2
        WHEN 1=3 THEN 4
        ELSE 5
       END,
       CASE
        WHEN 6=7 THEN 8
        ELSE 9
       END,
       CASE
        WHEN 10=11 THEN 12
        WHEN 13=13 THEN 14
       END,
       CASE
        WHEN 15=16 THEN 17
        WHEN 18=19 THEN 20
       END,
       CASE
        WHEN 21=22 THEN NULL
        WHEN 23=23 THEN 24
       END,
       CASE
        WHEN 25=26 THEN 27
        WHEN 28=28 THEN NULL
       END
FROM src LIMIT 1;

SELECT CASE
        WHEN 1=1 THEN 2
        WHEN 1=3 THEN 4
        ELSE 5
       END,
       CASE
        WHEN 6=7 THEN 8
        ELSE 9
       END,
       CASE
        WHEN 10=11 THEN 12
        WHEN 13=13 THEN 14
       END,
       CASE
        WHEN 15=16 THEN 17
        WHEN 18=19 THEN 20
       END,
       CASE
        WHEN 21=22 THEN NULL
        WHEN 23=23 THEN 24
       END,
       CASE
        WHEN 25=26 THEN 27
        WHEN 28=28 THEN NULL
       END
FROM src LIMIT 1;
