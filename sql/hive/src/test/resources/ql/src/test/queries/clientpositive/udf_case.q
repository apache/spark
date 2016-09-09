set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION case;
DESCRIBE FUNCTION EXTENDED case;

EXPLAIN
SELECT CASE 1
        WHEN 1 THEN 2
        WHEN 3 THEN 4
        ELSE 5
       END,
       CASE 2
        WHEN 1 THEN 2
        ELSE 5
       END,
       CASE 14
        WHEN 12 THEN 13
        WHEN 14 THEN 15
       END,
       CASE 16
        WHEN 12 THEN 13
        WHEN 14 THEN 15
       END,
       CASE 17
        WHEN 18 THEN NULL
        WHEN 17 THEN 20
       END,
       CASE 21
        WHEN 22 THEN 23
        WHEN 21 THEN 24
       END
FROM src tablesample (1 rows);

SELECT CASE 1
        WHEN 1 THEN 2
        WHEN 3 THEN 4
        ELSE 5
       END,
       CASE 2
        WHEN 1 THEN 2
        ELSE 5
       END,
       CASE 14
        WHEN 12 THEN 13
        WHEN 14 THEN 15
       END,
       CASE 16
        WHEN 12 THEN 13
        WHEN 14 THEN 15
       END,
       CASE 17
        WHEN 18 THEN NULL
        WHEN 17 THEN 20
       END,
       CASE 21
        WHEN 22 THEN 23
        WHEN 21 THEN 24
       END
FROM src tablesample (1 rows);

-- verify that short-circuiting is working correctly for CASE
-- we should never get to the ELSE branch, which would raise an exception
SELECT CASE 1 WHEN 1 THEN 'yo'
ELSE reflect('java.lang.String', 'bogus', 1) END
FROM src tablesample (1 rows);

-- Allow compatible types in when/return type
SELECT CASE 1
        WHEN 1 THEN 123.0BD
        ELSE 0.0BD
       END,
       CASE 1
        WHEN 1.0 THEN 123
        WHEN 2 THEN 1.0
        ELSE 222.02BD
       END,
       CASE 'abc'
        WHEN cast('abc' as varchar(3)) THEN 'abcd'
        WHEN 'efg' THEN cast('efgh' as varchar(10))
        ELSE cast('ijkl' as char(4))
       END
FROM src tablesample (1 rows);
