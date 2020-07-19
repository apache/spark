-- Test data.
CREATE OR REPLACE TEMPORARY VIEW t1 AS SELECT * FROM VALUES
('a'), ('b'), ('v')
as t1(a);

CREATE OR REPLACE TEMPORARY VIEW t2 AS SELECT * FROM VALUES
('1', true, unhex('537061726B2053514C'), tinyint(1), array_position(array(3, 2, 1), 1), float(1.0), 1.0, Decimal(1.0), timestamp(1), current_date),
('2', false, unhex('537061726B2053514C'), tinyint(2),  array_position(array(3, 2, 1), 2), float(2.0), 2.0, Decimal(2.0), timestamp(2), current_date),
('3', true, unhex('537061726B2053514C'), tinyint(3),  array_position(array(3, 2, 1), 1), float(3.0), 3.0, Decimal(3.0), timestamp(3), current_date)
as t2(a,b,c,d,e,f,g,h,i,j);

SELECT TRANSFORM(a)
USING 'cat' AS (a)
FROM t1;


-- with non-exist command
SELECT TRANSFORM(a)
USING 'some_non_existent_command' AS (a)
FROM t1;

-- with non-exist file
SELECT TRANSFORM(a)
USING 'python some_non_existent_file' AS (a)
FROM t1;


-- support different data type
SELECT a, b, decode(c, 'UTF-8'), d, e, f, g, h, i, j FROM (
    SELECT TRANSFORM(a, b, c, d, e, f, g, h, i, j)
    USING 'cat' AS (a string, b boolean, c binary, d tinyint, e long, f float, g double, h decimal(38, 18), i timestamp, j date)
    FROM t2
) tmp;


-- handle schema less
SELECT TRANSFORM(a, b)
USING 'cat'
FROM t2;

-- return null when return string incompatible(no serde)
SELECT TRANSFORM(a, b, c)
USING 'cat' as (a int, b int , c int)
FROM (
    SELECT
    1 AS a,
    "a" AS b,
    CAST(2000 AS timestamp) AS c
) tmp;