-- Test data.
CREATE OR REPLACE TEMPORARY VIEW t1 AS SELECT * FROM VALUES
('1', true, unhex('537061726B2053514C'), tinyint(1), smallint(100), array_position(array(3, 2, 1), 1),
 float(1.0), 1.0, Decimal(1.0), timestamp('1997-01-02'), date('2000-04-01'), array(1, 2, 3), map(1, '1'), struct(1, '1')),
('2', false, unhex('537061726B2053514C'), tinyint(2), smallint(200), array_position(array(3, 2, 1), 2),
 float(2.0), 2.0, Decimal(2.0), timestamp('1997-01-02 03:04:05'), date('2000-04-02'), array(2, 3, 4), map(1, '1'), struct(1, '1')),
('3', true, unhex('537061726B2053514C'), tinyint(3), smallint(300), array_position(array(3, 2, 1), 1),
 float(3.0), 3.0, Decimal(3.0), timestamp('1997-02-10 17:32:01-08'), date('2000-04-03'), array(3, 4, 5), map(1, '1'), struct(1, '1'))
as t1(a, b, c, d, e, f, g, h, i, j, k, l, m, n);

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
SELECT a, b, decode(c, 'UTF-8'), d, e, f, g, h, i, j, k, l, m, n FROM (
    SELECT TRANSFORM(a, b, c, d, e, f, g, h, i, j, k, l, m, n)
    USING 'cat' AS (
        a string,
        b boolean,
        c binary,
        d tinyint,
        e smallint,
        f long,
        g float,
        h double,
        i decimal(38, 18),
        j timestamp,
        k date,
        l array<int>,
        m map<int, string>,
        n struct<col1:int, col2:string>)
    FROM t1
) tmp;


-- handle schema less
SELECT TRANSFORM(a, b)
USING 'cat'
FROM t1;

-- return null when return string incompatible (no serde)
SELECT TRANSFORM(a, b, c)
USING 'cat' as (a int, b int , c int)
FROM (
    SELECT
    1 AS a,
    "a" AS b,
    CAST(2000 AS timestamp) AS c
) tmp;


-- transform can't run with aggregation
SELECT TRANSFORM(b, max(a), sum(f))
USING 'cat' AS (a, b)
FROM t1
GROUP BY b;

-- transform use MAP
MAP a, b USING 'cat' AS (a, b) FROM t1;


-- transform use REDUCE
REDUCE a, b USING 'cat' AS (a, b) FROM t1;


