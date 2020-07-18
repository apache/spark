-- Test data.
CREATE OR REPLACE TEMPORARY VIEW t1 AS SELECT * FROM VALUES
('a'), ('b'), ('v')
as t1(a);

CREATE OR REPLACE TEMPORARY VIEW t2 AS SELECT * FROM VALUES
(1, '1', 1.0, Decimal(1.0), timestamp(1)),
(2, '2', 2.0, Decimal(2.0), timestamp(2)),
(2, '2', 3.0, Decimal(3.0), timestamp(3))
as t2(a,b,c,d,e);

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
SELECT TRANSFORM(a, b, c, d, e)
USING 'CAT' AS (a, b, c, d, e)
FROM t2;


-- handle schema less
SELECT TRANSFORM(a, b)
USING 'CAT'
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