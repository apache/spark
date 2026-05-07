CREATE TABLE t1(col1 INT, col2 STRING);
CREATE TABLE t2(col1 STRUCT<a: STRING>, a STRING);

-- Update references to match fixed-point plan when realiasing expressions in LCA resolution
SELECT LEN(LOWER('X')) AS a, 1 AS b, b AS c GROUP BY LOWER('X') ORDER BY LOWER('X');
SELECT LEN(LOWER('X')) AS a, 1 AS b, b AS c GROUP BY LOWER('X') HAVING LOWER('X') = 'x';

-- LCA referencing nested field
SELECT col1.field, field FROM VALUES(named_struct('field', 1));
SELECT col1.field, field FROM VALUES(map('field', 1));

-- LCA attributes shouldn't be counted in UNRESOLVED_GROUP_BY_ALL validation
SELECT COUNT(col1) as alias, SUM(col1) + alias FROM t1 GROUP BY ALL;
SELECT COUNT(col1) as alias, SUM(col1) + alias, SUM(col1) + col1 FROM t1 GROUP BY ALL;

DROP TABLE t1;
DROP TABLE t2;
