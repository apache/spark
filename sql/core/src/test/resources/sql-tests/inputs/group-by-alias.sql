-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);

-- GROUP BY alias should work with case insensitive names
SELECT a from testData GROUP BY A;

-- Aliases in SELECT could be used in GROUP BY
SELECT a AS k, COUNT(b) FROM testData GROUP BY k;
SELECT a AS k, COUNT(b) FROM testData GROUP BY k HAVING k > 1;
SELECT col1 AS k, SUM(col2) FROM testData AS t(col1, col2) GROUP BY k;
SELECT a as alias FROM testData GROUP BY ALIAS;

-- GROUP BY literal
SELECT a AS k FROM testData GROUP BY 'k';
SELECT 1 AS k FROM testData GROUP BY 'k';

-- GROUP BY alias with the function name
SELECT concat_ws(' ', a, b) FROM testData GROUP BY `concat_ws( , a, b)`;

-- GROUP BY column with name same as an alias used in the project list
SELECT 1 AS a FROM testData GROUP BY a;
SELECT 1 AS a FROM testData GROUP BY `a`;

-- GROUP BY implicit alias
SELECT 1 GROUP BY `1`;

-- GROUP BY alias with the subquery name
SELECT (SELECT a FROM testData) + (SELECT b FROM testData) group by `(scalarsubquery() + scalarsubquery())`;

-- GROUP BY with expression subqueries
SELECT a, count(*) FROM testData GROUP BY (SELECT b FROM testData);
SELECT a, count(*) FROM testData GROUP BY a, (SELECT b FROM testData);
SELECT a, count(*) FROM testData GROUP BY a, (SELECT b FROM testData LIMIT 1);
SELECT a, count(*) FROM testData GROUP BY a, b IN (SELECT a FROM testData);
SELECT a, count(*) FROM testData GROUP BY a, a IN (SELECT b FROM testData);
SELECT a, count(*) FROM testData GROUP BY a, EXISTS(SELECT b FROM testData);

-- GROUP BY alias with invalid col in SELECT list
SELECT a AS k, COUNT(non_existing) FROM testData GROUP BY k;

-- Aggregate functions cannot be used in GROUP BY
SELECT COUNT(b) AS k FROM testData GROUP BY k;

-- Ordinal is replaced correctly when grouping by alias of a literal
SELECT MAX(col1), 3 as abc FROM VALUES(1),(2),(3),(4) GROUP BY col1 % abc;

-- GROUP BY attribute takes precedence over alias
SELECT 1 AS a FROM testData GROUP BY `a`;

-- Group alias on subquery with CTE inside
SELECT (
  WITH cte AS (SELECT 1)
  SELECT * FROM cte
) AS subq1
FROM
  VALUES (1)
GROUP BY
  subq1
;

-- Group by alias on subquery with relation
SELECT (
  SELECT a FROM testData LIMIT 1
) AS subq1
FROM
  VALUES (1)
GROUP BY
  subq1
;

-- Group by alias which has IntegerLiteral as its child
SELECT 111 AS abc GROUP BY 'a', abc;
SELECT *, 111 AS abc FROM VALUES ('a', 'b', 'c') GROUP BY col1, col2, col3, abc;

-- Group by alias should respect order of aliases in aggregate expressions list
SELECT col1 as a, col2 AS a FROM values('a','b') GROUP BY col2, a;
SELECT 1 AS a, 2 AS a FROM VALUES (1) t (a) GROUP BY a HAVING a > 1 ORDER BY a;
SELECT 1 AS a, 2 AS a FROM VALUES (1) t (col1) GROUP BY a HAVING a > 1 ORDER BY a;

-- turn off group by aliases
set spark.sql.groupByAliases=false;

-- Check analysis exceptions
SELECT a AS k, COUNT(b) FROM testData GROUP BY k;
SELECT 1 GROUP BY `1`;
SELECT 1 AS col FROM testData GROUP BY `col`;

-- GROUP BY attribute takes precedence over alias
SELECT 1 AS a FROM testData GROUP BY `a`;

-- Replacing SortOrder expression with alias from below
CREATE TEMPORARY VIEW t_alias_test AS SELECT * FROM VALUES(1,2,3);
SELECT col1, col1 AS a FROM t_alias_test GROUP BY col1 ORDER BY col1 ASC;
SELECT col1 AS a, col1 FROM t_alias_test GROUP BY col1 ORDER BY col1 ASC;
SELECT make_date(col1, col2, col3) AS a, a AS b FROM t_alias_test GROUP BY make_date(col1, col2, col3) ORDER BY make_date(col1, col2, col3);
SELECT 1 AS a, 1 / a AS b, ZEROIFNULL(SUM(col1)) FROM t_alias_test GROUP BY 1 ORDER BY ZEROIFNULL(SUM(col1));
SELECT col1 AS a, SUM(col2) AS b, CASE WHEN col1 = 1 THEN 1 END AS c FROM t_alias_test GROUP BY col1 ORDER BY CASE WHEN col1 = 1 THEN 1 END ASC;

-- Replacing Having condition with alias from below
SELECT col1, col1 AS a FROM t_alias_test GROUP BY col1 HAVING col1 > 0;
SELECT col1 AS a, col1 FROM t_alias_test GROUP BY col1 HAVING col1 > 0;
SELECT make_date(col1, col2, col3) AS a, a AS b FROM t_alias_test GROUP BY make_date(col1, col2, col3) HAVING make_date(col1, col2, col3) > '2025-01-01';
SELECT 1 AS a, 1 / a AS b, ZEROIFNULL(SUM(col1)) FROM t_alias_test GROUP BY 1 HAVING ZEROIFNULL(SUM(col1)) > 0;
SELECT col1 AS a, SUM(col2) AS b, CASE WHEN col1 = 1 THEN 1 END AS c FROM t_alias_test GROUP BY col1 HAVING CASE WHEN col1 = 1 THEN 1 END = 1;

-- Priority of aliases when replacing them in ORDER BY/ HAVING should be determined by name
SELECT col1+1 AS ltrl2, col1+1 AS ltrl1 FROM t_alias_test GROUP BY col1+1 ORDER BY col1+1;

-- GROUP BY alias cannot reference nested fields
SELECT col1 AS a FROM VALUES (NAMED_STRUCT('f1', 1)) GROUP BY a.f1;

DROP VIEW t_alias_test;
