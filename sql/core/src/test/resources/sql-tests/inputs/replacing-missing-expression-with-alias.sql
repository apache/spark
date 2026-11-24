SET spark.sql.analyzer.singlePassResolver.preventUsingAliasesFromNonDirectChildren = true;

-- Replace expression with alias that has semantically equal child if expression is not in output
SELECT col1 + 1 AS a FROM VALUES(1) GROUP BY a ORDER BY col1 + 1;
SELECT col1 + 1 AS a, a AS b FROM VALUES(1) GROUP BY a ORDER BY col1 + 1;
SELECT col1 + 1 AS a FROM VALUES(1) GROUP BY a HAVING col1 + 1 > 0;
SELECT col1 + 1 AS a, a AS b FROM VALUES(1) GROUP BY a HAVING col1 + 1 > 0;

SELECT col1, col2, GROUPING(col1) FROM VALUES("abc", 1) GROUP BY CUBE(col1, col2) ORDER BY GROUPING(col1);
SELECT col1, col2, GROUPING(col1) FROM VALUES("abc", 1) GROUP BY CUBE(col1, col2) HAVING GROUPING(col1) != NULL;


SELECT make_date(col1, col2, col3) AS a FROM VALUES(1,2,3) GROUP BY make_date(col1, col2, col3) ORDER BY make_date(col1, col2, col3);
SELECT make_date(col1, col2, col3) AS a, a AS b FROM VALUES(1,2,3) GROUP BY make_date(col1, col2, col3) ORDER BY make_date(col1, col2, col3);
SELECT make_date(col1, col2, col3) AS a FROM VALUES(1,2,3) GROUP BY make_date(col1, col2, col3) HAVING make_date(col1, col2, col3) > '2025-01-01';
SELECT make_date(col1, col2, col3) AS a, a AS b FROM VALUES(1,2,3) GROUP BY make_date(col1, col2, col3) HAVING make_date(col1, col2, col3) > '2025-01-01';

-- Don't replace expression with alias that has semantically equal child if expression is not grouping
SELECT make_date(col1, col2, col3) AS a FROM VALUES(1,2,3) ORDER BY make_date(col1, col2, col3);
SELECT make_date(col1, col2, col3) AS a, a AS b FROM VALUES(1,2,3) ORDER BY make_date(col1, col2, col3);

-- Don't replace expression with alias that has semantically equal child if expression is in output
SELECT col1, col1 AS a FROM VALUES(1) GROUP BY col1 ORDER BY col1 ASC;
SELECT col1 AS a, col1 FROM VALUES(1) GROUP BY col1 ORDER BY col1 ASC;
SELECT col1, col1 AS a FROM VALUES(1) GROUP BY col1 HAVING col1 > 0;
SELECT col1 AS a, col1 FROM VALUES(1) GROUP BY col1 HAVING col1 > 0;

SELECT col2 AS b, col2 FROM VALUES(1,2) GROUP BY 1,2 ORDER BY ALL;
SELECT col2 AS b, col2 FROM VALUES(1,2) GROUP BY 1,2 HAVING col2 > 0 ORDER BY ALL;
SELECT col2 AS b, col2, b as c FROM VALUES(1,2) GROUP BY 1,2 ORDER BY ALL;
SELECT col2 AS b, col2, b as c FROM VALUES(1,2) GROUP BY 1,2 HAVING col2 > 0 ORDER BY ALL;

-- We run the following tests in fixed-point and single-pass modes separately because their plans differ, and in golden
-- files we have to run them one after another. Fixed point sorts by the alias `a` of `col1` in the SELECT list. With
-- `preventUsingAliasesFromNonDirectChildren`, single pass does not use that alias below the Project that HAVING adds: it
-- sorts by `col1`, which it passes up to the Sort, adding it to the output of the Aggregate if the Aggregate does not
-- output it yet.

-- Fixed-point mode
SET spark.sql.analyzer.singlePassResolver.dualRunWithLegacy=false;
SET spark.sql.analyzer.singlePassResolver.enabledTentatively=false;
SELECT col1 AS a FROM VALUES(1,2) GROUP BY col1, col2 HAVING col2 > 1 ORDER BY col1;
SELECT col1 AS a, a AS b FROM VALUES(1,2) GROUP BY col1, col2 HAVING col2 > 1 ORDER BY col1;

-- Single-pass mode
SET spark.sql.analyzer.singlePassResolver.enabledTentatively=true;
SELECT col1 AS a FROM VALUES(1,2) GROUP BY col1, col2 HAVING col2 > 1 ORDER BY col1;
SELECT col1 AS a, a AS b FROM VALUES(1,2) GROUP BY col1, col2 HAVING col2 > 1 ORDER BY col1;

SET spark.sql.analyzer.singlePassResolver.dualRunWithLegacy=true;
SET spark.sql.analyzer.singlePassResolver.enabledTentatively=false;
