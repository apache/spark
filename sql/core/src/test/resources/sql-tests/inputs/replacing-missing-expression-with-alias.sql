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

-- Fixed point doesn't know to correctly replace `col1` with `a` because HAVING adds an artificial Project node
SELECT col1 AS a FROM VALUES(1,2) GROUP BY col1, col2 HAVING col2 > 1 ORDER BY col1;
SELECT col1 AS a, a AS b FROM VALUES(1,2) GROUP BY col1, col2 HAVING col2 > 1 ORDER BY col1;
