-- This test file was converted from group-analytics.sql.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)
AS testData(a, b);

-- CUBE on overlapping columns
SELECT udf(a + b), b, udf(SUM(a - b)) FROM testData GROUP BY udf(a + b), b WITH CUBE;

SELECT udf(a), udf(b), SUM(b) FROM testData GROUP BY udf(a), b WITH CUBE;

-- ROLLUP on overlapping columns
SELECT udf(a + b), b, SUM(a - b) FROM testData GROUP BY a + b, b WITH ROLLUP;

SELECT udf(a), b, udf(SUM(b)) FROM testData GROUP BY udf(a), b WITH ROLLUP;

CREATE OR REPLACE TEMPORARY VIEW courseSales AS SELECT * FROM VALUES
("dotNET", 2012, 10000), ("Java", 2012, 20000), ("dotNET", 2012, 5000), ("dotNET", 2013, 48000), ("Java", 2013, 30000)
AS courseSales(course, year, earnings);

-- ROLLUP
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY ROLLUP(course, year) ORDER BY udf(course), year;

-- CUBE
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY CUBE(course, year) ORDER BY course, udf(year);

-- GROUPING SETS
SELECT course, udf(year), SUM(earnings) FROM courseSales GROUP BY course, year GROUPING SETS(course, year);
SELECT course, year, udf(SUM(earnings)) FROM courseSales GROUP BY course, year GROUPING SETS(course);
SELECT udf(course), year, SUM(earnings) FROM courseSales GROUP BY course, year GROUPING SETS(year);

-- GROUPING SETS with aggregate functions containing groupBy columns
SELECT course, udf(SUM(earnings)) AS sum FROM courseSales
GROUP BY course, earnings GROUPING SETS((), (course), (course, earnings)) ORDER BY course, udf(sum);
SELECT course, SUM(earnings) AS sum, GROUPING_ID(course, earnings) FROM courseSales
GROUP BY course, earnings GROUPING SETS((), (course), (course, earnings)) ORDER BY udf(course), sum;

-- GROUPING/GROUPING_ID
SELECT udf(course), udf(year), GROUPING(course), GROUPING(year), GROUPING_ID(course, year) FROM courseSales
GROUP BY CUBE(course, year);
SELECT course, udf(year), GROUPING(course) FROM courseSales GROUP BY course, udf(year);
SELECT course, udf(year), GROUPING_ID(course, year) FROM courseSales GROUP BY udf(course), year;
SELECT course, year, grouping__id FROM courseSales GROUP BY CUBE(course, year) ORDER BY grouping__id, course, udf(year);

-- GROUPING/GROUPING_ID in having clause
SELECT course, year FROM courseSales GROUP BY CUBE(course, year)
HAVING GROUPING(year) = 1 AND GROUPING_ID(course, year) > 0 ORDER BY course, udf(year);
SELECT course, udf(year) FROM courseSales GROUP BY udf(course), year HAVING GROUPING(course) > 0;
SELECT course, udf(udf(year)) FROM courseSales GROUP BY course, year HAVING GROUPING_ID(course) > 0;
SELECT udf(course), year FROM courseSales GROUP BY CUBE(course, year) HAVING grouping__id > 0;

-- GROUPING/GROUPING_ID in orderBy clause
SELECT course, year, GROUPING(course), GROUPING(year) FROM courseSales GROUP BY CUBE(course, year)
ORDER BY GROUPING(course), GROUPING(year), course, udf(year);
SELECT course, year, GROUPING_ID(course, year) FROM courseSales GROUP BY CUBE(course, year)
ORDER BY GROUPING(course), GROUPING(year), course, udf(year);
SELECT course, udf(year) FROM courseSales GROUP BY course, udf(year) ORDER BY GROUPING(course);
SELECT course, udf(year) FROM courseSales GROUP BY course, udf(year) ORDER BY GROUPING_ID(course);
SELECT course, year FROM courseSales GROUP BY CUBE(course, year) ORDER BY grouping__id, udf(course), year;

-- Aliases in SELECT could be used in ROLLUP/CUBE/GROUPING SETS
SELECT udf(a + b) AS k1, udf(b) AS k2, SUM(a - b) FROM testData GROUP BY CUBE(k1, k2);
SELECT udf(udf(a + b)) AS k, b, SUM(a - b) FROM testData GROUP BY ROLLUP(k, b);
SELECT udf(a + b), udf(udf(b)) AS k, SUM(a - b) FROM testData GROUP BY a + b, k GROUPING SETS(k)
