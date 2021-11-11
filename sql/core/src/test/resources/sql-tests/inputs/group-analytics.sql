CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)
AS testData(a, b);

-- CUBE on overlapping columns
SELECT a + b, b, SUM(a - b) FROM testData GROUP BY a + b, b WITH CUBE;

SELECT a, b, SUM(b) FROM testData GROUP BY a, b WITH CUBE;

-- ROLLUP on overlapping columns
SELECT a + b, b, SUM(a - b) FROM testData GROUP BY a + b, b WITH ROLLUP;

SELECT a, b, SUM(b) FROM testData GROUP BY a, b WITH ROLLUP;

CREATE OR REPLACE TEMPORARY VIEW courseSales AS SELECT * FROM VALUES
("dotNET", 2012, 10000), ("Java", 2012, 20000), ("dotNET", 2012, 5000), ("dotNET", 2013, 48000), ("Java", 2013, 30000)
AS courseSales(course, year, earnings);

-- ROLLUP
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY ROLLUP(course, year) ORDER BY course, year;
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY ROLLUP(course, year, (course, year)) ORDER BY course, year;
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY ROLLUP(course, year, (course, year), ()) ORDER BY course, year;

-- CUBE
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY CUBE(course, year) ORDER BY course, year;
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY CUBE(course, year, (course, year)) ORDER BY course, year;
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY CUBE(course, year, (course, year), ()) ORDER BY course, year;

-- GROUPING SETS
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY course, year GROUPING SETS(course, year);
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY course, year GROUPING SETS(course, year, ());
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY course, year GROUPING SETS(course);
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY course, year GROUPING SETS(year);

-- Partial ROLLUP/CUBE/GROUPING SETS
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY course, CUBE(course, year) ORDER BY course, year;
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY CUBE(course, year), ROLLUP(course, year) ORDER BY course, year;
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY CUBE(course, year), ROLLUP(course, year), GROUPING SETS(course, year) ORDER BY course, year;

-- GROUPING SETS with aggregate functions containing groupBy columns
SELECT course, SUM(earnings) AS sum FROM courseSales
GROUP BY course, earnings GROUPING SETS((), (course), (course, earnings)) ORDER BY course, sum;
SELECT course, SUM(earnings) AS sum, GROUPING_ID(course, earnings) FROM courseSales
GROUP BY course, earnings GROUPING SETS((), (course), (course, earnings)) ORDER BY course, sum;

-- GROUPING/GROUPING_ID
SELECT course, year, GROUPING(course), GROUPING(year), GROUPING_ID(course, year) FROM courseSales
GROUP BY CUBE(course, year);
SELECT course, year, GROUPING(course) FROM courseSales GROUP BY course, year;
SELECT course, year, GROUPING_ID(course, year) FROM courseSales GROUP BY course, year;
SELECT course, year, grouping__id FROM courseSales GROUP BY CUBE(course, year) ORDER BY grouping__id, course, year;

-- GROUPING/GROUPING_ID in having clause
SELECT course, year FROM courseSales GROUP BY CUBE(course, year)
HAVING GROUPING(year) = 1 AND GROUPING_ID(course, year) > 0 ORDER BY course, year;
SELECT course, year FROM courseSales GROUP BY course, year HAVING GROUPING(course) > 0;
SELECT course, year FROM courseSales GROUP BY course, year HAVING GROUPING_ID(course) > 0;
SELECT course, year FROM courseSales GROUP BY CUBE(course, year) HAVING grouping__id > 0;

-- GROUPING/GROUPING_ID in orderBy clause
SELECT course, year, GROUPING(course), GROUPING(year) FROM courseSales GROUP BY CUBE(course, year)
ORDER BY GROUPING(course), GROUPING(year), course, year;
SELECT course, year, GROUPING_ID(course, year) FROM courseSales GROUP BY CUBE(course, year)
ORDER BY GROUPING(course), GROUPING(year), course, year;
SELECT course, year FROM courseSales GROUP BY course, year ORDER BY GROUPING(course);
SELECT course, year FROM courseSales GROUP BY course, year ORDER BY GROUPING_ID(course);
SELECT course, year FROM courseSales GROUP BY CUBE(course, year) ORDER BY grouping__id, course, year;

-- Aliases in SELECT could be used in ROLLUP/CUBE/GROUPING SETS
SELECT a + b AS k1, b AS k2, SUM(a - b) FROM testData GROUP BY CUBE(k1, k2);
SELECT a + b AS k, b, SUM(a - b) FROM testData GROUP BY ROLLUP(k, b);
SELECT a + b, b AS k, SUM(a - b) FROM testData GROUP BY a + b, k GROUPING SETS(k);

-- GROUP BY use mixed Separate columns and CUBE/ROLLUP/Gr
SELECT a, b, count(1) FROM testData GROUP BY a, b, CUBE(a, b);
SELECT a, b, count(1) FROM testData GROUP BY a, b, ROLLUP(a, b);
SELECT a, b, count(1) FROM testData GROUP BY CUBE(a, b), ROLLUP(a, b);
SELECT a, b, count(1) FROM testData GROUP BY a, CUBE(a, b), ROLLUP(b);
SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS((a, b), (a), ());
SELECT a, b, count(1) FROM testData GROUP BY a, CUBE(a, b), GROUPING SETS((a, b), (a), ());
SELECT a, b, count(1) FROM testData GROUP BY a, CUBE(a, b), ROLLUP(a, b), GROUPING SETS((a, b), (a), ());

-- Support nested CUBE/ROLLUP/GROUPING SETS in GROUPING SETS
SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS(ROLLUP(a, b));
SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS(GROUPING SETS((a, b), (a), ()));

SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS((a, b), GROUPING SETS(ROLLUP(a, b)));
SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS((a, b, a, b), (a, b, a), (a, b));
SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS(GROUPING SETS((a, b, a, b), (a, b, a), (a, b)));

SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS(ROLLUP(a, b), CUBE(a, b));
SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS(GROUPING SETS((a, b), (a), ()), GROUPING SETS((a, b), (a), (b), ()));
SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS((a, b), (a), (), (a, b), (a), (b), ());
