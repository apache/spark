-- This test file was converted from pivot.sql.

-- Note some test cases have been commented as the current integrated UDFs cannot handle complex types

create temporary view courseSales as select * from values
  ("dotNET", 2012, 10000),
  ("Java", 2012, 20000),
  ("dotNET", 2012, 5000),
  ("dotNET", 2013, 48000),
  ("Java", 2013, 30000)
  as courseSales(course, year, earnings);

create temporary view years as select * from values
  (2012, 1),
  (2013, 2)
  as years(y, s);

create temporary view yearsWithComplexTypes as select * from values
  (2012, array(1, 1), map('1', 1), struct(1, 'a')),
  (2013, array(2, 2), map('2', 2), struct(2, 'b'))
  as yearsWithComplexTypes(y, a, m, s);

-- pivot courses
SELECT * FROM (
  SELECT udf(year), course, earnings FROM courseSales
)
PIVOT (
  udf(sum(earnings))
  FOR course IN ('dotNET', 'Java')
);

-- pivot years with no subquery
SELECT * FROM courseSales
PIVOT (
  udf(sum(earnings))
  FOR year IN (2012, 2013)
);

-- pivot courses with multiple aggregations
SELECT * FROM (
  SELECT year, course, earnings FROM courseSales
)
PIVOT (
  udf(sum(earnings)), udf(avg(earnings))
  FOR course IN ('dotNET', 'Java')
);

-- pivot with no group by column
SELECT * FROM (
  SELECT udf(course) as course, earnings FROM courseSales
)
PIVOT (
  udf(sum(earnings))
  FOR course IN ('dotNET', 'Java')
);

-- pivot with no group by column and with multiple aggregations on different columns
SELECT * FROM (
  SELECT year, course, earnings FROM courseSales
)
PIVOT (
  udf(sum(udf(earnings))), udf(min(year))
  FOR course IN ('dotNET', 'Java')
);

-- pivot on join query with multiple group by columns
SELECT * FROM (
  SELECT course, year, earnings, udf(s) as s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  udf(sum(earnings))
  FOR s IN (1, 2)
);

-- pivot on join query with multiple aggregations on different columns
SELECT * FROM (
  SELECT course, year, earnings, s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  udf(sum(earnings)), udf(min(s))
  FOR course IN ('dotNET', 'Java')
);

-- pivot on join query with multiple columns in one aggregation
SELECT * FROM (
  SELECT course, year, earnings, s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  udf(sum(earnings * s))
  FOR course IN ('dotNET', 'Java')
);

-- pivot with aliases and projection
SELECT 2012_s, 2013_s, 2012_a, 2013_a, c FROM (
  SELECT year y, course c, earnings e FROM courseSales
)
PIVOT (
  udf(sum(e)) s, udf(avg(e)) a
  FOR y IN (2012, 2013)
);

-- pivot with projection and value aliases
SELECT firstYear_s, secondYear_s, firstYear_a, secondYear_a, c FROM (
  SELECT year y, course c, earnings e FROM courseSales
)
PIVOT (
  udf(sum(e)) s, udf(avg(e)) a
  FOR y IN (2012 as firstYear, 2013 secondYear)
);

-- pivot years with non-aggregate function
SELECT * FROM courseSales
PIVOT (
  udf(abs(earnings))
  FOR year IN (2012, 2013)
);

-- pivot with one of the expressions as non-aggregate function
SELECT * FROM (
  SELECT year, course, earnings FROM courseSales
)
PIVOT (
  udf(sum(earnings)), year
  FOR course IN ('dotNET', 'Java')
);

-- pivot with unresolvable columns
SELECT * FROM (
  SELECT course, earnings FROM courseSales
)
PIVOT (
  udf(sum(earnings))
  FOR year IN (2012, 2013)
);

-- pivot with complex aggregate expressions
SELECT * FROM (
  SELECT year, course, earnings FROM courseSales
)
PIVOT (
  udf(ceil(udf(sum(earnings)))), avg(earnings) + 1 as a1
  FOR course IN ('dotNET', 'Java')
);

-- pivot with invalid arguments in aggregate expressions
SELECT * FROM (
  SELECT year, course, earnings FROM courseSales
)
PIVOT (
  sum(udf(avg(earnings)))
  FOR course IN ('dotNET', 'Java')
);

-- pivot on multiple pivot columns
SELECT * FROM (
  SELECT course, year, earnings, s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  udf(sum(earnings))
  FOR (course, year) IN (('dotNET', 2012), ('Java', 2013))
);

-- pivot on multiple pivot columns with aliased values
SELECT * FROM (
  SELECT course, year, earnings, s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  udf(sum(earnings))
  FOR (course, s) IN (('dotNET', 2) as c1, ('Java', 1) as c2)
);

-- pivot on multiple pivot columns with values of wrong data types
SELECT * FROM (
  SELECT course, year, earnings, s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  udf(sum(earnings))
  FOR (course, year) IN ('dotNET', 'Java')
);

-- pivot with unresolvable values
SELECT * FROM courseSales
PIVOT (
  udf(sum(earnings))
  FOR year IN (s, 2013)
);

-- pivot with non-literal values
SELECT * FROM courseSales
PIVOT (
  udf(sum(earnings))
  FOR year IN (course, 2013)
);

-- Complex type is not supported in the current UDF. Skipped for now.
-- pivot on join query with columns of complex data types
-- SELECT * FROM (
--  SELECT course, year, a
--  FROM courseSales
--  JOIN yearsWithComplexTypes ON year = y
--)
--PIVOT (
--  udf(min(a))
--  FOR course IN ('dotNET', 'Java')
--);

-- Complex type is not supported in the current UDF. Skipped for now.
-- pivot on multiple pivot columns with agg columns of complex data types
-- SELECT * FROM (
--  SELECT course, year, y, a
--  FROM courseSales
--  JOIN yearsWithComplexTypes ON year = y
--)
--PIVOT (
--  udf(max(a))
--  FOR (y, course) IN ((2012, 'dotNET'), (2013, 'Java'))
--);

-- pivot on pivot column of array type
SELECT * FROM (
  SELECT earnings, year, a
  FROM courseSales
  JOIN yearsWithComplexTypes ON year = y
)
PIVOT (
  udf(sum(earnings))
  FOR a IN (array(1, 1), array(2, 2))
);

-- pivot on multiple pivot columns containing array type
SELECT * FROM (
  SELECT course, earnings, udf(year) as year, a
  FROM courseSales
  JOIN yearsWithComplexTypes ON year = y
)
PIVOT (
  udf(sum(earnings))
  FOR (course, a) IN (('dotNET', array(1, 1)), ('Java', array(2, 2)))
);

-- pivot on pivot column of struct type
SELECT * FROM (
  SELECT earnings, year, s
  FROM courseSales
  JOIN yearsWithComplexTypes ON year = y
)
PIVOT (
  udf(sum(earnings))
  FOR s IN ((1, 'a'), (2, 'b'))
);

-- pivot on multiple pivot columns containing struct type
SELECT * FROM (
  SELECT course, earnings, year, s
  FROM courseSales
  JOIN yearsWithComplexTypes ON year = y
)
PIVOT (
  udf(sum(earnings))
  FOR (course, s) IN (('dotNET', (1, 'a')), ('Java', (2, 'b')))
);

-- pivot on pivot column of map type
SELECT * FROM (
  SELECT earnings, year, m
  FROM courseSales
  JOIN yearsWithComplexTypes ON year = y
)
PIVOT (
  udf(sum(earnings))
  FOR m IN (map('1', 1), map('2', 2))
);

-- pivot on multiple pivot columns containing map type
SELECT * FROM (
  SELECT course, earnings, year, m
  FROM courseSales
  JOIN yearsWithComplexTypes ON year = y
)
PIVOT (
  udf(sum(earnings))
  FOR (course, m) IN (('dotNET', map('1', 1)), ('Java', map('2', 2)))
);

-- grouping columns output in the same order as input
-- correctly handle pivot columns with different cases
SELECT * FROM (
  SELECT course, earnings, udf("a") as a, udf("z") as z, udf("b") as b, udf("y") as y,
  udf("c") as c, udf("x") as x, udf("d") as d, udf("w") as w
  FROM courseSales
)
PIVOT (
  udf(sum(Earnings))
  FOR Course IN ('dotNET', 'Java')
);
