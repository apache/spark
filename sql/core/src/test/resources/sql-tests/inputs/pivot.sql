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

-- pivot courses
SELECT * FROM (
  SELECT year, course, earnings FROM courseSales
)
PIVOT (
  sum(earnings)
  FOR course IN ('dotNET', 'Java')
);

-- pivot years with no subquery
SELECT * FROM courseSales
PIVOT (
  sum(earnings)
  FOR year IN (2012, 2013)
);

-- pivot courses with multiple aggregations
SELECT * FROM (
  SELECT year, course, earnings FROM courseSales
)
PIVOT (
  sum(earnings), avg(earnings)
  FOR course IN ('dotNET', 'Java')
);

-- pivot with no group by column
SELECT * FROM (
  SELECT course, earnings FROM courseSales
)
PIVOT (
  sum(earnings)
  FOR course IN ('dotNET', 'Java')
);

-- pivot with no group by column and with multiple aggregations on different columns
SELECT * FROM (
  SELECT year, course, earnings FROM courseSales
)
PIVOT (
  sum(earnings), min(year)
  FOR course IN ('dotNET', 'Java')
);

-- pivot on join query with multiple group by columns
SELECT * FROM (
  SELECT course, year, earnings, s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  sum(earnings)
  FOR s IN (1, 2)
);

-- pivot on join query with multiple aggregations on different columns
SELECT * FROM (
  SELECT course, year, earnings, s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  sum(earnings), min(s)
  FOR course IN ('dotNET', 'Java')
);

-- pivot on join query with multiple columns in one aggregation
SELECT * FROM (
  SELECT course, year, earnings, s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  sum(earnings * s)
  FOR course IN ('dotNET', 'Java')
);

-- pivot with aliases and projection
SELECT 2012_s, 2013_s, 2012_a, 2013_a, c FROM (
  SELECT year y, course c, earnings e FROM courseSales
)
PIVOT (
  sum(e) s, avg(e) a
  FOR y IN (2012, 2013)
);

-- pivot years with non-aggregate function
SELECT * FROM courseSales
PIVOT (
  abs(earnings)
  FOR year IN (2012, 2013)
);

-- pivot with unresolvable columns
SELECT * FROM (
  SELECT course, earnings FROM courseSales
)
PIVOT (
  sum(earnings)
  FOR year IN (2012, 2013)
);

-- pivot with complex aggregate expressions
SELECT * FROM (
  SELECT year, course, earnings FROM courseSales
)
PIVOT (
  ceil(sum(earnings)), avg(earnings) + 1 as a1
  FOR course IN ('dotNET', 'Java')
);

-- pivot with invalid arguments in aggregate expressions
SELECT * FROM (
  SELECT year, course, earnings FROM courseSales
)
PIVOT (
  sum(avg(earnings))
  FOR course IN ('dotNET', 'Java')
);
