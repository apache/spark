create temporary view courseEarnings as select * from values
  ("dotNET", 15000, 48000, 22500),
  ("Java", 20000, 30000, NULL)
  as courseEarnings(course, `2012`, `2013`, `2014`);

SELECT * FROM courseEarnings
UNPIVOT (
  earningsYear FOR year IN (`2012`, `2013`, `2014`)
);

SELECT * FROM courseEarnings
UNPIVOT INCLUDE NULLS (
  earningsYear FOR year IN (`2012`, `2013`, `2014`)
);

SELECT * FROM courseEarnings
UNPIVOT EXCLUDE NULLS (
  earningsYear FOR year IN (`2012`, `2013`, `2014`)
);

-- work with dots in column names and struct fields accessed in FROM clause
SELECT * FROM (
  SELECT course, `the.earnings`.* FROM (
    SELECT course, struct(`2012`, `2013`, `2014`) AS `the.earnings`
    FROM courseEarnings
  )
)
UNPIVOT (
  earningsYear FOR year IN (`2012`, `2013`, `2014`)
);

-- struct fields not accessible in UNPIVOT
SELECT * FROM (
  SELECT course, struct(`2012`, `2013`, `2014`) AS `the.earnings`
  FROM courseEarnings
)
UNPIVOT (
  earningsYear FOR year IN (`the.earnings`.`2012`, `the.earnings`.`2013`, `the.earnings`.`2014`)
);

-- columns not in the unpivot columns are part of the output
SELECT * FROM courseEarnings
UNPIVOT (
  earningsYear FOR year IN (`2013`, `2014`)
);

-- empty unpivot columns list not allowed
SELECT * FROM courseEarningsAndSales
UNPIVOT (
  values FOR year IN ()
);

-- alias for column names not allowed, use alias in FROM clause
SELECT * FROM courseEarnings
UNPIVOT (
  earningsYear FOR year IN (`2012` as `twenty-twelve`, `2013` as `twenty-thirteen`, `2014` as `twenty-fourteen`)
);

SELECT up.* FROM courseEarnings
UNPIVOT (
  earningsYear FOR year IN (`2012`, `2013`, `2014`)
) up;

SELECT up.* FROM courseEarnings
UNPIVOT (
  earningsYear FOR year IN (`2012`, `2013`, `2014`)
) AS up;

SELECT up.* FROM courseEarnings
UNPIVOT (
  earningsYear FOR year IN (`2012`, `2013`, `2014`)
);


create temporary view courseEarningsAndSales as select * from values
  ("dotNET", 15000, 2, 48000, 1, 22500, 1),
  ("Java", 20000, 1, 30000, 2, NULL, NULL)
  as courseEarningsAndSales(course, earnings2012, sales2012, earnings2013, sales2013, earnings2014, sales2014);

SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales) FOR year IN ((earnings2012, sales2012) `2012`, (earnings2013, sales2013) `2013`, (earnings2014, sales2014) `2014`)
);

SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales) FOR year IN ((earnings2012, sales2012) as `2012`, (earnings2013, sales2013) as `2013`, (earnings2014, sales2014) as `2014`)
);

-- individual alias not allowed for multiple unpivot columns
SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales) FOR year IN ((earnings2012 as earnings, sales2012 as sales) as `2012`, (earnings2013 as earnings, sales2013 as sales) as `2013`, (earnings2014 as earnings, sales2014 as sales) as `2014`)
);

SELECT * FROM courseEarningsAndSales
UNPIVOT EXCLUDE NULLS (
  (earnings, sales) FOR year IN ((earnings2012, sales2012) as `2012`, (earnings2013, sales2013) as `2013`, (earnings2014, sales2014) as `2014`)
);

SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales) FOR year IN ((earnings2012, sales2012), (earnings2013, sales2013), (earnings2014, sales2014))
);

-- work with dots in column names and struct fields
SELECT * FROM (
  SELECT course,
         named_struct('2012', earnings2012, '2013', earnings2013, '2014', earnings2014) AS `the.earnings`,
         named_struct('2012', sales2012, '2013', sales2013, '2014', sales2014) AS `the.sales`
  FROM courseEarningsAndSales
)
UNPIVOT (
  (earnings, sales) FOR year IN (
    (`the.earnings`.`2012`, `the.sales`.`2012`) `2012`,
    (`the.earnings`.`2013`, `the.sales`.`2013`) `2013`,
    (`the.earnings`.`2014`, `the.sales`.`2014`) `2014`
  )
);


-- columns not in the unpivot columns are part of the output
SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales) FOR year IN ((earnings2013, sales2013), (earnings2014, sales2014))
);

-- empty unpivot value columns list not allowed
SELECT * FROM courseEarningsAndSales
UNPIVOT (
  () FOR year IN ((earnings2012, sales2012), (earnings2013, sales2013), (earnings2014, sales2014))
);

-- empty unpivot columns list not allowed
SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales) FOR year IN ()
);

-- all unpivot column lists must have size of unpivot value columns list
SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales, extra) FOR year IN ((earnings2012, sales2012), (earnings2013, sales2013), (earnings2014, sales2014))
);

-- all unpivot column lists must have size of unpivot value columns list
SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales) FOR year IN ((earnings2012), (earnings2013, sales2013), (earnings2014, sales2014, sales2014))
);
