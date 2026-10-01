create temporary view courseEarnings as select * from values
  ("dotNET", 15000, 48000, 22500),
  ("Java", 20000, 30000, NULL)
  as courseEarnings(course, `2012`, `2013`, `2014`);

SELECT * FROM courseEarnings
UNPIVOT (
  earningsYear FOR year IN (`2012`, `2013`, `2014`)
);

-- NULL values excluded by default, include them explicitly
SELECT * FROM courseEarnings
UNPIVOT INCLUDE NULLS (
  earningsYear FOR year IN (`2012`, `2013`, `2014`)
);

-- alias for column names
SELECT * FROM courseEarnings
UNPIVOT (
  earningsYear FOR year IN (`2012` as `twenty-twelve`, `2013` as `twenty-thirteen`, `2014` as `twenty-fourteen`)
);


create temporary view courseEarningsAndSales as select * from values
  ("dotNET", 15000, NULL, 48000, 1, 22500, 1),
  ("Java", 20000, 1, 30000, 2, NULL, NULL)
  as courseEarningsAndSales(course, earnings2012, sales2012, earnings2013, sales2013, earnings2014, sales2014);

SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales) FOR year IN ((earnings2012, sales2012), (earnings2013, sales2013), (earnings2014, sales2014))
);

-- NULL values excluded by default, include them explicitly
SELECT * FROM courseEarningsAndSales
UNPIVOT INCLUDE NULLS (
  (earnings, sales) FOR year IN ((earnings2012, sales2012), (earnings2013, sales2013), (earnings2014, sales2014))
);

-- alias for column names
SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales) FOR year IN ((earnings2012, sales2012) as `2012`, (earnings2013, sales2013) as `2013`, (earnings2014, sales2014) as `2014`)
);


create temporary view mixedTypeValues as select
  1 as id,
  cast('2026-01-01' as date) as date1, cast(null as int) as int1,
  cast(null as date) as date2, 2 as int2,
  cast(null as date) as date3, cast(null as int) as int3;

-- EXCLUDE NULLS keeps partially-null tuples and removes all-null tuples across different types
SELECT * FROM mixedTypeValues
UNPIVOT EXCLUDE NULLS (
  (date_value, int_value) FOR kind IN (
    (date1, int1) as date_only,
    (date2, int2) as int_only,
    (date3, int3) as all_null
  )
)
ORDER BY kind;

-- INCLUDE NULLS retains the all-null tuple that EXCLUDE NULLS drops above
SELECT * FROM mixedTypeValues
UNPIVOT INCLUDE NULLS (
  (date_value, int_value) FOR kind IN (
    (date1, int1) as date_only,
    (date2, int2) as int_only,
    (date3, int3) as all_null
  )
)
ORDER BY kind;
