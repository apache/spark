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
  values FOR year IN ((earnings2012, sales2012) as `2012`, (earnings2013, sales2013) as `2013`, (earnings2014, sales2014) as `2014`)
);

SELECT * FROM courseEarningsAndSales
UNPIVOT (
  values FOR year IN ((earnings2012 as earnings, sales2012 as sales) as `2012`, (earnings2013 as earnings, sales2013 as sales) as `2013`, (earnings2014 as earnings, sales2014 as sales) as `2014`)
);

SELECT * FROM courseEarningsAndSales
UNPIVOT (
  values FOR year IN ()
);

SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales) FOR year IN ((earnings2012, sales2012) as `2012`, (earnings2013, sales2013) as `2013`, (earnings2014, sales2014) as `2014`)
);

SELECT * FROM courseEarningsAndSales
UNPIVOT EXCLUDE NULLS (
  (earnings, sales) FOR year IN ((earnings2012, sales2012) as `2012`, (earnings2013, sales2013) as `2013`, (earnings2014, sales2014) as `2014`)
);

SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales) FOR year IN ((earnings2012, sales2012), (earnings2013, sales2013), (earnings2014, sales2014))
);

SELECT * FROM courseEarningsAndSales
UNPIVOT (
  () FOR year IN ((earnings2012, sales2012), (earnings2013, sales2013), (earnings2014, sales2014))
);

SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales) FOR year IN ()
);

SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales, extra) FOR year IN ((earnings2012, sales2012), (earnings2013, sales2013), (earnings2014, sales2014))
);

SELECT * FROM courseEarningsAndSales
UNPIVOT (
  (earnings, sales) FOR year IN ((earnings2012), (earnings2013, sales2013), (earnings2014, sales2014, sales2014))
);
