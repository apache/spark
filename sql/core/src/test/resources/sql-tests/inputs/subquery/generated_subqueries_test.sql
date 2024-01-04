CREATE TEMPORARY VIEW inner_table (a, b) AS VALUES
    (1, 10),
    (2, 20),
    (3, 30),
    (4, 40),
    (5, 50),
    (8, 80),
    (9, 90);
CREATE TEMPORARY VIEW outer_table (a, b) AS VALUES
    (1, 100),
    (2, 200),
    (3, 300),
    (4, 400),
    (6, 600),
    (7, 700),
    (10, 1000);
CREATE TEMPORARY VIEW no_match_inner_table (a, b) AS VALUES
    (6, 600),
    (7, 700),
    (10, 1000);
CREATE TEMPORARY VIEW no_match_outer_table (a, b) AS VALUES
    (5, 50),
    (8, 80),
    (9, 90);

-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT c  FROM (SELECT  SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  a  FROM inner_table  WHERE inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM outer_table  ORDER BY outer_table.b, subquery ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT c  FROM (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT c  FROM (SELECT  SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT c  FROM (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT c  FROM (SELECT  COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery  ORDER BY c ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=FROM,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a  FROM (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery  ORDER BY a ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a IN(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE outer_table.a NOT IN(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE EXISTS(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM outer_table WHERE NOT EXISTS(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM outer_table WHERE outer_table.a =(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  a  FROM inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  SUM(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  COUNT(a) AS c  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  a  FROM inner_table  WHERE inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  SUM(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  SUM(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  COUNT(a) AS c  FROM inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  COUNT(a) AS c  FROM inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  a  FROM inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  a  FROM inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT no_match_outer_table.b, (SELECT  COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=SELECT,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT no_match_outer_table.b, (SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )  AS subquery FROM no_match_outer_table  ORDER BY no_match_outer_table.b, subquery ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE EXISTS(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  SUM(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  COUNT(a) AS c  FROM no_match_inner_table )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 10 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=False
SELECT a, b  FROM no_match_outer_table WHERE NOT EXISTS(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  SUM(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  COUNT(a) AS c  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  a  FROM no_match_inner_table  WHERE no_match_inner_table.a = no_match_outer_table.a ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  SUM(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=SUM,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  SUM(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=True,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  COUNT(a) AS c  FROM no_match_inner_table GROUP BY a ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=True,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=AGGREGATE,distinct_projection=False,aggregate_function(count_bug)=COUNT,group_by=False,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  COUNT(a) AS c  FROM no_match_inner_table ORDER BY c  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=LIMIT,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=True,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,subquery_operator=ORDER BY,distinct_projection=False,aggregate_function(count_bug)=None,group_by=None,has_limit=True
SELECT a, b  FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT  a  FROM no_match_inner_table ORDER BY a  DESC  LIMIT 1 )   ORDER BY a, b ;
