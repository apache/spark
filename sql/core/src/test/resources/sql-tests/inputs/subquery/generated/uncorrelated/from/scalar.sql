CREATE TEMPORARY VIEW inner_table(a, b) AS VALUES
    (1, 1),
    (2, 2),
    (3, 3),
    (4, 4),
    (5, 5),
    (8, 8),
    (9, 9);

CREATE TEMPORARY VIEW outer_table(a, b) AS VALUES
    (1, 1),
    (2, 1),
    (3, 3),
    (6, 6),
    (7, 7),
    (9, 9);

CREATE TEMPORARY VIEW no_match_table(a, b) AS VALUES
    (1000, 1000);

CREATE TEMPORARY VIEW join_table(a, b) AS VALUES
    (1, 1),
    (2, 1),
    (3, 3),
    (7, 8),
    (5, 6);

CREATE TEMPORARY VIEW null_table(a, b) AS SELECT CAST(null AS int), CAST(null as int);

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT innerSubqueryAlias.a FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT innerSubqueryAlias.a FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(null_table.a) AS aggFunctionAlias FROM null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table INNER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(null_table.a) AS aggFunctionAlias FROM null_table INNER JOIN join_table ON null_table.a = join_table.a GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(null_table.a) AS aggFunctionAlias FROM null_table) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=inner_table INNER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table INNER JOIN join_table ON inner_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table INNER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table INNER JOIN join_table ON inner_table.a = join_table.a GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(null_table.a) AS aggFunctionAlias FROM null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(null_table.a) AS aggFunctionAlias FROM null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table INNER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table INNER JOIN join_table ON inner_table.a = join_table.a GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT innerSubqueryAlias.a FROM (SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=null_table INNER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(null_table.a) AS aggFunctionAlias FROM null_table INNER JOIN join_table ON null_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT innerSubqueryAlias.a FROM (SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(null_table.a) AS aggFunctionAlias FROM null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(null_table.a) AS aggFunctionAlias FROM null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT innerSubqueryAlias.a FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=null_table INNER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(null_table.a) AS aggFunctionAlias FROM null_table INNER JOIN join_table ON null_table.a = join_table.a GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(null_table.a) AS aggFunctionAlias FROM null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(null_table.a) AS aggFunctionAlias FROM null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT inner_table.a FROM inner_table ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=no_match_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT null_table.a FROM null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(null_table.a) AS aggFunctionAlias FROM null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table INNER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(null_table.a) AS aggFunctionAlias FROM null_table INNER JOIN join_table ON null_table.a = join_table.a GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT innerSubqueryAlias.a FROM (SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table INNER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table INNER JOIN join_table ON inner_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table INNER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(null_table.a) AS aggFunctionAlias FROM null_table INNER JOIN join_table ON null_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT innerSubqueryAlias.a FROM (SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(null_table.a) AS aggFunctionAlias FROM null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT innerSubqueryAlias.a FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=null_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(null_table.a) AS aggFunctionAlias FROM null_table) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT innerSubqueryAlias.a FROM (SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT innerSubqueryAlias.a FROM (SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table INNER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(null_table.a) AS aggFunctionAlias FROM null_table INNER JOIN join_table ON null_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(null_table.a) AS aggFunctionAlias FROM null_table GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT null_table.a FROM null_table ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(null_table.a) AS aggFunctionAlias FROM null_table GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table INNER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(null_table.a) AS aggFunctionAlias FROM null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT no_match_table.a FROM no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT innerSubqueryAlias.a FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(null_table.a) AS aggFunctionAlias FROM null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table INNER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(null_table.a) AS aggFunctionAlias FROM null_table INNER JOIN join_table ON null_table.a = join_table.a GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(null_table.a) AS aggFunctionAlias FROM null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT innerSubqueryAlias.a FROM (SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT innerSubqueryAlias.a FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT null_table.a FROM null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=no_match_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT no_match_table.a FROM no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT no_match_table.a FROM no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT no_match_table.a FROM no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(null_table.a) AS aggFunctionAlias FROM null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(null_table.a) AS aggFunctionAlias FROM null_table) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT innerSubqueryAlias.a FROM (SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table INNER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT null_table.a FROM null_table INNER JOIN join_table ON null_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT no_match_table.a FROM no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(null_table.a) AS aggFunctionAlias FROM null_table GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(null_table.a) AS aggFunctionAlias FROM null_table) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table INNER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table INNER JOIN join_table ON inner_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT no_match_table.a FROM no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table INNER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT innerSubqueryAlias.a FROM (SELECT inner_table.a, inner_table.b FROM inner_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=inner_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(null_table.a) AS aggFunctionAlias FROM null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table INNER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table INNER JOIN join_table ON inner_table.a = join_table.a GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table LEFT OUTER JOIN join_table ON no_match_table.a = join_table.a GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT null_table.a FROM null_table ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[no_match_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a GROUP BY no_match_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(no_match_table.a) AS aggFunctionAlias FROM no_match_table RIGHT OUTER JOIN join_table ON no_match_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT null_table.a FROM null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table INNER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table INNER JOIN join_table ON inner_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT inner_table.a, inner_table.b FROM inner_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT innerSubqueryAlias.a FROM (SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT null_table.a FROM null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=inner_table INNER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table INNER JOIN join_table ON inner_table.a = join_table.a GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT no_match_table.a FROM no_match_table ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=null_table INNER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT null_table.a FROM null_table INNER JOIN join_table ON null_table.a = join_table.a ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=null_table INNER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(null_table.a) AS aggFunctionAlias FROM null_table INNER JOIN join_table ON null_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(inner_table.a) AS aggFunctionAlias],groupingExpr=[inner_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(inner_table.a) AS aggFunctionAlias FROM inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a GROUP BY inner_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT innerSubqueryAlias.a FROM (SELECT inner_table.a, inner_table.b FROM inner_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(null_table.a) AS aggFunctionAlias FROM null_table RIGHT OUTER JOIN join_table ON null_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(null_table.a) AS aggFunctionAlias FROM null_table LEFT OUTER JOIN join_table ON null_table.a = join_table.a GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT no_match_table.a, no_match_table.b FROM no_match_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT no_match_table.a FROM no_match_table ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=null_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[SUM(null_table.a) AS aggFunctionAlias],groupingExpr=[null_table.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT SUM(null_table.a) AS aggFunctionAlias FROM null_table GROUP BY null_table.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(inner_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(inner_table.a) AS aggFunctionAlias FROM inner_table LEFT OUTER JOIN join_table ON inner_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table UNION SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=inner_table, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT DISTINCT inner_table.a FROM inner_table ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(innerSubqueryAlias.a) AS aggFunctionAlias],groupingExpr=[innerSubqueryAlias.b])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(innerSubqueryAlias.a) AS aggFunctionAlias FROM (SELECT null_table.a, null_table.b FROM null_table EXCEPT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias GROUP BY innerSubqueryAlias.b ORDER BY aggFunctionAlias DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=true, subqueryOperator=AGGREGATE(resultExpr=[COUNT(no_match_table.a) AS aggFunctionAlias],groupingExpr=[])
SELECT subqueryAlias.aggFunctionAlias FROM (SELECT DISTINCT COUNT(no_match_table.a) AS aggFunctionAlias FROM no_match_table INNER JOIN join_table ON no_match_table.a = join_table.a) AS subqueryAlias ORDER BY aggFunctionAlias DESC NULLS FIRST;

-- innerTable=(SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias, outerTable=outer_table, subqueryLocation=FROM, subqueryType=SCALAR_PREDICATE_LESS_THAN, isCorrelated=false, subqueryDistinct=false, subqueryOperator=LIMIT 1
SELECT subqueryAlias.a FROM (SELECT innerSubqueryAlias.a FROM (SELECT null_table.a, null_table.b FROM null_table INTERSECT SELECT join_table.a, join_table.b FROM join_table) AS innerSubqueryAlias ORDER BY a DESC NULLS FIRST LIMIT 1) AS subqueryAlias ORDER BY a DESC NULLS FIRST
