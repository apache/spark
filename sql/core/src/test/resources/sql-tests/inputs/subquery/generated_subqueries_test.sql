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
    (5, 50),
    (8, 80),
    (9, 90);
CREATE TEMPORARY VIEW no_match_outer_table (a, b) AS VALUES
    (6, 600),
    (7, 700),
    (10, 1000);
CREATE TEMPORARY VIEW join_table (a, b) AS VALUES
    (1, 10),
    (3, 30),
    (4, 400),
    (6, 600),
    (8, 80);

-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT outer_table.b, (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT outer_table.b, (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT outer_table.b, (SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT outer_table.b, (SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT outer_table.b, (SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT outer_table.b, (SELECT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT outer_table.b, (SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT outer_table.b, (SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT outer_table.b, (SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT outer_table.b, (SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT outer_table.b, (SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT outer_table.b, (SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT outer_table.b, (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT outer_table.b, (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT outer_table.b, (SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT outer_table.b, (SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT outer_table.b, (SELECT SUM(inner_table.a) AS c FROM inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT outer_table.b, (SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT outer_table.b, (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT outer_table.b, (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT outer_table.b, (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT outer_table.b, (SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT outer_table.b, (SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT outer_table.b, (SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table    ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT SUM(inner_table.a) AS c FROM inner_table    ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table    ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT SUM(inner_table.a) AS c FROM inner_table    ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT SUM(inner_table.a) AS c FROM inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT inner_table.a FROM (SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT SUM(inner_table.a) AS c FROM inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT inner_table.a FROM (SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT inner_table.a FROM inner_table WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table   ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT SUM(inner_table.a) AS c FROM inner_table   ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT outer_table.b, (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT outer_table.b, (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT outer_table.b, (SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT outer_table.b, (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT outer_table.b, (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT outer_table.b, (SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT outer_table.b, (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT outer_table.b, (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT outer_table.b, (SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT outer_table.b, (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT outer_table.b, (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT outer_table.b, (SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM outer_table ORDER BY outer_table.b, subquery_alias;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT c FROM (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT c FROM (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT c FROM (SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias ORDER BY c;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_inner_table.a FROM (SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias ORDER BY no_match_inner_table.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=FROM,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT set_alias.a FROM (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias ORDER BY set_alias.a;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE EXISTS(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table   ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table   ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM outer_table WHERE outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT no_match_outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT no_match_outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT no_match_outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT no_match_outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT no_match_outer_table.b, (SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT no_match_outer_table.b, (SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT no_match_outer_table.b, (SELECT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT no_match_outer_table.b, (SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_outer_table.b, (SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_outer_table.b, (SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_outer_table.b, (SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT no_match_outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT no_match_outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT no_match_outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT no_match_outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT no_match_outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT no_match_outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT no_match_outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT no_match_outer_table.b, (SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT no_match_outer_table.b, (SELECT SUM(inner_table.a) AS c FROM inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT no_match_outer_table.b, (SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT no_match_outer_table.b, (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_outer_table.b, (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_outer_table.b, (SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_outer_table.b, (SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT no_match_outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT no_match_outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT no_match_outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT SUM(inner_table.a) AS c FROM inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT SUM(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT COUNT(inner_table.a) AS c FROM inner_table WHERE inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT inner_table.a FROM inner_table WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b) WHERE inner_table.a = no_match_outer_table.a  ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(inner_table.a) AS c FROM inner_table   ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT SUM(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT SUM(inner_table.a) AS c FROM inner_table   ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT COUNT(inner_table.a) AS c FROM inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT inner_table.a FROM inner_table   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT inner_table.a FROM inner_table INNER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT inner_table.a FROM inner_table LEFT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT inner_table.a FROM inner_table RIGHT OUTER JOIN join_table ON inner_table.b = join_table.b)   ORDER BY inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT no_match_outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT no_match_outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT no_match_outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT no_match_outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT no_match_outer_table.b, (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT no_match_outer_table.b, (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT no_match_outer_table.b, (SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT no_match_outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT no_match_outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT no_match_outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT no_match_outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT no_match_outer_table.b, (SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT no_match_outer_table.b, (SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT no_match_outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT no_match_outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT no_match_outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT no_match_outer_table.b, (SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT no_match_outer_table.b, (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT no_match_outer_table.b, (SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table   ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT no_match_outer_table.b, (SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT no_match_outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT no_match_outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT no_match_outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT no_match_outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT no_match_outer_table.b, (SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT no_match_outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT no_match_outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=SELECT,subquery_type=NA,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT no_match_outer_table.b, (SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) AS subquery_alias FROM no_match_outer_table ORDER BY no_match_outer_table.b, subquery_alias;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT IN,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a NOT IN(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a   ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a  ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 10) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type=NOT EXISTS,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE NOT EXISTS(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias    ) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b) WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=True,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias WHERE no_match_inner_table.a = no_match_outer_table.a  ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table   ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=True,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT DISTINCT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('SUM', False)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT SUM(no_match_inner_table.a) AS c FROM no_match_inner_table   ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=AGGREGATE,subquery_operator_type=('COUNT', True)
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT COUNT(no_match_inner_table.a) AS c FROM no_match_inner_table  GROUP BY a ORDER BY c DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=1
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=LIMIT,subquery_operator_type=10
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=ORDER BY,subquery_operator_type=None
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=INNER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table INNER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=LEFT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table LEFT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=JOIN,subquery_operator_type=RIGHT OUTER JOIN
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT no_match_inner_table.a FROM no_match_inner_table RIGHT OUTER JOIN join_table ON no_match_inner_table.b = join_table.b)   ORDER BY no_match_inner_table.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=INTERSECT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table INTERSECT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=UNION
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table UNION SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
-- subquery_in=WHERE,subquery_type==,is_correlated=False,distinct=False,subquery_operator=SET_OP,subquery_operator_type=EXCEPT
SELECT a, b FROM no_match_outer_table WHERE no_match_outer_table.a =(SELECT set_alias.a FROM (SELECT a, b FROM no_match_inner_table EXCEPT SELECT a, b FROM join_table) AS set_alias   ORDER BY set_alias.a DESC LIMIT 1) ORDER BY a, b;
