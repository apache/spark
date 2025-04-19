--ONLY_IF spark

set spark.sql.optimizer.supportNestedCorrelatedSubqueries.enabled=true;

DROP TABLE IF EXISTS t;
CREATE TABLE t(ps_supplycost INT, n_name INT);

SELECT NULL
FROM
    t AS ref_2,
    LATERAL (SELECT (SELECT NULL
         FROM (SELECT * FROM t AS ref_5,
              LATERAL (SELECT ref_5.ps_supplycost AS c0,
                      ref_2.n_name AS c1) AS alias1) AS alias2) AS alias3) AS alias4;

SELECT *
FROM
    t AS ref_2,
    LATERAL (SELECT (SELECT NULL
         FROM (SELECT * FROM t AS ref_5,
              LATERAL (SELECT ref_5.ps_supplycost AS c0,
                      ref_2.n_name AS c1) AS alias1) AS alias2) AS alias3) AS alias4;

