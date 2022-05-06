-- Tests for conditional functions
--SET spark.sql.maxMetadataStringLength = 500

CREATE TABLE t USING PARQUET AS SELECT c1, c2 FROM VALUES(1d, 0),(2d, 1),(null, 1),(CAST('NaN' AS DOUBLE), 0) AS t(c1, c2);

SELECT nanvl(c1, c1/c2 + c1/c2) FROM t;
-- do not fail at compile side
EXPLAIN SELECT nanvl(c1, c1/c2 + c1/c2) FROM t;

SELECT nanvl(c2, c1/c2 + c1/c2) FROM t;
SELECT nanvl(c1, 1/0) FROM t;
SELECT nanvl(1-0, 1/0) FROM t;
SELECT nanvl(1/0, 1/0) FROM t;

SELECT if(c2 > 2, 1-0, 1/0) from t;
-- do not fail at compile side
EXPLAIN SELECT if(c2 > 2, 1-0, 1/0) from t;

SELECT if(1 == 1, 1, 1/0);
SELECT if(1 == 1, 1/0, 1);
SELECT if(1 == 1/0, 1, 1/0);

SELECT coalesce(c1, 1/0) from t;
-- do not fail at compile side
EXPLAIN SELECT coalesce(c1, 1/0) from t;

SELECT coalesce(1, 1/0);
SELECT coalesce(null, 1/0);
SELECT coalesce(null, 1, 1/0);
SELECT coalesce(null, 1/0, 1);

SELECT case when c2 > 2 then 1 else 1/0 end from t;
-- do not fail at compile side
EXPLAIN SELECT case when c2 > 2 then 1 else 1/0 end from t;

SELECT case when 1 < 2 then 1 else 1/0 end;
SELECT case when 1 < 2/0 then 1 else 1/0 end;
SELECT case when 1 > 2 then 1 else 1/0 end;
SELECT case when 1 > 2 then 1/0 else 1 end;

DROP TABLE IF EXISTS t;
