-- Tests for conditional functions
CREATE TABLE t USING PARQUET AS SELECT c1, c2 FROM VALUES(1, 0),(2, 1) AS t(c1, c2);

SELECT nanvl(c1, c1/c2 + c1/c2) FROM t;

DROP TABLE IF EXISTS t;
