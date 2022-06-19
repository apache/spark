-- Tests for conditional functions

CREATE TABLE conditional_t USING PARQUET AS SELECT c1, c2 FROM VALUES(1d, 0),(2d, 1),(null, 1),(CAST('NaN' AS DOUBLE), 0) AS t(c1, c2);

SELECT nanvl(c2, c1/c2 + c1/c2) FROM conditional_t;
SELECT nanvl(c2, 1/0) FROM conditional_t;
SELECT nanvl(1-0, 1/0) FROM conditional_t;

SELECT if(c2 >= 0, 1-0, 1/0) from conditional_t;
SELECT if(1 == 1, 1, 1/0);
SELECT if(1 != 1, 1/0, 1);

SELECT coalesce(c2, 1/0) from conditional_t;
SELECT coalesce(1, 1/0);
SELECT coalesce(null, 1, 1/0);

SELECT case when c2 >= 0 then 1 else 1/0 end from conditional_t;
SELECT case when 1 < 2 then 1 else 1/0 end;
SELECT case when 1 > 2 then 1/0 else 1 end;

DROP TABLE conditional_t;
