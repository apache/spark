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

-- Test with number values
SELECT NULLIF(1, 1);
SELECT NULLIF(1, 2);

-- Test with NULL values
SELECT NULLIF(NULL, 1);
SELECT NULLIF(1, NULL);
SELECT NULLIF(NULL, NULL);

-- Test with strings
SELECT NULLIF('abc', 'abc');
SELECT NULLIF('abc', 'xyz');

-- Test with more complex expressions
SELECT NULLIF(id, 1) FROM range(10) GROUP BY NULLIF(id, 1);
SELECT NULLIF(id, 1), COUNT(*) FROM range(10) GROUP BY NULLIF(id, 2);
SELECT NULLIF(id, 1), COUNT(*) FROM range(10) GROUP BY NULLIF(id, 1) HAVING COUNT(*) > 1;

SELECT nullifzero(0),
       nullifzero(cast(0 as tinyint)),
       nullifzero(cast(0 as bigint)),
       nullifzero('0'),
       nullifzero(0.0),
       nullifzero(1),
       nullifzero(null);

SELECT nullifzero('abc');

SELECT zeroifnull(null),
       zeroifnull(1),
       zeroifnull(cast(1 as tinyint)),
       zeroifnull(cast(1 as bigint));

SELECT zeroifnull('abc');

DROP TABLE conditional_t;
