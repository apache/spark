-- Temporary data.
create temporary view myview as values 128, 256 as v(int_col);

-- group by should produce all input rows,
select int_col, count(*) from myview group by int_col;

-- group by should produce a single row.
select 'foo', count(*) from myview group by 1;

-- group-by should not produce any rows (whole stage code generation).
select 'foo' from myview where int_col == 0 group by 1;

-- group-by should not produce any rows (hash aggregate).
select 'foo', approx_count_distinct(int_col) from myview where int_col == 0 group by 1;

-- group-by should not produce any rows (sort aggregate).
select 'foo', max(struct(int_col)) from myview where int_col == 0 group by 1;

-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);

-- Aggregate with empty GroupBy expressions.
SELECT a, COUNT(b) FROM testData;
SELECT COUNT(a), COUNT(b) FROM testData;

-- Aggregate with non-empty GroupBy expressions.
SELECT a, COUNT(b) FROM testData GROUP BY a;
SELECT a, COUNT(b) FROM testData GROUP BY b;
SELECT COUNT(a), COUNT(b) FROM testData GROUP BY a;

-- Aggregate with complex GroupBy expressions.
SELECT a + b, COUNT(b) FROM testData GROUP BY a + b;
SELECT a + 2, COUNT(b) FROM testData GROUP BY a + 1;
SELECT a + 1 + 1, COUNT(b) FROM testData GROUP BY a + 1;

-- Aggregate with nulls.
SELECT SKEWNESS(a), KURTOSIS(a), MIN(a), MAX(a), AVG(a), VARIANCE(a), STDDEV(a), SUM(a), COUNT(a)
FROM testData;