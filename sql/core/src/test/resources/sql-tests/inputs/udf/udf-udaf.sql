    -- This test file was converted from udaf.sql.

CREATE OR REPLACE TEMPORARY VIEW t1 AS SELECT * FROM VALUES
(1), (2), (3), (4)
as t1(int_col1);

CREATE FUNCTION myDoubleAvg AS 'test.org.apache.spark.sql.MyDoubleAvg';

SELECT default.myDoubleAvg(udf(int_col1)) as my_avg, udf(default.myDoubleAvg(udf(int_col1))) as my_avg2, udf(default.myDoubleAvg(int_col1)) as my_avg3 from t1;

SELECT default.myDoubleAvg(udf(int_col1), udf(3)) as my_avg from t1;

CREATE FUNCTION udaf1 AS 'test.non.existent.udaf';

SELECT default.udaf1(udf(int_col1)) as udaf1, udf(default.udaf1(udf(int_col1))) as udaf2, udf(default.udaf1(int_col1)) as udaf3 from t1;

DROP FUNCTION myDoubleAvg;
DROP FUNCTION udaf1;