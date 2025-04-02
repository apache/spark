CREATE TABLE decimal_test STORED AS ORC AS SELECT cbigint, cdouble, CAST (((cdouble*22.1)/37) AS DECIMAL(20,10)) AS cdecimal1, CAST (((cdouble*9.3)/13) AS DECIMAL(23,14)) AS cdecimal2 FROM alltypesorc;
SET hive.vectorized.execution.enabled=true;

-- Test math functions in vectorized mode to verify they run correctly end-to-end.

explain 
select
   cdecimal1
  ,Round(cdecimal1, 2)
  ,Round(cdecimal1)
  ,Floor(cdecimal1)
  ,Ceil(cdecimal1)
  ,Exp(cdecimal1)
  ,Ln(cdecimal1)  
  ,Log10(cdecimal1)
  -- Use log2 as a representative function to test all input types.
  ,Log2(cdecimal1)
  -- Use 15601.0 to test zero handling, as there are no zeroes in the table
  ,Log2(cdecimal1 - 15601.0)
  ,Log(2.0, cdecimal1)
  ,Pow(log2(cdecimal1), 2.0)  
  ,Power(log2(cdecimal1), 2.0)
  ,Sqrt(cdecimal1)
  ,Abs(cdecimal1)
  ,Sin(cdecimal1)
  ,Asin(cdecimal1)
  ,Cos(cdecimal1)
  ,ACos(cdecimal1)
  ,Atan(cdecimal1)
  ,Degrees(cdecimal1)
  ,Radians(cdecimal1)
  ,Positive(cdecimal1)
  ,Negative(cdecimal1)
  ,Sign(cdecimal1)
  -- Test nesting
  ,cos(-sin(log(cdecimal1)) + 3.14159)
from decimal_test
-- limit output to a reasonably small number of rows
where cbigint % 500 = 0
-- test use of a math function in the WHERE clause
and sin(cdecimal1) >= -1.0;

select
   cdecimal1
  ,Round(cdecimal1, 2)
  ,Round(cdecimal1)
  ,Floor(cdecimal1)
  ,Ceil(cdecimal1)
  ,Exp(cdecimal1)
  ,Ln(cdecimal1)  
  ,Log10(cdecimal1)
  -- Use log2 as a representative function to test all input types.
  ,Log2(cdecimal1)
  -- Use 15601.0 to test zero handling, as there are no zeroes in the table
  ,Log2(cdecimal1 - 15601.0)
  ,Log(2.0, cdecimal1)
  ,Pow(log2(cdecimal1), 2.0)  
  ,Power(log2(cdecimal1), 2.0)
  ,Sqrt(cdecimal1)
  ,Abs(cdecimal1)
  ,Sin(cdecimal1)
  ,Asin(cdecimal1)
  ,Cos(cdecimal1)
  ,ACos(cdecimal1)
  ,Atan(cdecimal1)
  ,Degrees(cdecimal1)
  ,Radians(cdecimal1)
  ,Positive(cdecimal1)
  ,Negative(cdecimal1)
  ,Sign(cdecimal1)
  -- Test nesting
  ,cos(-sin(log(cdecimal1)) + 3.14159)
from decimal_test
-- limit output to a reasonably small number of rows
where cbigint % 500 = 0
-- test use of a math function in the WHERE clause
and sin(cdecimal1) >= -1.0;
