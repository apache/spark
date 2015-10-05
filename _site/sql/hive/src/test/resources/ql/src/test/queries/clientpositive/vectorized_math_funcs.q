SET hive.vectorized.execution.enabled = true;

-- Test math functions in vectorized mode to verify they run correctly end-to-end.

explain 
select
   cdouble
  ,Round(cdouble, 2)
  ,Floor(cdouble)
  ,Ceil(cdouble)
  ,Rand()
  ,Rand(98007)
  ,Exp(ln(cdouble))
  ,Ln(cdouble)  
  ,Ln(cfloat)
  ,Log10(cdouble)
  -- Use log2 as a representative function to test all input types.
  ,Log2(cdouble)
  -- Use 15601.0 to test zero handling, as there are no zeroes in the table
  ,Log2(cdouble - 15601.0)
  ,Log2(cfloat)
  ,Log2(cbigint)
  ,Log2(cint)
  ,Log2(csmallint)
  ,Log2(ctinyint)
  ,Log(2.0, cdouble)
  ,Pow(log2(cdouble), 2.0)  
  ,Power(log2(cdouble), 2.0)
  ,Sqrt(cdouble)
  ,Sqrt(cbigint)
  ,Bin(cbigint)
  ,Hex(cdouble)
  ,Conv(cbigint, 10, 16)
  ,Abs(cdouble)
  ,Abs(ctinyint)
  ,Pmod(cint, 3)
  ,Sin(cdouble)
  ,Asin(cdouble)
  ,Cos(cdouble)
  ,ACos(cdouble)
  ,Atan(cdouble)
  ,Degrees(cdouble)
  ,Radians(cdouble)
  ,Positive(cdouble)
  ,Positive(cbigint)
  ,Negative(cdouble)
  ,Sign(cdouble)
  ,Sign(cbigint)
  -- Test nesting
  ,cos(-sin(log(cdouble)) + 3.14159)
from alltypesorc
-- limit output to a reasonably small number of rows
where cbigint % 500 = 0
-- test use of a math function in the WHERE clause
and sin(cfloat) >= -1.0;

select
   cdouble
  ,Round(cdouble, 2)
  ,Floor(cdouble)
  ,Ceil(cdouble)
  -- Omit rand() from runtime test because it's nondeterministic.
  -- ,Rand()
  ,Rand(98007)
  ,Exp(ln(cdouble))
  ,Ln(cdouble)  
  ,Ln(cfloat)
  ,Log10(cdouble)
  -- Use log2 as a representative function to test all input types.
  ,Log2(cdouble)
  -- Use 15601.0 to test zero handling, as there are no zeroes in the table
  ,Log2(cdouble - 15601.0)
  ,Log2(cfloat)
  ,Log2(cbigint)
  ,Log2(cint)
  ,Log2(csmallint)
  ,Log2(ctinyint)
  ,Log(2.0, cdouble)
  ,Pow(log2(cdouble), 2.0)  
  ,Power(log2(cdouble), 2.0)
  ,Sqrt(cdouble)
  ,Sqrt(cbigint)
  ,Bin(cbigint)
  ,Hex(cdouble)
  ,Conv(cbigint, 10, 16)
  ,Abs(cdouble)
  ,Abs(ctinyint)
  ,Pmod(cint, 3)
  ,Sin(cdouble)
  ,Asin(cdouble)
  ,Cos(cdouble)
  ,ACos(cdouble)
  ,Atan(cdouble)
  ,Degrees(cdouble)
  ,Radians(cdouble)
  ,Positive(cdouble)
  ,Positive(cbigint)
  ,Negative(cdouble)
  ,Sign(cdouble)
  ,Sign(cbigint)
  -- Test nesting
  ,cos(-sin(log(cdouble)) + 3.14159)
from alltypesorc
-- limit output to a reasonably small number of rows
where cbigint % 500 = 0
-- test use of a math function in the WHERE clause
and sin(cfloat) >= -1.0;
