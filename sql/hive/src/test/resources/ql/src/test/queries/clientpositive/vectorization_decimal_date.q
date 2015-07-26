CREATE TABLE date_decimal_test STORED AS ORC AS SELECT cint, cdouble, CAST (CAST (cint AS TIMESTAMP) AS DATE) AS cdate, CAST (((cdouble*22.1)/37) AS DECIMAL(20,10)) AS cdecimal FROM alltypesorc;
SET hive.vectorized.execution.enabled=true;
EXPLAIN SELECT cdate, cdecimal from date_decimal_test where cint IS NOT NULL AND cdouble IS NOT NULL LIMIT 10;
SELECT cdate, cdecimal from date_decimal_test where cint IS NOT NULL AND cdouble IS NOT NULL LIMIT 10;
