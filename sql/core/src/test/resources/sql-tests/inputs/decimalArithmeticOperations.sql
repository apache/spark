--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--

CREATE TEMPORARY VIEW t AS SELECT 1.0 as a, 0.0 as b;

-- division, remainder and pmod by 0 return NULL
select a / b from t;
select a % b from t;
select pmod(a, b) from t;

-- tests for decimals handling in operations
create table decimals_test(id int, a decimal(38,18), b decimal(38,18)) using parquet;

insert into decimals_test values(1, 100.0, 999.0), (2, 12345.123, 12345.123),
  (3, 0.1234567891011, 1234.1), (4, 123456789123456789.0, 1.123456789123456789);

-- test decimal operations
select id, a+b, a-b, a*b, a/b from decimals_test order by id;

-- test operations between decimals and constants
select id, a*10, b/10 from decimals_test order by id;

-- test operations on constants
select 10.3 * 3.0;
select 10.3000 * 3.0;
select 10.30000 * 30.0;
select 10.300000000000000000 * 3.000000000000000000;
select 10.300000000000000000 * 3.0000000000000000000;
select 2.35E10 * 1.0;

-- arithmetic operations causing an overflow return NULL
select (5e36BD + 0.1) + 5e36BD;
select (-4e36BD - 0.1) - 7e36BD;
select 12345678901234567890.0 * 12345678901234567890.0;
select 1e35BD / 0.1;
select 1.2345678901234567890E30BD * 1.2345678901234567890E25BD;

-- arithmetic operations causing a precision loss are truncated
select 12345678912345678912345678912.1234567 + 9999999999999999999999999999999.12345;
select 123456789123456789.1234567890 * 1.123456789123456789;
select 12345678912345.123456789123 / 0.000000012345678;

-- union decimal type

SELECT CAST(20 AS DECIMAL(4, 1))
UNION ALL
SELECT CAST(10 AS DECIMAL(3, 1)) + CAST(90 AS DECIMAL(3, 1));

SELECT CAST(20 AS DECIMAL(4, 1))
UNION ALL
SELECT CAST(10 AS DECIMAL(3, 1)) - CAST(-90 AS DECIMAL(3, 1));

SELECT CAST(20 AS DECIMAL(4, 1))
UNION ALL
SELECT CAST(10 AS DECIMAL(3, 1)) * CAST(10 AS DECIMAL(3, 1));

SELECT CAST(20 AS DECIMAL(4, 1))
UNION ALL
SELECT CAST(10 AS DECIMAL(3, 1)) / CAST(10 AS DECIMAL(3, 1));

SELECT CAST(20 AS DECIMAL(4, 1))
UNION ALL
SELECT CAST(10 AS DECIMAL(10, 2)) % CAST(3 AS DECIMAL(5, 1));

SELECT CAST(20 AS DECIMAL(4, 1))
UNION ALL
SELECT pmod(CAST(10 AS DECIMAL(10, 2)), CAST(3 AS DECIMAL(5, 1)));

SELECT CAST(20 AS DECIMAL(4, 1))
UNION ALL
SELECT CAST(10 AS DECIMAL(10, 2)) div CAST(3 AS DECIMAL(5, 1));

-- return NULL instead of rounding, according to old Spark versions' behavior
set spark.sql.decimalOperations.allowPrecisionLoss=false;

-- test decimal operations
select id, a+b, a-b, a*b, a/b from decimals_test order by id;

-- test operations between decimals and constants
select id, a*10, b/10 from decimals_test order by id;

-- test operations on constants
select 10.3 * 3.0;
select 10.3000 * 3.0;
select 10.30000 * 30.0;
select 10.300000000000000000 * 3.000000000000000000;
select 10.300000000000000000 * 3.0000000000000000000;
select 2.35E10 * 1.0;

-- arithmetic operations causing an overflow return NULL
select (5e36BD + 0.1) + 5e36BD;
select (-4e36BD - 0.1) - 7e36BD;
select 12345678901234567890.0 * 12345678901234567890.0;
select 1e35BD / 0.1;
select 1.2345678901234567890E30BD * 1.2345678901234567890E25BD;

-- arithmetic operations causing a precision loss return NULL
select 12345678912345678912345678912.1234567 + 9999999999999999999999999999999.12345;
select 123456789123456789.1234567890 * 1.123456789123456789;
select 12345678912345.123456789123 / 0.000000012345678;

select 1.0123456789012345678901234567890123456e36BD / 0.1;
select 1.0123456789012345678901234567890123456e35BD / 1.0;
select 1.0123456789012345678901234567890123456e34BD / 1.0;
select 1.0123456789012345678901234567890123456e33BD / 1.0;
select 1.0123456789012345678901234567890123456e32BD / 1.0;
select 1.0123456789012345678901234567890123456e31BD / 1.0;
select 1.0123456789012345678901234567890123456e31BD / 0.1;
select 1.0123456789012345678901234567890123456e31BD / 10.0;

drop table decimals_test;
