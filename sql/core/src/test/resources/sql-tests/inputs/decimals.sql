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

-- tests for decimals handling in operations
create table decimals_test(id int, a decimal(38,18), b decimal(38,18)) using parquet;

insert into decimals_test values(1, 100.0, 999.0), (2, 12345.123, 12345.123),
  (3, 0.1234567891011, 1234.1), (4, 123456789123456789.0, 1.123456789123456789);

-- test decimal operations
select id, a+b, a-b, a*b, a/b from decimals_test order by id;

-- test operations between decimals and constants
select id, a*10, b/10 from decimals_test order by id;

-- use rounding instead of returning NULL, according to new Hive's behavior and SQL standard
set spark.sql.decimalOperations.allowTruncat=true;

-- test decimal operations
select id, a+b, a-b, a*b, a/b from decimals_test order by id;

-- test operations between decimals and constants
select id, a*10, b/10 from decimals_test order by id;

drop table decimals_test;
