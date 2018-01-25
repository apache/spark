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

-- arithmetic operations causing an overflow return NULL
select (5e36 + 0.1) + 5e36;
select (-4e36 - 0.1) - 7e36;
select 12345678901234567890.0 * 12345678901234567890.0;
select 1e35 / 0.1;

-- arithmetic operations causing a precision loss return NULL
select 123456789123456789.1234567890 * 1.123456789123456789;
select 0.001 / 9876543210987654321098765432109876543.2
