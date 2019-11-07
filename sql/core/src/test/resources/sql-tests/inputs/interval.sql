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

-- test for interacting with intervals

-- using SQL_STANDARD output style
set spark.sql.IntervalOutputStyle=SQL_STANDARD;
select interval 4 month 2 weeks 3 microseconds;
select interval '1 year 20 month';
select interval '-1 year -20 month';
select interval '20 month 30 days -21 hours 10 minutes 999 milliseconds';
select date'2019-10-15' - timestamp'2019-10-15 10:11:12.001002';

-- using MULTI_UNITS (which is default) output style
set spark.sql.IntervalOutputStyle=MULTI_UNITS;

-- interval operations
select 3 * (timestamp'2019-10-15 10:11:12.001002' - date'2019-10-15');
select interval 4 month 2 weeks 3 microseconds * 1.5;
select (timestamp'2019-10-15' - timestamp'2019-10-14') / 1.5;

-- interval operation with null and zero case
select interval '2 seconds' / 0;
select interval '2 seconds' / null;
select interval '2 seconds' * null;
select null * interval '2 seconds';
