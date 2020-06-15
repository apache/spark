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

CREATE TEMPORARY VIEW t AS SELECT 'aa' as a;

-- casting to data types which are unable to represent the string input returns NULL
select cast(a as byte) from t;
select cast(a as short) from t;
select cast(a as int) from t;
select cast(a as long) from t;
select cast(a as float) from t;
select cast(a as double) from t;
select cast(a as decimal) from t;
select cast(a as boolean) from t;
select cast(a as timestamp) from t;
select cast(a as date) from t;
-- casting to binary works correctly
select cast(a as binary) from t;
-- casting to array, struct or map throws exception
select cast(a as array<string>) from t;
select cast(a as struct<s:string>) from t;
select cast(a as map<string, string>) from t;

-- all timestamp/date expressions return NULL if bad input strings are provided
select to_timestamp(a) from t;
select to_timestamp('2018-01-01', a) from t;
select to_unix_timestamp(a) from t;
select to_unix_timestamp('2018-01-01', a) from t;
select unix_timestamp(a) from t;
select unix_timestamp('2018-01-01', a) from t;
select from_unixtime(a) from t;
select from_unixtime('2018-01-01', a) from t;
select next_day(a, 'MO') from t;
select next_day('2018-01-01', a) from t;
select trunc(a, 'MM') from t;
select trunc('2018-01-01', a) from t;

-- some functions return NULL if bad input is provided
select unhex('-123');
select sha2(a, a) from t;
select get_json_object(a, a) from t;
select json_tuple(a, a) from t;
select from_json(a, 'a INT') from t;
