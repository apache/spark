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

CREATE TEMPORARY VIEW t AS SELECT 1;

select cast(1 as tinyint) + interval 2 day;
select cast(1 as smallint) + interval 2 day;
select cast(1 as int) + interval 2 day;
select cast(1 as bigint) + interval 2 day;
select cast(1 as float) + interval 2 day;
select cast(1 as double) + interval 2 day;
select cast(1 as decimal(10, 0)) + interval 2 day;
select cast('2017-12-11' as string) + interval 2 day;
select cast('2017-12-11 09:30:00' as string) + interval 2 day;
select cast('1' as binary) + interval 2 day;
select cast(1 as boolean) + interval 2 day;
select cast('2017-12-11 09:30:00.0' as timestamp) + interval 2 day;
select cast('2017-12-11 09:30:00' as date) + interval 2 day;

select interval 2 day + cast(1 as tinyint);
select interval 2 day + cast(1 as smallint);
select interval 2 day + cast(1 as int);
select interval 2 day + cast(1 as bigint);
select interval 2 day + cast(1 as float);
select interval 2 day + cast(1 as double);
select interval 2 day + cast(1 as decimal(10, 0));
select interval 2 day + cast('2017-12-11' as string);
select interval 2 day + cast('2017-12-11 09:30:00' as string);
select interval 2 day + cast('1' as binary);
select interval 2 day + cast(1 as boolean);
select interval 2 day + cast('2017-12-11 09:30:00.0' as timestamp);
select interval 2 day + cast('2017-12-11 09:30:00' as date);

select cast(1 as tinyint) - interval 2 day;
select cast(1 as smallint) - interval 2 day;
select cast(1 as int) - interval 2 day;
select cast(1 as bigint) - interval 2 day;
select cast(1 as float) - interval 2 day;
select cast(1 as double) - interval 2 day;
select cast(1 as decimal(10, 0)) - interval 2 day;
select cast('2017-12-11' as string) - interval 2 day;
select cast('2017-12-11 09:30:00' as string) - interval 2 day;
select cast('1' as binary) - interval 2 day;
select cast(1 as boolean) - interval 2 day;
select cast('2017-12-11 09:30:00.0' as timestamp) - interval 2 day;
select cast('2017-12-11 09:30:00' as date) - interval 2 day;

create temporary view intervals as select * from values (interval 1 day 2 hours, 33, date '2018-05-14', timestamp '2018-05-14 18:31:00'), (interval 3 months 1 second, 2, date '2017-05-30', timestamp '2017-05-30 10:22:40'), (null, 2, date '2015-04-30', timestamp '2015-04-30 13:30:31'), (interval 1 year 3 weeks, null, date '2019-01-23', timestamp '2019-01-23 19:24:13') as hav(i, m, d, t);

select i, m, d, t, i * m as im, m * i as mi, d + i as di, d + i * m as dim, t + m * i as tmi from intervals;
