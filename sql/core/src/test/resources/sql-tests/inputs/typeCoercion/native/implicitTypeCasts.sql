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

-- ImplicitTypeCasts

CREATE TEMPORARY VIEW t AS SELECT 1;

SELECT 1 + '2' FROM t;
SELECT 1 - '2' FROM t;
SELECT 1 * '2' FROM t;
SELECT 4 / '2' FROM t;
SELECT 1.1 + '2' FROM t;
SELECT 1.1 - '2' FROM t;
SELECT 1.1 * '2' FROM t;
SELECT 4.4 / '2' FROM t;
SELECT 1.1 + '2.2' FROM t;
SELECT 1.1 - '2.2' FROM t;
SELECT 1.1 * '2.2' FROM t;
SELECT 4.4 / '2.2' FROM t;

-- concatenation
SELECT '$' || cast(1 as smallint) || '$' FROM t;
SELECT '$' || 1 || '$' FROM t;
SELECT '$' || cast(1 as bigint) || '$' FROM t;
SELECT '$' || cast(1.1 as float) || '$' FROM t;
SELECT '$' || cast(1.1 as double) || '$' FROM t;
SELECT '$' || 1.1 || '$' FROM t;
SELECT '$' || cast(1.1 as decimal(8,3)) || '$' FROM t;
SELECT '$' || 'abcd' || '$' FROM t;
SELECT '$' || date('1996-09-09') || '$' FROM t;
SELECT '$' || timestamp('1996-09-09 10:11:12.4' )|| '$' FROM t;

-- length functions
SELECT length(cast(1 as smallint)) FROM t;
SELECT length(cast(1 as int)) FROM t;
SELECT length(cast(1 as bigint)) FROM t;
SELECT length(cast(1.1 as float)) FROM t;
SELECT length(cast(1.1 as double)) FROM t;
SELECT length(1.1) FROM t;
SELECT length(cast(1.1 as decimal(8,3))) FROM t;
SELECT length('four') FROM t;
SELECT length(date('1996-09-10')) FROM t;
SELECT length(timestamp('1996-09-10 10:11:12.4')) FROM t;

-- extract
SELECT year( '1996-01-10') FROM t;
SELECT month( '1996-01-10') FROM t;
SELECT day( '1996-01-10') FROM t;
SELECT hour( '10:11:12') FROM t;
SELECT minute( '10:11:12') FROM t;
SELECT second( '10:11:12') FROM t;

-- like
select 1 like '%' FROM t;
select date('1996-09-10') like '19%' FROM t;
select '1' like 1 FROM t;
select '1 ' like 1 FROM t;
select '1996-09-10' like date('1996-09-10') FROM t;
