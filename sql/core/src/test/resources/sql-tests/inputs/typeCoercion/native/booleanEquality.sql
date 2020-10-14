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

SELECT true = cast(1 as tinyint) FROM t;
SELECT true = cast(1 as smallint) FROM t;
SELECT true = cast(1 as int) FROM t;
SELECT true = cast(1 as bigint) FROM t;
SELECT true = cast(1 as float) FROM t;
SELECT true = cast(1 as double) FROM t;
SELECT true = cast(1 as decimal(10, 0)) FROM t;
SELECT true = cast(1 as string) FROM t;
SELECT true = cast('1' as binary) FROM t;
SELECT true = cast(1 as boolean) FROM t;
SELECT true = cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT true = cast('2017-12-11 09:30:00' as date) FROM t;

SELECT true <=> cast(1 as tinyint) FROM t;
SELECT true <=> cast(1 as smallint) FROM t;
SELECT true <=> cast(1 as int) FROM t;
SELECT true <=> cast(1 as bigint) FROM t;
SELECT true <=> cast(1 as float) FROM t;
SELECT true <=> cast(1 as double) FROM t;
SELECT true <=> cast(1 as decimal(10, 0)) FROM t;
SELECT true <=> cast(1 as string) FROM t;
SELECT true <=> cast('1' as binary) FROM t;
SELECT true <=> cast(1 as boolean) FROM t;
SELECT true <=> cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT true <=> cast('2017-12-11 09:30:00' as date) FROM t;

SELECT cast(1 as tinyint) = true FROM t;
SELECT cast(1 as smallint) = true FROM t;
SELECT cast(1 as int) = true FROM t;
SELECT cast(1 as bigint) = true FROM t;
SELECT cast(1 as float) = true FROM t;
SELECT cast(1 as double) = true FROM t;
SELECT cast(1 as decimal(10, 0)) = true FROM t;
SELECT cast(1 as string) = true FROM t;
SELECT cast('1' as binary) = true FROM t;
SELECT cast(1 as boolean) = true FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) = true FROM t;
SELECT cast('2017-12-11 09:30:00' as date) = true FROM t;

SELECT cast(1 as tinyint) <=> true FROM t;
SELECT cast(1 as smallint) <=> true FROM t;
SELECT cast(1 as int) <=> true FROM t;
SELECT cast(1 as bigint) <=> true FROM t;
SELECT cast(1 as float) <=> true FROM t;
SELECT cast(1 as double) <=> true FROM t;
SELECT cast(1 as decimal(10, 0)) <=> true FROM t;
SELECT cast(1 as string) <=> true FROM t;
SELECT cast('1' as binary) <=> true FROM t;
SELECT cast(1 as boolean) <=> true FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) <=> true FROM t;
SELECT cast('2017-12-11 09:30:00' as date) <=> true FROM t;

SELECT false = cast(0 as tinyint) FROM t;
SELECT false = cast(0 as smallint) FROM t;
SELECT false = cast(0 as int) FROM t;
SELECT false = cast(0 as bigint) FROM t;
SELECT false = cast(0 as float) FROM t;
SELECT false = cast(0 as double) FROM t;
SELECT false = cast(0 as decimal(10, 0)) FROM t;
SELECT false = cast(0 as string) FROM t;
SELECT false = cast('0' as binary) FROM t;
SELECT false = cast(0 as boolean) FROM t;
SELECT false = cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT false = cast('2017-12-11 09:30:00' as date) FROM t;

SELECT false <=> cast(0 as tinyint) FROM t;
SELECT false <=> cast(0 as smallint) FROM t;
SELECT false <=> cast(0 as int) FROM t;
SELECT false <=> cast(0 as bigint) FROM t;
SELECT false <=> cast(0 as float) FROM t;
SELECT false <=> cast(0 as double) FROM t;
SELECT false <=> cast(0 as decimal(10, 0)) FROM t;
SELECT false <=> cast(0 as string) FROM t;
SELECT false <=> cast('0' as binary) FROM t;
SELECT false <=> cast(0 as boolean) FROM t;
SELECT false <=> cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT false <=> cast('2017-12-11 09:30:00' as date) FROM t;

SELECT cast(0 as tinyint) = false FROM t;
SELECT cast(0 as smallint) = false FROM t;
SELECT cast(0 as int) = false FROM t;
SELECT cast(0 as bigint) = false FROM t;
SELECT cast(0 as float) = false FROM t;
SELECT cast(0 as double) = false FROM t;
SELECT cast(0 as decimal(10, 0)) = false FROM t;
SELECT cast(0 as string) = false FROM t;
SELECT cast('0' as binary) = false FROM t;
SELECT cast(0 as boolean) = false FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) = false FROM t;
SELECT cast('2017-12-11 09:30:00' as date) = false FROM t;

SELECT cast(0 as tinyint) <=> false FROM t;
SELECT cast(0 as smallint) <=> false FROM t;
SELECT cast(0 as int) <=> false FROM t;
SELECT cast(0 as bigint) <=> false FROM t;
SELECT cast(0 as float) <=> false FROM t;
SELECT cast(0 as double) <=> false FROM t;
SELECT cast(0 as decimal(10, 0)) <=> false FROM t;
SELECT cast(0 as string) <=> false FROM t;
SELECT cast('0' as binary) <=> false FROM t;
SELECT cast(0 as boolean) <=> false FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) <=> false FROM t;
SELECT cast('2017-12-11 09:30:00' as date) <=> false FROM t;
