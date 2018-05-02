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

SELECT CASE WHEN true THEN cast(1 as tinyint) ELSE cast(2 as tinyint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as tinyint) ELSE cast(2 as smallint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as tinyint) ELSE cast(2 as int) END FROM t;
SELECT CASE WHEN true THEN cast(1 as tinyint) ELSE cast(2 as bigint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as tinyint) ELSE cast(2 as float) END FROM t;
SELECT CASE WHEN true THEN cast(1 as tinyint) ELSE cast(2 as double) END FROM t;
SELECT CASE WHEN true THEN cast(1 as tinyint) ELSE cast(2 as decimal(10, 0)) END FROM t;
SELECT CASE WHEN true THEN cast(1 as tinyint) ELSE cast(2 as string) END FROM t;
SELECT CASE WHEN true THEN cast(1 as tinyint) ELSE cast('2' as binary) END FROM t;
SELECT CASE WHEN true THEN cast(1 as tinyint) ELSE cast(2 as boolean) END FROM t;
SELECT CASE WHEN true THEN cast(1 as tinyint) ELSE cast('2017-12-11 09:30:00.0' as timestamp) END FROM t;
SELECT CASE WHEN true THEN cast(1 as tinyint) ELSE cast('2017-12-11 09:30:00' as date) END FROM t;

SELECT CASE WHEN true THEN cast(1 as smallint) ELSE cast(2 as tinyint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as smallint) ELSE cast(2 as smallint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as smallint) ELSE cast(2 as int) END FROM t;
SELECT CASE WHEN true THEN cast(1 as smallint) ELSE cast(2 as bigint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as smallint) ELSE cast(2 as float) END FROM t;
SELECT CASE WHEN true THEN cast(1 as smallint) ELSE cast(2 as double) END FROM t;
SELECT CASE WHEN true THEN cast(1 as smallint) ELSE cast(2 as decimal(10, 0)) END FROM t;
SELECT CASE WHEN true THEN cast(1 as smallint) ELSE cast(2 as string) END FROM t;
SELECT CASE WHEN true THEN cast(1 as smallint) ELSE cast('2' as binary) END FROM t;
SELECT CASE WHEN true THEN cast(1 as smallint) ELSE cast(2 as boolean) END FROM t;
SELECT CASE WHEN true THEN cast(1 as smallint) ELSE cast('2017-12-11 09:30:00.0' as timestamp) END FROM t;
SELECT CASE WHEN true THEN cast(1 as smallint) ELSE cast('2017-12-11 09:30:00' as date) END FROM t;

SELECT CASE WHEN true THEN cast(1 as int) ELSE cast(2 as tinyint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as int) ELSE cast(2 as smallint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as int) ELSE cast(2 as int) END FROM t;
SELECT CASE WHEN true THEN cast(1 as int) ELSE cast(2 as bigint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as int) ELSE cast(2 as float) END FROM t;
SELECT CASE WHEN true THEN cast(1 as int) ELSE cast(2 as double) END FROM t;
SELECT CASE WHEN true THEN cast(1 as int) ELSE cast(2 as decimal(10, 0)) END FROM t;
SELECT CASE WHEN true THEN cast(1 as int) ELSE cast(2 as string) END FROM t;
SELECT CASE WHEN true THEN cast(1 as int) ELSE cast('2' as binary) END FROM t;
SELECT CASE WHEN true THEN cast(1 as int) ELSE cast(2 as boolean) END FROM t;
SELECT CASE WHEN true THEN cast(1 as int) ELSE cast('2017-12-11 09:30:00.0' as timestamp) END FROM t;
SELECT CASE WHEN true THEN cast(1 as int) ELSE cast('2017-12-11 09:30:00' as date) END FROM t;

SELECT CASE WHEN true THEN cast(1 as bigint) ELSE cast(2 as tinyint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as bigint) ELSE cast(2 as smallint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as bigint) ELSE cast(2 as int) END FROM t;
SELECT CASE WHEN true THEN cast(1 as bigint) ELSE cast(2 as bigint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as bigint) ELSE cast(2 as float) END FROM t;
SELECT CASE WHEN true THEN cast(1 as bigint) ELSE cast(2 as double) END FROM t;
SELECT CASE WHEN true THEN cast(1 as bigint) ELSE cast(2 as decimal(10, 0)) END FROM t;
SELECT CASE WHEN true THEN cast(1 as bigint) ELSE cast(2 as string) END FROM t;
SELECT CASE WHEN true THEN cast(1 as bigint) ELSE cast('2' as binary) END FROM t;
SELECT CASE WHEN true THEN cast(1 as bigint) ELSE cast(2 as boolean) END FROM t;
SELECT CASE WHEN true THEN cast(1 as bigint) ELSE cast('2017-12-11 09:30:00.0' as timestamp) END FROM t;
SELECT CASE WHEN true THEN cast(1 as bigint) ELSE cast('2017-12-11 09:30:00' as date) END FROM t;

SELECT CASE WHEN true THEN cast(1 as float) ELSE cast(2 as tinyint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as float) ELSE cast(2 as smallint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as float) ELSE cast(2 as int) END FROM t;
SELECT CASE WHEN true THEN cast(1 as float) ELSE cast(2 as bigint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as float) ELSE cast(2 as float) END FROM t;
SELECT CASE WHEN true THEN cast(1 as float) ELSE cast(2 as double) END FROM t;
SELECT CASE WHEN true THEN cast(1 as float) ELSE cast(2 as decimal(10, 0)) END FROM t;
SELECT CASE WHEN true THEN cast(1 as float) ELSE cast(2 as string) END FROM t;
SELECT CASE WHEN true THEN cast(1 as float) ELSE cast('2' as binary) END FROM t;
SELECT CASE WHEN true THEN cast(1 as float) ELSE cast(2 as boolean) END FROM t;
SELECT CASE WHEN true THEN cast(1 as float) ELSE cast('2017-12-11 09:30:00.0' as timestamp) END FROM t;
SELECT CASE WHEN true THEN cast(1 as float) ELSE cast('2017-12-11 09:30:00' as date) END FROM t;

SELECT CASE WHEN true THEN cast(1 as double) ELSE cast(2 as tinyint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as double) ELSE cast(2 as smallint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as double) ELSE cast(2 as int) END FROM t;
SELECT CASE WHEN true THEN cast(1 as double) ELSE cast(2 as bigint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as double) ELSE cast(2 as float) END FROM t;
SELECT CASE WHEN true THEN cast(1 as double) ELSE cast(2 as double) END FROM t;
SELECT CASE WHEN true THEN cast(1 as double) ELSE cast(2 as decimal(10, 0)) END FROM t;
SELECT CASE WHEN true THEN cast(1 as double) ELSE cast(2 as string) END FROM t;
SELECT CASE WHEN true THEN cast(1 as double) ELSE cast('2' as binary) END FROM t;
SELECT CASE WHEN true THEN cast(1 as double) ELSE cast(2 as boolean) END FROM t;
SELECT CASE WHEN true THEN cast(1 as double) ELSE cast('2017-12-11 09:30:00.0' as timestamp) END FROM t;
SELECT CASE WHEN true THEN cast(1 as double) ELSE cast('2017-12-11 09:30:00' as date) END FROM t;

SELECT CASE WHEN true THEN cast(1 as decimal(10, 0)) ELSE cast(2 as tinyint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as decimal(10, 0)) ELSE cast(2 as smallint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as decimal(10, 0)) ELSE cast(2 as int) END FROM t;
SELECT CASE WHEN true THEN cast(1 as decimal(10, 0)) ELSE cast(2 as bigint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as decimal(10, 0)) ELSE cast(2 as float) END FROM t;
SELECT CASE WHEN true THEN cast(1 as decimal(10, 0)) ELSE cast(2 as double) END FROM t;
SELECT CASE WHEN true THEN cast(1 as decimal(10, 0)) ELSE cast(2 as decimal(10, 0)) END FROM t;
SELECT CASE WHEN true THEN cast(1 as decimal(10, 0)) ELSE cast(2 as string) END FROM t;
SELECT CASE WHEN true THEN cast(1 as decimal(10, 0)) ELSE cast('2' as binary) END FROM t;
SELECT CASE WHEN true THEN cast(1 as decimal(10, 0)) ELSE cast(2 as boolean) END FROM t;
SELECT CASE WHEN true THEN cast(1 as decimal(10, 0)) ELSE cast('2017-12-11 09:30:00.0' as timestamp) END FROM t;
SELECT CASE WHEN true THEN cast(1 as decimal(10, 0)) ELSE cast('2017-12-11 09:30:00' as date) END FROM t;

SELECT CASE WHEN true THEN cast(1 as string) ELSE cast(2 as tinyint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as string) ELSE cast(2 as smallint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as string) ELSE cast(2 as int) END FROM t;
SELECT CASE WHEN true THEN cast(1 as string) ELSE cast(2 as bigint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as string) ELSE cast(2 as float) END FROM t;
SELECT CASE WHEN true THEN cast(1 as string) ELSE cast(2 as double) END FROM t;
SELECT CASE WHEN true THEN cast(1 as string) ELSE cast(2 as decimal(10, 0)) END FROM t;
SELECT CASE WHEN true THEN cast(1 as string) ELSE cast(2 as string) END FROM t;
SELECT CASE WHEN true THEN cast(1 as string) ELSE cast('2' as binary) END FROM t;
SELECT CASE WHEN true THEN cast(1 as string) ELSE cast(2 as boolean) END FROM t;
SELECT CASE WHEN true THEN cast(1 as string) ELSE cast('2017-12-11 09:30:00.0' as timestamp) END FROM t;
SELECT CASE WHEN true THEN cast(1 as string) ELSE cast('2017-12-11 09:30:00' as date) END FROM t;

SELECT CASE WHEN true THEN cast('1' as binary) ELSE cast(2 as tinyint) END FROM t;
SELECT CASE WHEN true THEN cast('1' as binary) ELSE cast(2 as smallint) END FROM t;
SELECT CASE WHEN true THEN cast('1' as binary) ELSE cast(2 as int) END FROM t;
SELECT CASE WHEN true THEN cast('1' as binary) ELSE cast(2 as bigint) END FROM t;
SELECT CASE WHEN true THEN cast('1' as binary) ELSE cast(2 as float) END FROM t;
SELECT CASE WHEN true THEN cast('1' as binary) ELSE cast(2 as double) END FROM t;
SELECT CASE WHEN true THEN cast('1' as binary) ELSE cast(2 as decimal(10, 0)) END FROM t;
SELECT CASE WHEN true THEN cast('1' as binary) ELSE cast(2 as string) END FROM t;
SELECT CASE WHEN true THEN cast('1' as binary) ELSE cast('2' as binary) END FROM t;
SELECT CASE WHEN true THEN cast('1' as binary) ELSE cast(2 as boolean) END FROM t;
SELECT CASE WHEN true THEN cast('1' as binary) ELSE cast('2017-12-11 09:30:00.0' as timestamp) END FROM t;
SELECT CASE WHEN true THEN cast('1' as binary) ELSE cast('2017-12-11 09:30:00' as date) END FROM t;

SELECT CASE WHEN true THEN cast(1 as boolean) ELSE cast(2 as tinyint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as boolean) ELSE cast(2 as smallint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as boolean) ELSE cast(2 as int) END FROM t;
SELECT CASE WHEN true THEN cast(1 as boolean) ELSE cast(2 as bigint) END FROM t;
SELECT CASE WHEN true THEN cast(1 as boolean) ELSE cast(2 as float) END FROM t;
SELECT CASE WHEN true THEN cast(1 as boolean) ELSE cast(2 as double) END FROM t;
SELECT CASE WHEN true THEN cast(1 as boolean) ELSE cast(2 as decimal(10, 0)) END FROM t;
SELECT CASE WHEN true THEN cast(1 as boolean) ELSE cast(2 as string) END FROM t;
SELECT CASE WHEN true THEN cast(1 as boolean) ELSE cast('2' as binary) END FROM t;
SELECT CASE WHEN true THEN cast(1 as boolean) ELSE cast(2 as boolean) END FROM t;
SELECT CASE WHEN true THEN cast(1 as boolean) ELSE cast('2017-12-11 09:30:00.0' as timestamp) END FROM t;
SELECT CASE WHEN true THEN cast(1 as boolean) ELSE cast('2017-12-11 09:30:00' as date) END FROM t;

SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00.0' as timestamp) ELSE cast(2 as tinyint) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00.0' as timestamp) ELSE cast(2 as smallint) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00.0' as timestamp) ELSE cast(2 as int) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00.0' as timestamp) ELSE cast(2 as bigint) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00.0' as timestamp) ELSE cast(2 as float) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00.0' as timestamp) ELSE cast(2 as double) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00.0' as timestamp) ELSE cast(2 as decimal(10, 0)) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00.0' as timestamp) ELSE cast(2 as string) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00.0' as timestamp) ELSE cast('2' as binary) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00.0' as timestamp) ELSE cast(2 as boolean) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00.0' as timestamp) ELSE cast('2017-12-11 09:30:00.0' as timestamp) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00.0' as timestamp) ELSE cast('2017-12-11 09:30:00' as date) END FROM t;

SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00' as date) ELSE cast(2 as tinyint) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00' as date) ELSE cast(2 as smallint) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00' as date) ELSE cast(2 as int) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00' as date) ELSE cast(2 as bigint) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00' as date) ELSE cast(2 as float) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00' as date) ELSE cast(2 as double) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00' as date) ELSE cast(2 as decimal(10, 0)) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00' as date) ELSE cast(2 as string) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00' as date) ELSE cast('2' as binary) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00' as date) ELSE cast(2 as boolean) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00' as date) ELSE cast('2017-12-11 09:30:00.0' as timestamp) END FROM t;
SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00' as date) ELSE cast('2017-12-11 09:30:00' as date) END FROM t;
