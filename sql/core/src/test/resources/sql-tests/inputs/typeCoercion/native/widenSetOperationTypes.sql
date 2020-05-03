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

-- UNION
SELECT cast(1 as tinyint) FROM t UNION SELECT cast(2 as tinyint) FROM t;
SELECT cast(1 as tinyint) FROM t UNION SELECT cast(2 as smallint) FROM t;
SELECT cast(1 as tinyint) FROM t UNION SELECT cast(2 as int) FROM t;
SELECT cast(1 as tinyint) FROM t UNION SELECT cast(2 as bigint) FROM t;
SELECT cast(1 as tinyint) FROM t UNION SELECT cast(2 as float) FROM t;
SELECT cast(1 as tinyint) FROM t UNION SELECT cast(2 as double) FROM t;
SELECT cast(1 as tinyint) FROM t UNION SELECT cast(2 as decimal(10, 0)) FROM t;
SELECT cast(1 as tinyint) FROM t UNION SELECT cast(2 as string) FROM t;
SELECT cast(1 as tinyint) FROM t UNION SELECT cast('2' as binary) FROM t;
SELECT cast(1 as tinyint) FROM t UNION SELECT cast(2 as boolean) FROM t;
SELECT cast(1 as tinyint) FROM t UNION SELECT cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT cast(1 as tinyint) FROM t UNION SELECT cast('2017-12-11 09:30:00' as date) FROM t;

SELECT cast(1 as smallint) FROM t UNION SELECT cast(2 as tinyint) FROM t;
SELECT cast(1 as smallint) FROM t UNION SELECT cast(2 as smallint) FROM t;
SELECT cast(1 as smallint) FROM t UNION SELECT cast(2 as int) FROM t;
SELECT cast(1 as smallint) FROM t UNION SELECT cast(2 as bigint) FROM t;
SELECT cast(1 as smallint) FROM t UNION SELECT cast(2 as float) FROM t;
SELECT cast(1 as smallint) FROM t UNION SELECT cast(2 as double) FROM t;
SELECT cast(1 as smallint) FROM t UNION SELECT cast(2 as decimal(10, 0)) FROM t;
SELECT cast(1 as smallint) FROM t UNION SELECT cast(2 as string) FROM t;
SELECT cast(1 as smallint) FROM t UNION SELECT cast('2' as binary) FROM t;
SELECT cast(1 as smallint) FROM t UNION SELECT cast(2 as boolean) FROM t;
SELECT cast(1 as smallint) FROM t UNION SELECT cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT cast(1 as smallint) FROM t UNION SELECT cast('2017-12-11 09:30:00' as date) FROM t;

SELECT cast(1 as int) FROM t UNION SELECT cast(2 as tinyint) FROM t;
SELECT cast(1 as int) FROM t UNION SELECT cast(2 as smallint) FROM t;
SELECT cast(1 as int) FROM t UNION SELECT cast(2 as int) FROM t;
SELECT cast(1 as int) FROM t UNION SELECT cast(2 as bigint) FROM t;
SELECT cast(1 as int) FROM t UNION SELECT cast(2 as float) FROM t;
SELECT cast(1 as int) FROM t UNION SELECT cast(2 as double) FROM t;
SELECT cast(1 as int) FROM t UNION SELECT cast(2 as decimal(10, 0)) FROM t;
SELECT cast(1 as int) FROM t UNION SELECT cast(2 as string) FROM t;
SELECT cast(1 as int) FROM t UNION SELECT cast('2' as binary) FROM t;
SELECT cast(1 as int) FROM t UNION SELECT cast(2 as boolean) FROM t;
SELECT cast(1 as int) FROM t UNION SELECT cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT cast(1 as int) FROM t UNION SELECT cast('2017-12-11 09:30:00' as date) FROM t;

SELECT cast(1 as bigint) FROM t UNION SELECT cast(2 as tinyint) FROM t;
SELECT cast(1 as bigint) FROM t UNION SELECT cast(2 as smallint) FROM t;
SELECT cast(1 as bigint) FROM t UNION SELECT cast(2 as int) FROM t;
SELECT cast(1 as bigint) FROM t UNION SELECT cast(2 as bigint) FROM t;
SELECT cast(1 as bigint) FROM t UNION SELECT cast(2 as float) FROM t;
SELECT cast(1 as bigint) FROM t UNION SELECT cast(2 as double) FROM t;
SELECT cast(1 as bigint) FROM t UNION SELECT cast(2 as decimal(10, 0)) FROM t;
SELECT cast(1 as bigint) FROM t UNION SELECT cast(2 as string) FROM t;
SELECT cast(1 as bigint) FROM t UNION SELECT cast('2' as binary) FROM t;
SELECT cast(1 as bigint) FROM t UNION SELECT cast(2 as boolean) FROM t;
SELECT cast(1 as bigint) FROM t UNION SELECT cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT cast(1 as bigint) FROM t UNION SELECT cast('2017-12-11 09:30:00' as date) FROM t;

SELECT cast(1 as float) FROM t UNION SELECT cast(2 as tinyint) FROM t;
SELECT cast(1 as float) FROM t UNION SELECT cast(2 as smallint) FROM t;
SELECT cast(1 as float) FROM t UNION SELECT cast(2 as int) FROM t;
SELECT cast(1 as float) FROM t UNION SELECT cast(2 as bigint) FROM t;
SELECT cast(1 as float) FROM t UNION SELECT cast(2 as float) FROM t;
SELECT cast(1 as float) FROM t UNION SELECT cast(2 as double) FROM t;
SELECT cast(1 as float) FROM t UNION SELECT cast(2 as decimal(10, 0)) FROM t;
SELECT cast(1 as float) FROM t UNION SELECT cast(2 as string) FROM t;
SELECT cast(1 as float) FROM t UNION SELECT cast('2' as binary) FROM t;
SELECT cast(1 as float) FROM t UNION SELECT cast(2 as boolean) FROM t;
SELECT cast(1 as float) FROM t UNION SELECT cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT cast(1 as float) FROM t UNION SELECT cast('2017-12-11 09:30:00' as date) FROM t;

SELECT cast(1 as double) FROM t UNION SELECT cast(2 as tinyint) FROM t;
SELECT cast(1 as double) FROM t UNION SELECT cast(2 as smallint) FROM t;
SELECT cast(1 as double) FROM t UNION SELECT cast(2 as int) FROM t;
SELECT cast(1 as double) FROM t UNION SELECT cast(2 as bigint) FROM t;
SELECT cast(1 as double) FROM t UNION SELECT cast(2 as float) FROM t;
SELECT cast(1 as double) FROM t UNION SELECT cast(2 as double) FROM t;
SELECT cast(1 as double) FROM t UNION SELECT cast(2 as decimal(10, 0)) FROM t;
SELECT cast(1 as double) FROM t UNION SELECT cast(2 as string) FROM t;
SELECT cast(1 as double) FROM t UNION SELECT cast('2' as binary) FROM t;
SELECT cast(1 as double) FROM t UNION SELECT cast(2 as boolean) FROM t;
SELECT cast(1 as double) FROM t UNION SELECT cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT cast(1 as double) FROM t UNION SELECT cast('2017-12-11 09:30:00' as date) FROM t;

SELECT cast(1 as decimal(10, 0)) FROM t UNION SELECT cast(2 as tinyint) FROM t;
SELECT cast(1 as decimal(10, 0)) FROM t UNION SELECT cast(2 as smallint) FROM t;
SELECT cast(1 as decimal(10, 0)) FROM t UNION SELECT cast(2 as int) FROM t;
SELECT cast(1 as decimal(10, 0)) FROM t UNION SELECT cast(2 as bigint) FROM t;
SELECT cast(1 as decimal(10, 0)) FROM t UNION SELECT cast(2 as float) FROM t;
SELECT cast(1 as decimal(10, 0)) FROM t UNION SELECT cast(2 as double) FROM t;
SELECT cast(1 as decimal(10, 0)) FROM t UNION SELECT cast(2 as decimal(10, 0)) FROM t;
SELECT cast(1 as decimal(10, 0)) FROM t UNION SELECT cast(2 as string) FROM t;
SELECT cast(1 as decimal(10, 0)) FROM t UNION SELECT cast('2' as binary) FROM t;
SELECT cast(1 as decimal(10, 0)) FROM t UNION SELECT cast(2 as boolean) FROM t;
SELECT cast(1 as decimal(10, 0)) FROM t UNION SELECT cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT cast(1 as decimal(10, 0)) FROM t UNION SELECT cast('2017-12-11 09:30:00' as date) FROM t;

SELECT cast(1 as string) FROM t UNION SELECT cast(2 as tinyint) FROM t;
SELECT cast(1 as string) FROM t UNION SELECT cast(2 as smallint) FROM t;
SELECT cast(1 as string) FROM t UNION SELECT cast(2 as int) FROM t;
SELECT cast(1 as string) FROM t UNION SELECT cast(2 as bigint) FROM t;
SELECT cast(1 as string) FROM t UNION SELECT cast(2 as float) FROM t;
SELECT cast(1 as string) FROM t UNION SELECT cast(2 as double) FROM t;
SELECT cast(1 as string) FROM t UNION SELECT cast(2 as decimal(10, 0)) FROM t;
SELECT cast(1 as string) FROM t UNION SELECT cast(2 as string) FROM t;
SELECT cast(1 as string) FROM t UNION SELECT cast('2' as binary) FROM t;
SELECT cast(1 as string) FROM t UNION SELECT cast(2 as boolean) FROM t;
SELECT cast(1 as string) FROM t UNION SELECT cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT cast(1 as string) FROM t UNION SELECT cast('2017-12-11 09:30:00' as date) FROM t;

SELECT cast('1' as binary) FROM t UNION SELECT cast(2 as tinyint) FROM t;
SELECT cast('1' as binary) FROM t UNION SELECT cast(2 as smallint) FROM t;
SELECT cast('1' as binary) FROM t UNION SELECT cast(2 as int) FROM t;
SELECT cast('1' as binary) FROM t UNION SELECT cast(2 as bigint) FROM t;
SELECT cast('1' as binary) FROM t UNION SELECT cast(2 as float) FROM t;
SELECT cast('1' as binary) FROM t UNION SELECT cast(2 as double) FROM t;
SELECT cast('1' as binary) FROM t UNION SELECT cast(2 as decimal(10, 0)) FROM t;
SELECT cast('1' as binary) FROM t UNION SELECT cast(2 as string) FROM t;
SELECT cast('1' as binary) FROM t UNION SELECT cast('2' as binary) FROM t;
SELECT cast('1' as binary) FROM t UNION SELECT cast(2 as boolean) FROM t;
SELECT cast('1' as binary) FROM t UNION SELECT cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT cast('1' as binary) FROM t UNION SELECT cast('2017-12-11 09:30:00' as date) FROM t;

SELECT cast(1 as boolean) FROM t UNION SELECT cast(2 as tinyint) FROM t;
SELECT cast(1 as boolean) FROM t UNION SELECT cast(2 as smallint) FROM t;
SELECT cast(1 as boolean) FROM t UNION SELECT cast(2 as int) FROM t;
SELECT cast(1 as boolean) FROM t UNION SELECT cast(2 as bigint) FROM t;
SELECT cast(1 as boolean) FROM t UNION SELECT cast(2 as float) FROM t;
SELECT cast(1 as boolean) FROM t UNION SELECT cast(2 as double) FROM t;
SELECT cast(1 as boolean) FROM t UNION SELECT cast(2 as decimal(10, 0)) FROM t;
SELECT cast(1 as boolean) FROM t UNION SELECT cast(2 as string) FROM t;
SELECT cast(1 as boolean) FROM t UNION SELECT cast('2' as binary) FROM t;
SELECT cast(1 as boolean) FROM t UNION SELECT cast(2 as boolean) FROM t;
SELECT cast(1 as boolean) FROM t UNION SELECT cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT cast(1 as boolean) FROM t UNION SELECT cast('2017-12-11 09:30:00' as date) FROM t;

SELECT cast('2017-12-12 09:30:00.0' as timestamp) FROM t UNION SELECT cast(2 as tinyint) FROM t;
SELECT cast('2017-12-12 09:30:00.0' as timestamp) FROM t UNION SELECT cast(2 as smallint) FROM t;
SELECT cast('2017-12-12 09:30:00.0' as timestamp) FROM t UNION SELECT cast(2 as int) FROM t;
SELECT cast('2017-12-12 09:30:00.0' as timestamp) FROM t UNION SELECT cast(2 as bigint) FROM t;
SELECT cast('2017-12-12 09:30:00.0' as timestamp) FROM t UNION SELECT cast(2 as float) FROM t;
SELECT cast('2017-12-12 09:30:00.0' as timestamp) FROM t UNION SELECT cast(2 as double) FROM t;
SELECT cast('2017-12-12 09:30:00.0' as timestamp) FROM t UNION SELECT cast(2 as decimal(10, 0)) FROM t;
SELECT cast('2017-12-12 09:30:00.0' as timestamp) FROM t UNION SELECT cast(2 as string) FROM t;
SELECT cast('2017-12-12 09:30:00.0' as timestamp) FROM t UNION SELECT cast('2' as binary) FROM t;
SELECT cast('2017-12-12 09:30:00.0' as timestamp) FROM t UNION SELECT cast(2 as boolean) FROM t;
SELECT cast('2017-12-12 09:30:00.0' as timestamp) FROM t UNION SELECT cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT cast('2017-12-12 09:30:00.0' as timestamp) FROM t UNION SELECT cast('2017-12-11 09:30:00' as date) FROM t;

SELECT cast('2017-12-12 09:30:00' as date) FROM t UNION SELECT cast(2 as tinyint) FROM t;
SELECT cast('2017-12-12 09:30:00' as date) FROM t UNION SELECT cast(2 as smallint) FROM t;
SELECT cast('2017-12-12 09:30:00' as date) FROM t UNION SELECT cast(2 as int) FROM t;
SELECT cast('2017-12-12 09:30:00' as date) FROM t UNION SELECT cast(2 as bigint) FROM t;
SELECT cast('2017-12-12 09:30:00' as date) FROM t UNION SELECT cast(2 as float) FROM t;
SELECT cast('2017-12-12 09:30:00' as date) FROM t UNION SELECT cast(2 as double) FROM t;
SELECT cast('2017-12-12 09:30:00' as date) FROM t UNION SELECT cast(2 as decimal(10, 0)) FROM t;
SELECT cast('2017-12-12 09:30:00' as date) FROM t UNION SELECT cast(2 as string) FROM t;
SELECT cast('2017-12-12 09:30:00' as date) FROM t UNION SELECT cast('2' as binary) FROM t;
SELECT cast('2017-12-12 09:30:00' as date) FROM t UNION SELECT cast(2 as boolean) FROM t;
SELECT cast('2017-12-12 09:30:00' as date) FROM t UNION SELECT cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT cast('2017-12-12 09:30:00' as date) FROM t UNION SELECT cast('2017-12-11 09:30:00' as date) FROM t;
