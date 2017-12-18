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

SELECT stack(cast(1 as tinyint), cast(1 as tinyint), null) FROM t;
SELECT stack(cast(1 as tinyint), cast(1 as smallint), null) FROM t;
SELECT stack(cast(1 as tinyint), cast(1 as int), null) FROM t;
SELECT stack(cast(1 as tinyint), cast(1 as bigint), null) FROM t;
SELECT stack(cast(1 as tinyint), cast(1 as float), null) FROM t;
SELECT stack(cast(1 as tinyint), cast(1 as double), null) FROM t;
SELECT stack(cast(1 as tinyint), cast(1 as decimal(10, 0)), null) FROM t;
SELECT stack(cast(1 as tinyint), cast(1 as string), null) FROM t;
SELECT stack(cast(1 as tinyint), cast('1' as binary), null) FROM t;
SELECT stack(cast(1 as tinyint), cast(1 as boolean), null) FROM t;
SELECT stack(cast(1 as tinyint), cast('2017-12-11 09:30:00.0' as timestamp), null) FROM t;
SELECT stack(cast(1 as tinyint), cast('2017-12-11 09:30:00' as date), null) FROM t;

SELECT stack(cast(1 as smallint), cast(1 as tinyint), null) FROM t;
SELECT stack(cast(1 as smallint), cast(1 as smallint), null) FROM t;
SELECT stack(cast(1 as smallint), cast(1 as int), null) FROM t;
SELECT stack(cast(1 as smallint), cast(1 as bigint), null) FROM t;
SELECT stack(cast(1 as smallint), cast(1 as float), null) FROM t;
SELECT stack(cast(1 as smallint), cast(1 as double), null) FROM t;
SELECT stack(cast(1 as smallint), cast(1 as decimal(10, 0)), null) FROM t;
SELECT stack(cast(1 as smallint), cast(1 as string), null) FROM t;
SELECT stack(cast(1 as smallint), cast('1' as binary), null) FROM t;
SELECT stack(cast(1 as smallint), cast(1 as boolean), null) FROM t;
SELECT stack(cast(1 as smallint), cast('2017-12-11 09:30:00.0' as timestamp), null) FROM t;
SELECT stack(cast(1 as smallint), cast('2017-12-11 09:30:00' as date), null) FROM t;

SELECT stack(cast(1 as int), cast(1 as tinyint), null) FROM t;
SELECT stack(cast(1 as int), cast(1 as smallint), null) FROM t;
SELECT stack(cast(1 as int), cast(1 as int), null) FROM t;
SELECT stack(cast(1 as int), cast(1 as bigint), null) FROM t;
SELECT stack(cast(1 as int), cast(1 as float), null) FROM t;
SELECT stack(cast(1 as int), cast(1 as double), null) FROM t;
SELECT stack(cast(1 as int), cast(1 as decimal(10, 0)), null) FROM t;
SELECT stack(cast(1 as int), cast(1 as string), null) FROM t;
SELECT stack(cast(1 as int), cast('1' as binary), null) FROM t;
SELECT stack(cast(1 as int), cast(1 as boolean), null) FROM t;
SELECT stack(cast(1 as int), cast('2017-12-11 09:30:00.0' as timestamp), null) FROM t;
SELECT stack(cast(1 as int), cast('2017-12-11 09:30:00' as date), null) FROM t;

SELECT stack(cast(1 as bigint), cast(1 as tinyint), null) FROM t;
SELECT stack(cast(1 as bigint), cast(1 as smallint), null) FROM t;
SELECT stack(cast(1 as bigint), cast(1 as int), null) FROM t;
SELECT stack(cast(1 as bigint), cast(1 as bigint), null) FROM t;
SELECT stack(cast(1 as bigint), cast(1 as float), null) FROM t;
SELECT stack(cast(1 as bigint), cast(1 as double), null) FROM t;
SELECT stack(cast(1 as bigint), cast(1 as decimal(10, 0)), null) FROM t;
SELECT stack(cast(1 as bigint), cast(1 as string), null) FROM t;
SELECT stack(cast(1 as bigint), cast('1' as binary), null) FROM t;
SELECT stack(cast(1 as bigint), cast(1 as boolean), null) FROM t;
SELECT stack(cast(1 as bigint), cast('2017-12-11 09:30:00.0' as timestamp), null) FROM t;
SELECT stack(cast(1 as bigint), cast('2017-12-11 09:30:00' as date), null) FROM t;

SELECT stack(cast(1 as float), cast(1 as tinyint), null) FROM t;
SELECT stack(cast(1 as float), cast(1 as smallint), null) FROM t;
SELECT stack(cast(1 as float), cast(1 as int), null) FROM t;
SELECT stack(cast(1 as float), cast(1 as bigint), null) FROM t;
SELECT stack(cast(1 as float), cast(1 as float), null) FROM t;
SELECT stack(cast(1 as float), cast(1 as double), null) FROM t;
SELECT stack(cast(1 as float), cast(1 as decimal(10, 0)), null) FROM t;
SELECT stack(cast(1 as float), cast(1 as string), null) FROM t;
SELECT stack(cast(1 as float), cast('1' as binary), null) FROM t;
SELECT stack(cast(1 as float), cast(1 as boolean), null) FROM t;
SELECT stack(cast(1 as float), cast('2017-12-11 09:30:00.0' as timestamp), null) FROM t;
SELECT stack(cast(1 as float), cast('2017-12-11 09:30:00' as date), null) FROM t;

SELECT stack(cast(1 as double), cast(1 as tinyint), null) FROM t;
SELECT stack(cast(1 as double), cast(1 as smallint), null) FROM t;
SELECT stack(cast(1 as double), cast(1 as int), null) FROM t;
SELECT stack(cast(1 as double), cast(1 as bigint), null) FROM t;
SELECT stack(cast(1 as double), cast(1 as float), null) FROM t;
SELECT stack(cast(1 as double), cast(1 as double), null) FROM t;
SELECT stack(cast(1 as double), cast(1 as decimal(10, 0)), null) FROM t;
SELECT stack(cast(1 as double), cast(1 as string), null) FROM t;
SELECT stack(cast(1 as double), cast('1' as binary), null) FROM t;
SELECT stack(cast(1 as double), cast(1 as boolean), null) FROM t;
SELECT stack(cast(1 as double), cast('2017-12-11 09:30:00.0' as timestamp), null) FROM t;
SELECT stack(cast(1 as double), cast('2017-12-11 09:30:00' as date), null) FROM t;

SELECT stack(cast(1 as decimal(10, 0)), cast(1 as tinyint), null) FROM t;
SELECT stack(cast(1 as decimal(10, 0)), cast(1 as smallint), null) FROM t;
SELECT stack(cast(1 as decimal(10, 0)), cast(1 as int), null) FROM t;
SELECT stack(cast(1 as decimal(10, 0)), cast(1 as bigint), null) FROM t;
SELECT stack(cast(1 as decimal(10, 0)), cast(1 as float), null) FROM t;
SELECT stack(cast(1 as decimal(10, 0)), cast(1 as double), null) FROM t;
SELECT stack(cast(1 as decimal(10, 0)), cast(1 as decimal(10, 0)), null) FROM t;
SELECT stack(cast(1 as decimal(10, 0)), cast(1 as string), null) FROM t;
SELECT stack(cast(1 as decimal(10, 0)), cast('1' as binary), null) FROM t;
SELECT stack(cast(1 as decimal(10, 0)), cast(1 as boolean), null) FROM t;
SELECT stack(cast(1 as decimal(10, 0)), cast('2017-12-11 09:30:00.0' as timestamp), null) FROM t;
SELECT stack(cast(1 as decimal(10, 0)), cast('2017-12-11 09:30:00' as date), null) FROM t;

SELECT stack(cast(1 as string), cast(1 as tinyint), null) FROM t;
SELECT stack(cast(1 as string), cast(1 as smallint), null) FROM t;
SELECT stack(cast(1 as string), cast(1 as int), null) FROM t;
SELECT stack(cast(1 as string), cast(1 as bigint), null) FROM t;
SELECT stack(cast(1 as string), cast(1 as float), null) FROM t;
SELECT stack(cast(1 as string), cast(1 as double), null) FROM t;
SELECT stack(cast(1 as string), cast(1 as decimal(10, 0)), null) FROM t;
SELECT stack(cast(1 as string), cast(1 as string), null) FROM t;
SELECT stack(cast(1 as string), cast('1' as binary), null) FROM t;
SELECT stack(cast(1 as string), cast(1 as boolean), null) FROM t;
SELECT stack(cast(1 as string), cast('2017-12-11 09:30:00.0' as timestamp), null) FROM t;
SELECT stack(cast(1 as string), cast('2017-12-11 09:30:00' as date), null) FROM t;

SELECT stack(cast('1' as binary), cast(1 as tinyint), null) FROM t;
SELECT stack(cast('1' as binary), cast(1 as smallint), null) FROM t;
SELECT stack(cast('1' as binary), cast(1 as int), null) FROM t;
SELECT stack(cast('1' as binary), cast(1 as bigint), null) FROM t;
SELECT stack(cast('1' as binary), cast(1 as float), null) FROM t;
SELECT stack(cast('1' as binary), cast(1 as double), null) FROM t;
SELECT stack(cast('1' as binary), cast(1 as decimal(10, 0)), null) FROM t;
SELECT stack(cast('1' as binary), cast(1 as string), null) FROM t;
SELECT stack(cast('1' as binary), cast('1' as binary), null) FROM t;
SELECT stack(cast('1' as binary), cast(1 as boolean), null) FROM t;
SELECT stack(cast('1' as binary), cast('2017-12-11 09:30:00.0' as timestamp), null) FROM t;
SELECT stack(cast('1' as binary), cast('2017-12-11 09:30:00' as date), null) FROM t;

SELECT stack(cast(1 as boolean), cast(1 as tinyint), null) FROM t;
SELECT stack(cast(1 as boolean), cast(1 as smallint), null) FROM t;
SELECT stack(cast(1 as boolean), cast(1 as int), null) FROM t;
SELECT stack(cast(1 as boolean), cast(1 as bigint), null) FROM t;
SELECT stack(cast(1 as boolean), cast(1 as float), null) FROM t;
SELECT stack(cast(1 as boolean), cast(1 as double), null) FROM t;
SELECT stack(cast(1 as boolean), cast(1 as decimal(10, 0)), null) FROM t;
SELECT stack(cast(1 as boolean), cast(1 as string), null) FROM t;
SELECT stack(cast(1 as boolean), cast('1' as binary), null) FROM t;
SELECT stack(cast(1 as boolean), cast(1 as boolean), null) FROM t;
SELECT stack(cast(1 as boolean), cast('2017-12-11 09:30:00.0' as timestamp), null) FROM t;
SELECT stack(cast(1 as boolean), cast('2017-12-11 09:30:00' as date), null) FROM t;

SELECT stack(cast('2017-12-11 09:30:00.0' as timestamp), cast(1 as tinyint), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00.0' as timestamp), cast(1 as smallint), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00.0' as timestamp), cast(1 as int), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00.0' as timestamp), cast(1 as bigint), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00.0' as timestamp), cast(1 as float), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00.0' as timestamp), cast(1 as double), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00.0' as timestamp), cast(1 as decimal(10, 0)), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00.0' as timestamp), cast(1 as string), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00.0' as timestamp), cast('1' as binary), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00.0' as timestamp), cast(1 as boolean), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00.0' as timestamp), cast('2017-12-11 09:30:00.0' as timestamp), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00.0' as timestamp), cast('2017-12-11 09:30:00' as date), null) FROM t;

SELECT stack(cast('2017-12-11 09:30:00' as date), cast(1 as tinyint), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00' as date), cast(1 as smallint), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00' as date), cast(1 as int), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00' as date), cast(1 as bigint), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00' as date), cast(1 as float), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00' as date), cast(1 as double), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00' as date), cast(1 as decimal(10, 0)), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00' as date), cast(1 as string), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00' as date), cast('1' as binary), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00' as date), cast(1 as boolean), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00' as date), cast('2017-12-11 09:30:00.0' as timestamp), null) FROM t;
SELECT stack(cast('2017-12-11 09:30:00' as date), cast('2017-12-11 09:30:00' as date), null) FROM t;
