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

SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as tinyint)) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as smallint)) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as int)) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as bigint)) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as float)) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as double)) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as decimal(10, 0))) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as string)) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast('1' as binary)) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as boolean)) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast('2017-12-11 09:30:00.0' as timestamp)) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast('2017-12-11 09:30:00' as date)) FROM t;

SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as tinyint) DESC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as smallint) DESC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as int) DESC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as bigint) DESC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as float) DESC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as double) DESC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as decimal(10, 0)) DESC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as string) DESC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast('1' as binary) DESC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast(1 as boolean) DESC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast('2017-12-11 09:30:00.0' as timestamp) DESC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t;
SELECT COUNT(*) OVER (PARTITION BY 1 ORDER BY cast('2017-12-11 09:30:00' as date) DESC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t;
