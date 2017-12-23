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

-- Binary arithmetic
SELECT '1' + cast(1 as tinyint)                         FROM t;
SELECT '1' + cast(1 as smallint)                        FROM t;
SELECT '1' + cast(1 as int)                             FROM t;
SELECT '1' + cast(1 as bigint)                          FROM t;
SELECT '1' + cast(1 as float)                           FROM t;
SELECT '1' + cast(1 as double)                          FROM t;
SELECT '1' + cast(1 as decimal(10, 0))                  FROM t;
SELECT '1' + '1'                                        FROM t;
SELECT '1' + cast('1' as binary)                        FROM t;
SELECT '1' + cast(1 as boolean)                         FROM t;
SELECT '1' + cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT '1' + cast('2017-12-11 09:30:00' as date)        FROM t;

SELECT '1' - cast(1 as tinyint)                         FROM t;
SELECT '1' - cast(1 as smallint)                        FROM t;
SELECT '1' - cast(1 as int)                             FROM t;
SELECT '1' - cast(1 as bigint)                          FROM t;
SELECT '1' - cast(1 as float)                           FROM t;
SELECT '1' - cast(1 as double)                          FROM t;
SELECT '1' - cast(1 as decimal(10, 0))                  FROM t;
SELECT '1' - '1'                                        FROM t;
SELECT '1' - cast('1' as binary)                        FROM t;
SELECT '1' - cast(1 as boolean)                         FROM t;
SELECT '1' - cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT '1' - cast('2017-12-11 09:30:00' as date)        FROM t;

SELECT '1' * cast(1 as tinyint)                         FROM t;
SELECT '1' * cast(1 as smallint)                        FROM t;
SELECT '1' * cast(1 as int)                             FROM t;
SELECT '1' * cast(1 as bigint)                          FROM t;
SELECT '1' * cast(1 as float)                           FROM t;
SELECT '1' * cast(1 as double)                          FROM t;
SELECT '1' * cast(1 as decimal(10, 0))                  FROM t;
SELECT '1' * '1'                                        FROM t;
SELECT '1' * cast('1' as binary)                        FROM t;
SELECT '1' * cast(1 as boolean)                         FROM t;
SELECT '1' * cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT '1' * cast('2017-12-11 09:30:00' as date)        FROM t;

SELECT '1' / cast(1 as tinyint)                         FROM t;
SELECT '1' / cast(1 as smallint)                        FROM t;
SELECT '1' / cast(1 as int)                             FROM t;
SELECT '1' / cast(1 as bigint)                          FROM t;
SELECT '1' / cast(1 as float)                           FROM t;
SELECT '1' / cast(1 as double)                          FROM t;
SELECT '1' / cast(1 as decimal(10, 0))                  FROM t;
SELECT '1' / '1'                                        FROM t;
SELECT '1' / cast('1' as binary)                        FROM t;
SELECT '1' / cast(1 as boolean)                         FROM t;
SELECT '1' / cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT '1' / cast('2017-12-11 09:30:00' as date)        FROM t;

SELECT '1' % cast(1 as tinyint)                         FROM t;
SELECT '1' % cast(1 as smallint)                        FROM t;
SELECT '1' % cast(1 as int)                             FROM t;
SELECT '1' % cast(1 as bigint)                          FROM t;
SELECT '1' % cast(1 as float)                           FROM t;
SELECT '1' % cast(1 as double)                          FROM t;
SELECT '1' % cast(1 as decimal(10, 0))                  FROM t;
SELECT '1' % '1'                                        FROM t;
SELECT '1' % cast('1' as binary)                        FROM t;
SELECT '1' % cast(1 as boolean)                         FROM t;
SELECT '1' % cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT '1' % cast('2017-12-11 09:30:00' as date)        FROM t;

SELECT pmod('1', cast(1 as tinyint))                         FROM t;
SELECT pmod('1', cast(1 as smallint))                        FROM t;
SELECT pmod('1', cast(1 as int))                             FROM t;
SELECT pmod('1', cast(1 as bigint))                          FROM t;
SELECT pmod('1', cast(1 as float))                           FROM t;
SELECT pmod('1', cast(1 as double))                          FROM t;
SELECT pmod('1', cast(1 as decimal(10, 0)))                  FROM t;
SELECT pmod('1', '1')                                        FROM t;
SELECT pmod('1', cast('1' as binary))                        FROM t;
SELECT pmod('1', cast(1 as boolean))                         FROM t;
SELECT pmod('1', cast('2017-12-11 09:30:00.0' as timestamp)) FROM t;
SELECT pmod('1', cast('2017-12-11 09:30:00' as date))        FROM t;

SELECT cast(1 as tinyint)                         + '1' FROM t;
SELECT cast(1 as smallint)                        + '1' FROM t;
SELECT cast(1 as int)                             + '1' FROM t;
SELECT cast(1 as bigint)                          + '1' FROM t;
SELECT cast(1 as float)                           + '1' FROM t;
SELECT cast(1 as double)                          + '1' FROM t;
SELECT cast(1 as decimal(10, 0))                  + '1' FROM t;
SELECT cast('1' as binary)                        + '1' FROM t;
SELECT cast(1 as boolean)                         + '1' FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) + '1' FROM t;
SELECT cast('2017-12-11 09:30:00' as date)        + '1' FROM t;

SELECT cast(1 as tinyint)                         - '1' FROM t;
SELECT cast(1 as smallint)                        - '1' FROM t;
SELECT cast(1 as int)                             - '1' FROM t;
SELECT cast(1 as bigint)                          - '1' FROM t;
SELECT cast(1 as float)                           - '1' FROM t;
SELECT cast(1 as double)                          - '1' FROM t;
SELECT cast(1 as decimal(10, 0))                  - '1' FROM t;
SELECT cast('1' as binary)                        - '1' FROM t;
SELECT cast(1 as boolean)                         - '1' FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) - '1' FROM t;
SELECT cast('2017-12-11 09:30:00' as date)        - '1' FROM t;

SELECT cast(1 as tinyint)                         * '1' FROM t;
SELECT cast(1 as smallint)                        * '1' FROM t;
SELECT cast(1 as int)                             * '1' FROM t;
SELECT cast(1 as bigint)                          * '1' FROM t;
SELECT cast(1 as float)                           * '1' FROM t;
SELECT cast(1 as double)                          * '1' FROM t;
SELECT cast(1 as decimal(10, 0))                  * '1' FROM t;
SELECT cast('1' as binary)                        * '1' FROM t;
SELECT cast(1 as boolean)                         * '1' FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) * '1' FROM t;
SELECT cast('2017-12-11 09:30:00' as date)        * '1' FROM t;

SELECT cast(1 as tinyint)                         / '1' FROM t;
SELECT cast(1 as smallint)                        / '1' FROM t;
SELECT cast(1 as int)                             / '1' FROM t;
SELECT cast(1 as bigint)                          / '1' FROM t;
SELECT cast(1 as float)                           / '1' FROM t;
SELECT cast(1 as double)                          / '1' FROM t;
SELECT cast(1 as decimal(10, 0))                  / '1' FROM t;
SELECT cast('1' as binary)                        / '1' FROM t;
SELECT cast(1 as boolean)                         / '1' FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) / '1' FROM t;
SELECT cast('2017-12-11 09:30:00' as date)        / '1' FROM t;

SELECT cast(1 as tinyint)                         % '1' FROM t;
SELECT cast(1 as smallint)                        % '1' FROM t;
SELECT cast(1 as int)                             % '1' FROM t;
SELECT cast(1 as bigint)                          % '1' FROM t;
SELECT cast(1 as float)                           % '1' FROM t;
SELECT cast(1 as double)                          % '1' FROM t;
SELECT cast(1 as decimal(10, 0))                  % '1' FROM t;
SELECT cast('1' as binary)                        % '1' FROM t;
SELECT cast(1 as boolean)                         % '1' FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) % '1' FROM t;
SELECT cast('2017-12-11 09:30:00' as date)        % '1' FROM t;

SELECT pmod(cast(1 as tinyint), '1')                         FROM t;
SELECT pmod(cast(1 as smallint), '1')                        FROM t;
SELECT pmod(cast(1 as int), '1')                             FROM t;
SELECT pmod(cast(1 as bigint), '1')                          FROM t;
SELECT pmod(cast(1 as float), '1')                           FROM t;
SELECT pmod(cast(1 as double), '1')                          FROM t;
SELECT pmod(cast(1 as decimal(10, 0)), '1')                  FROM t;
SELECT pmod(cast('1' as binary), '1')                        FROM t;
SELECT pmod(cast(1 as boolean), '1')                         FROM t;
SELECT pmod(cast('2017-12-11 09:30:00.0' as timestamp), '1') FROM t;
SELECT pmod(cast('2017-12-11 09:30:00' as date), '1')        FROM t;

-- Equality
SELECT '1' = cast(1 as tinyint)                         FROM t;
SELECT '1' = cast(1 as smallint)                        FROM t;
SELECT '1' = cast(1 as int)                             FROM t;
SELECT '1' = cast(1 as bigint)                          FROM t;
SELECT '1' = cast(1 as float)                           FROM t;
SELECT '1' = cast(1 as double)                          FROM t;
SELECT '1' = cast(1 as decimal(10, 0))                  FROM t;
SELECT '1' = '1'                                        FROM t;
SELECT '1' = cast('1' as binary)                        FROM t;
SELECT '1' = cast(1 as boolean)                         FROM t;
SELECT '1' = cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT '1' = cast('2017-12-11 09:30:00' as date)        FROM t;

SELECT cast(1 as tinyint)                         = '1' FROM t;
SELECT cast(1 as smallint)                        = '1' FROM t;
SELECT cast(1 as int)                             = '1' FROM t;
SELECT cast(1 as bigint)                          = '1' FROM t;
SELECT cast(1 as float)                           = '1' FROM t;
SELECT cast(1 as double)                          = '1' FROM t;
SELECT cast(1 as decimal(10, 0))                  = '1' FROM t;
SELECT cast('1' as binary)                        = '1' FROM t;
SELECT cast(1 as boolean)                         = '1' FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) = '1' FROM t;
SELECT cast('2017-12-11 09:30:00' as date)        = '1' FROM t;

SELECT '1' <=> cast(1 as tinyint)                         FROM t;
SELECT '1' <=> cast(1 as smallint)                        FROM t;
SELECT '1' <=> cast(1 as int)                             FROM t;
SELECT '1' <=> cast(1 as bigint)                          FROM t;
SELECT '1' <=> cast(1 as float)                           FROM t;
SELECT '1' <=> cast(1 as double)                          FROM t;
SELECT '1' <=> cast(1 as decimal(10, 0))                  FROM t;
SELECT '1' <=> '1'                                        FROM t;
SELECT '1' <=> cast('1' as binary)                        FROM t;
SELECT '1' <=> cast(1 as boolean)                         FROM t;
SELECT '1' <=> cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT '1' <=> cast('2017-12-11 09:30:00' as date)        FROM t;

SELECT cast(1 as tinyint)                         <=> '1' FROM t;
SELECT cast(1 as smallint)                        <=> '1' FROM t;
SELECT cast(1 as int)                             <=> '1' FROM t;
SELECT cast(1 as bigint)                          <=> '1' FROM t;
SELECT cast(1 as float)                           <=> '1' FROM t;
SELECT cast(1 as double)                          <=> '1' FROM t;
SELECT cast(1 as decimal(10, 0))                  <=> '1' FROM t;
SELECT cast('1' as binary)                        <=> '1' FROM t;
SELECT cast(1 as boolean)                         <=> '1' FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) <=> '1' FROM t;
SELECT cast('2017-12-11 09:30:00' as date)        <=> '1' FROM t;

-- Binary comparison
SELECT '1' < cast(1 as tinyint)                         FROM t;
SELECT '1' < cast(1 as smallint)                        FROM t;
SELECT '1' < cast(1 as int)                             FROM t;
SELECT '1' < cast(1 as bigint)                          FROM t;
SELECT '1' < cast(1 as float)                           FROM t;
SELECT '1' < cast(1 as double)                          FROM t;
SELECT '1' < cast(1 as decimal(10, 0))                  FROM t;
SELECT '1' < '1'                                        FROM t;
SELECT '1' < cast('1' as binary)                        FROM t;
SELECT '1' < cast(1 as boolean)                         FROM t;
SELECT '1' < cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT '1' < cast('2017-12-11 09:30:00' as date)        FROM t;

SELECT '1' <= cast(1 as tinyint)                         FROM t;
SELECT '1' <= cast(1 as smallint)                        FROM t;
SELECT '1' <= cast(1 as int)                             FROM t;
SELECT '1' <= cast(1 as bigint)                          FROM t;
SELECT '1' <= cast(1 as float)                           FROM t;
SELECT '1' <= cast(1 as double)                          FROM t;
SELECT '1' <= cast(1 as decimal(10, 0))                  FROM t;
SELECT '1' <= '1'                                        FROM t;
SELECT '1' <= cast('1' as binary)                        FROM t;
SELECT '1' <= cast(1 as boolean)                         FROM t;
SELECT '1' <= cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT '1' <= cast('2017-12-11 09:30:00' as date)        FROM t;

SELECT '1' > cast(1 as tinyint)                         FROM t;
SELECT '1' > cast(1 as smallint)                        FROM t;
SELECT '1' > cast(1 as int)                             FROM t;
SELECT '1' > cast(1 as bigint)                          FROM t;
SELECT '1' > cast(1 as float)                           FROM t;
SELECT '1' > cast(1 as double)                          FROM t;
SELECT '1' > cast(1 as decimal(10, 0))                  FROM t;
SELECT '1' > '1'                                        FROM t;
SELECT '1' > cast('1' as binary)                        FROM t;
SELECT '1' > cast(1 as boolean)                         FROM t;
SELECT '1' > cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT '1' > cast('2017-12-11 09:30:00' as date)        FROM t;

SELECT '1' >= cast(1 as tinyint)                         FROM t;
SELECT '1' >= cast(1 as smallint)                        FROM t;
SELECT '1' >= cast(1 as int)                             FROM t;
SELECT '1' >= cast(1 as bigint)                          FROM t;
SELECT '1' >= cast(1 as float)                           FROM t;
SELECT '1' >= cast(1 as double)                          FROM t;
SELECT '1' >= cast(1 as decimal(10, 0))                  FROM t;
SELECT '1' >= '1'                                        FROM t;
SELECT '1' >= cast('1' as binary)                        FROM t;
SELECT '1' >= cast(1 as boolean)                         FROM t;
SELECT '1' >= cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT '1' >= cast('2017-12-11 09:30:00' as date)        FROM t;

SELECT '1' <> cast(1 as tinyint)                         FROM t;
SELECT '1' <> cast(1 as smallint)                        FROM t;
SELECT '1' <> cast(1 as int)                             FROM t;
SELECT '1' <> cast(1 as bigint)                          FROM t;
SELECT '1' <> cast(1 as float)                           FROM t;
SELECT '1' <> cast(1 as double)                          FROM t;
SELECT '1' <> cast(1 as decimal(10, 0))                  FROM t;
SELECT '1' <> '1'                                        FROM t;
SELECT '1' <> cast('1' as binary)                        FROM t;
SELECT '1' <> cast(1 as boolean)                         FROM t;
SELECT '1' <> cast('2017-12-11 09:30:00.0' as timestamp) FROM t;
SELECT '1' <> cast('2017-12-11 09:30:00' as date)        FROM t;

SELECT cast(1 as tinyint)                         < '1' FROM t;
SELECT cast(1 as smallint)                        < '1' FROM t;
SELECT cast(1 as int)                             < '1' FROM t;
SELECT cast(1 as bigint)                          < '1' FROM t;
SELECT cast(1 as float)                           < '1' FROM t;
SELECT cast(1 as double)                          < '1' FROM t;
SELECT cast(1 as decimal(10, 0))                  < '1' FROM t;
SELECT '1'                                        < '1' FROM t;
SELECT cast('1' as binary)                        < '1' FROM t;
SELECT cast(1 as boolean)                         < '1' FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) < '1' FROM t;
SELECT cast('2017-12-11 09:30:00' as date)        < '1' FROM t;

SELECT cast(1 as tinyint)                         <= '1' FROM t;
SELECT cast(1 as smallint)                        <= '1' FROM t;
SELECT cast(1 as int)                             <= '1' FROM t;
SELECT cast(1 as bigint)                          <= '1' FROM t;
SELECT cast(1 as float)                           <= '1' FROM t;
SELECT cast(1 as double)                          <= '1' FROM t;
SELECT cast(1 as decimal(10, 0))                  <= '1' FROM t;
SELECT '1'                                        <= '1' FROM t;
SELECT cast('1' as binary)                        <= '1' FROM t;
SELECT cast(1 as boolean)                         <= '1' FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) <= '1' FROM t;
SELECT cast('2017-12-11 09:30:00' as date)        <= '1' FROM t;

SELECT cast(1 as tinyint)                         > '1' FROM t;
SELECT cast(1 as smallint)                        > '1' FROM t;
SELECT cast(1 as int)                             > '1' FROM t;
SELECT cast(1 as bigint)                          > '1' FROM t;
SELECT cast(1 as float)                           > '1' FROM t;
SELECT cast(1 as double)                          > '1' FROM t;
SELECT cast(1 as decimal(10, 0))                  > '1' FROM t;
SELECT '1'                                        > '1' FROM t;
SELECT cast('1' as binary)                        > '1' FROM t;
SELECT cast(1 as boolean)                         > '1' FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) > '1' FROM t;
SELECT cast('2017-12-11 09:30:00' as date)        > '1' FROM t;

SELECT cast(1 as tinyint)                         >= '1' FROM t;
SELECT cast(1 as smallint)                        >= '1' FROM t;
SELECT cast(1 as int)                             >= '1' FROM t;
SELECT cast(1 as bigint)                          >= '1' FROM t;
SELECT cast(1 as float)                           >= '1' FROM t;
SELECT cast(1 as double)                          >= '1' FROM t;
SELECT cast(1 as decimal(10, 0))                  >= '1' FROM t;
SELECT '1'                                        >= '1' FROM t;
SELECT cast('1' as binary)                        >= '1' FROM t;
SELECT cast(1 as boolean)                         >= '1' FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) >= '1' FROM t;
SELECT cast('2017-12-11 09:30:00' as date)        >= '1' FROM t;

SELECT cast(1 as tinyint)                         <> '1' FROM t;
SELECT cast(1 as smallint)                        <> '1' FROM t;
SELECT cast(1 as int)                             <> '1' FROM t;
SELECT cast(1 as bigint)                          <> '1' FROM t;
SELECT cast(1 as float)                           <> '1' FROM t;
SELECT cast(1 as double)                          <> '1' FROM t;
SELECT cast(1 as decimal(10, 0))                  <> '1' FROM t;
SELECT '1'                                        <> '1' FROM t;
SELECT cast('1' as binary)                        <> '1' FROM t;
SELECT cast(1 as boolean)                         <> '1' FROM t;
SELECT cast('2017-12-11 09:30:00.0' as timestamp) <> '1' FROM t;
SELECT cast('2017-12-11 09:30:00' as date)        <> '1' FROM t;

-- Functions
SELECT abs('1') FROM t;
SELECT sum('1') FROM t;
SELECT avg('1') FROM t;
SELECT stddev_pop('1') FROM t;
SELECT stddev_samp('1') FROM t;
SELECT - '1' FROM t;
SELECT + '1' FROM t;
SELECT var_pop('1') FROM t;
SELECT var_samp('1') FROM t;
SELECT skewness('1') FROM t;
SELECT kurtosis('1') FROM t;
