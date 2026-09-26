-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

--SET spark.sql.ansi.enabled = true

-- Valid ordinary and try functions remain usable in ANSI mode.
select inet_aton('127.0.0.1');
select inet_ntoa(2130706433L);
select try_inet_aton('invalid');
select try_inet_ntoa(-1L);

-- Spark analyzer coercion is applied before IPv4 evaluation.
select inet_aton(1.5D);
select inet_ntoa(1.5D);
select inet_ntoa('abc');

-- Non-ASCII input is invalid IPv4 text but remains NULL in the try variant.
select try_inet_aton(concat('127.0.0.1', chr(233)));
