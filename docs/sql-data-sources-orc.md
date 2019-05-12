---
layout: global
title: ORC Files
displayTitle: ORC Files
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

Since Spark 2.3, Spark supports a vectorized ORC reader with a new ORC file format for ORC files.
To do that, the following configurations are newly added. The vectorized reader is used for the
native ORC tables (e.g., the ones created using the clause `USING ORC`) when `spark.sql.orc.impl`
is set to `native` and `spark.sql.orc.enableVectorizedReader` is set to `true`. For the Hive ORC
serde tables (e.g., the ones created using the clause `USING HIVE OPTIONS (fileFormat 'ORC')`),
the vectorized reader is used when `spark.sql.hive.convertMetastoreOrc` is also set to `true`.

<table class="table">
  <tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th></tr>
  <tr>
    <td><code>spark.sql.orc.impl</code></td>
    <td><code>native</code></td>
    <td>The name of ORC implementation. It can be one of <code>native</code> and <code>hive</code>. <code>native</code> means the native ORC support that is built on Apache ORC 1.4. `hive` means the ORC library in Hive 1.2.1.</td>
  </tr>
  <tr>
    <td><code>spark.sql.orc.enableVectorizedReader</code></td>
    <td><code>true</code></td>
    <td>Enables vectorized orc decoding in <code>native</code> implementation. If <code>false</code>, a new non-vectorized ORC reader is used in <code>native</code> implementation. For <code>hive</code> implementation, this is ignored.</td>
  </tr>
</table>
