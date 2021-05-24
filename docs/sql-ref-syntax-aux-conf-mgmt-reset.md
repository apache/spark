---
layout: global
title: RESET
displayTitle: RESET
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

### Description

The RESET command resets runtime configurations specific to the current session which were set via the [SET](sql-ref-syntax-aux-conf-mgmt-set.html) command to their default values.

### Syntax

```sql
RESET;

RESET configuration_key;
```

### Parameters

* **(none)**

    Reset any runtime configurations specific to the current session which were set via the [SET](sql-ref-syntax-aux-conf-mgmt-set.html) command to their default values.

* **configuration_key**

    Restore the value of the `configuration_key` to the default value. If the default value is undefined, the `configuration_key` will be removed.

### Examples

```sql
-- Reset any runtime configurations specific to the current session which were set via the SET command to their default values.
RESET;

-- If you start your application with --conf spark.foo=bar and set spark.foo=foobar in runtime, the example below will restore it to 'bar'. If spark.foo is not specified during starting, the example bellow will remove this config from the SQLConf. It will ignore nonexistent keys.
RESET spark.abc;
```

### Related Statements

* [SET](sql-ref-syntax-aux-conf-mgmt-set.html)
