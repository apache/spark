---
layout: global
title: SET
displayTitle: SET
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

The SET command sets a property, returns the value of an existing property or returns all SQLConf properties with value and meaning.

### Syntax

```sql
SET
SET [ -v ]
SET property_key[ = property_value ]
```

### Parameters

* **-v**

    Outputs the key, value and meaning of existing SQLConf properties.

* **property_key**

    Returns the value of specified property key.

* **property_key=property_value**

     Sets the value for a given property key. If an old value exists for a given property key, then it gets overridden by the new value.

### Examples

```sql
-- Set a property.
SET spark.sql.variable.substitute=false;

-- List all SQLConf properties with value and meaning.
SET -v;

-- List all SQLConf properties with value for current session.
SET;

-- List the value of specified property key.
SET spark.sql.variable.substitute;
+-----------------------------+-----+
|                          key|value|
+-----------------------------+-----+
|spark.sql.variable.substitute|false|
+-----------------------------+-----+
```

### Related Statements

* [RESET](sql-ref-syntax-aux-conf-mgmt-reset.html)
