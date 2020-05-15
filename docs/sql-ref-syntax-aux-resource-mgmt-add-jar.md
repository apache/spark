---
layout: global
title: ADD JAR
displayTitle: ADD JAR
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

`ADD JAR` adds a JAR file to the list of resources. The added JAR file can be listed using [LIST JAR](sql-ref-syntax-aux-resource-mgmt-list-jar.html).

### Syntax

```sql
ADD JAR file_name
```

### Parameters

* **file_name**

    The name of the JAR file to be added. It could be either on a local file system or a distributed file system.

### Examples

```sql
ADD JAR /tmp/test.jar;
ADD JAR "/path/to/some.jar";
ADD JAR '/some/other.jar';
ADD JAR "/path with space/abc.jar";
```

### Related Statements

* [LIST JAR](sql-ref-syntax-aux-resource-mgmt-list-jar.html)
* [ADD FILE](sql-ref-syntax-aux-resource-mgmt-add-file.html)
* [LIST FILE](sql-ref-syntax-aux-resource-mgmt-list-file.html)
