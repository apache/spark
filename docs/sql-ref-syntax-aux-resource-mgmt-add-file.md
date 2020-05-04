---
layout: global
title: ADD FILE
displayTitle: ADD FILE
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

`ADD FILE` can be used to add a single file as well as a directory to the list of resources. The added resource can be listed using [LIST FILE](sql-ref-syntax-aux-resource-mgmt-list-file.html).

### Syntax

```sql
ADD FILE resource_name
```

### Parameters

* **resource_name**

    The name of the file or directory to be added.

### Examples

```sql
ADD FILE /tmp/test;
ADD FILE "/path/to/file/abc.txt";
ADD FILE '/another/test.txt';
ADD FILE "/path with space/abc.txt";
ADD FILE "/path/to/some/directory";
```

### Related Statements

* [LIST FILE](sql-ref-syntax-aux-resource-mgmt-list-file.html)
* [LIST JAR](sql-ref-syntax-aux-resource-mgmt-list-jar.html)
* [ADD JAR](sql-ref-syntax-aux-resource-mgmt-add-jar.html)

