---
layout: global
title: LIST ARCHIVE
displayTitle: LIST ARCHIVE
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

`LIST ARCHIVE` lists the archives added by [ADD ARCHIVE](sql-ref-syntax-aux-resource-mgmt-add-archive.html).

### Syntax

```sql
LIST { ARCHIVE | ARCHIVES } file_name [ ... ]
```

### Examples

```sql
ADD ARCHIVE /tmp/test.zip;
ADD ARCHIVE /tmp/test_2.tar.gz;
LIST ARCHIVE;
-- output for LIST ARCHIVE
file:/tmp/test.zip
file:/tmp/test_2.tar.gz

LIST ARCHIVE /tmp/test.zip /some/random.tgz /another/random.tar;
-- output
file:/tmp/test.zip
```

### Related Statements

* [ADD JAR](sql-ref-syntax-aux-resource-mgmt-add-jar.html)
* [ADD FILE](sql-ref-syntax-aux-resource-mgmt-add-file.html)
* [ADD ARCHIVE](sql-ref-syntax-aux-resource-mgmt-add-archive.html)
* [LIST FILE](sql-ref-syntax-aux-resource-mgmt-list-file.html)
* [LIST JAR](sql-ref-syntax-aux-resource-mgmt-list-jar.html)
