---
layout: global
title: ADD ARCHIVE
displayTitle: ADD ARCHIVE
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

`ADD ARCHIVE` can be used to add an archive file to the list of resources. The given archive file should be one of .zip, .tar, .tar.gz, .tgz and .jar. The added archive file can be listed using [LIST ARCHIVE](sql-ref-syntax-aux-resource-mgmt-list-archive.html).

### Syntax

```sql
ADD { ARCHIVE | ARCHIVES } file_name [ ... ]
```

### Parameters

* **file_name**

    The name of the archive file to be added. It could be either on a local file system or a distributed file system.

### Examples

```sql
ADD ARCHIVE /tmp/test.tar.gz;
ADD ARCHIVE "/path/to/some.zip";
ADD ARCHIVE '/some/other.tgz';
ADD ARCHIVE "/path with space/abc.tar";
ADD ARCHIVES "/path with space/def.tgz" '/path with space/ghi.zip';
```

### Related Statements

* [LIST FILE](sql-ref-syntax-aux-resource-mgmt-list-file.html)
* [LIST JAR](sql-ref-syntax-aux-resource-mgmt-list-jar.html)
* [LIST ARCHIVE](sql-ref-syntax-aux-resource-mgmt-list-archive.html)
* [ADD FILE](sql-ref-syntax-aux-resource-mgmt-add-file.html)
* [ADD JAR](sql-ref-syntax-aux-resource-mgmt-add-jar.html)
