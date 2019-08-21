---
layout: global
title: INSERT OVERWRITE DIRECTORY
displayTitle: INSERT OVERWRITE DIRECTORY
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

Insert the query results into a directory using Spark native format. If the specified path exists, it is replaced with the query.

### Syntax

{% highlight sql %}
INSERT OVERWRITE [LOCAL] DIRECTORY [directory_path]
  USING file_format [OPTIONS (key1=val1, key2=val2, ...)] [AS] query
{% endhighlight %}

### Examples
{% highlight sql %}
INSERT OVERWRITE DIRECTORY '/tmp/destination/path'
  USING parquet
  OPTIONS (a 1, b 0.1, c TRUE)
  SELECT * FROM source_table

INSERT OVERWRITE LOCAL DIRECTORY
  USING parquet
  OPTIONS ('path' '/tmp/destination/path', a 1, b 0.1, c TRUE)
  SELECT * FROM source_table
{% endhighlight %}

### Parameters

#### ***directory_path***:
The destination directory. It can also be specified in OPTIONS using ```path```.  If LOCAL is used, the directory is on the local file system.

#### ***file_format***:
The file format to use for the insert. It could be TEXT, CSV, JSON, JDBC, PARQUET, ORC, HIVE, DELTA, and LIBSVM, or a fully qualified class name of a custom implementation of org.apache.spark.sql.sources.DataSourceRegister.

#### ***query***:
A query (SELECT statement) that provides the rows to be inserted.
