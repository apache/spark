---
layout: global
title: INSERT OVERWRITE DIRECTORY with Hive format
displayTitle: INSERT OVERWRITE DIRECTORY with Hive format
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

Insert the query results into a directory using Hive SerDe. If the specified path exists, it is replaced with the output of the query. This command is supported only when Hive support is enabled.

### Syntax
{% highlight sql %}
INSERT OVERWRITE [LOCAL] DIRECTORY directory_path
  [ROW FORMAT row_format] [STORED AS file_format]
  [AS] query
{% endhighlight %}

### Examples
{% highlight sql %}
 INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination/path'
   STORED AS orc
   SELECT * FROM source_table where key < 10

 INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination/path'
   ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
   SELECT * FROM source_table
{% endhighlight %}

### Parameters

#### ***directory_path***:
The destination directory. If LOCAL is used, the directory is on the local file system.

#### ***row_format***:
Use the `SERDE` clause to specify a custom SerDe for this insert. Otherwise, use the `DELIMITED` clause to use the native SerDe and specify the delimiter, escape character, null character, and so on.

#### ***file_format***:
The file format for this insert. One of TEXTFILE, SEQUENCEFILE, RCFILE, ORC, PARQUET, and AVRO. Alternatively, you can specify your own input and output format through INPUTFORMAT and OUTPUTFORMAT. Only TEXTFILE, SEQUENCEFILE, and RCFILE can be used with ROW FORMAT SERDE, and only TEXTFILE can be used with ROW FORMAT DELIMITED.

#### ***query***:
A query (SELECT statement) that provides the rows to be inserted.
