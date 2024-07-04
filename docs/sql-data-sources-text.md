---
layout: global
title: Text Files
displayTitle: Text Files
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

Spark SQL provides `spark.read().text("file_name")` to read a file or directory of text files into a Spark DataFrame, and `dataframe.write().text("path")` to write to a text file. When reading a text file, each line becomes each row that has string "value" column by default. The line separator can be changed as shown in the example below. The `option()` function can be used to customize the behavior of reading or writing, such as controlling behavior of the line separator, compression, and so on.

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% include_example text_dataset python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example text_dataset scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example text_dataset java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

</div>

## Data Source Option

Data source options of text can be set via:
* the `.option`/`.options` methods of
  *  `DataFrameReader`
  *  `DataFrameWriter`
  *  `DataStreamReader`
  *  `DataStreamWriter`
*  `OPTIONS` clause at [CREATE TABLE USING DATA_SOURCE](sql-ref-syntax-ddl-create-table-datasource.html)

<table>
  <thead><tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th><th><b>Scope</b></th></tr></thead>
  <tr>
    <td><code>wholetext</code></td>
    <td><code>false</code></td>
    <td>If true, read each file from input path(s) as a single row.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>lineSep</code></td>
    <td><code>\r</code>, <code>\r\n</code>, <code>\n</code> (for reading), <code>\n</code> (for writing)</td>
    <td>Defines the line separator that should be used for reading or writing.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>compression</code></td>
    <td>(none)</td>
    <td>Compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).</td>
    <td>write</td>
  </tr>
</table>
Other generic options can be found in <a href="https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html"> Generic File Source Options</a>.
