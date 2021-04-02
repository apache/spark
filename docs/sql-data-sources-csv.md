---
layout: global
title: CSV Files
displayTitle: CSV Files
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

Spark SQL provides `spark.read().csv("file_name")` to read a file or directory of files in CSV format into Spark DataFrame, and `dataframe.write().csv("path")` to write to a CSV file. Function `option()` can be used to customize the behavior of reading or writing, such as controlling behavior of the header, delimiter character, character set, and so on. 

<!--TODO: add `option()` document reference--> 

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% include_example csv_dataset scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example csv_dataset java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="python"  markdown="1">
{% include_example csv_dataset python/sql/datasource.py %}
</div>

</div>
