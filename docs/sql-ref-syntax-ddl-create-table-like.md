---
layout: global
title: CREATE TABLE LIKE
displayTitle: CREATE TABLE LIKE
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

The `CREATE TABLE` statement defines a new table using the definition/metadata of an existing table or view.

### Syntax
{% highlight sql %}
CREATE TABLE [IF NOT EXISTS] table_identifier LIKE source_table_identifier [LOCATION path]
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>table_identifier</em></code></dt>
  <dd>
    Specifies a table name, which may be optionally qualified with a database name.<br><br>
    <b>Syntax:</b>
      <code>
        [ database_name. ] table_name
      </code>
  </dd>
</dl>
<dl>
  <dt><code><em>LOCATION</em></code></dt>
  <dd>Path to the directory where table data is stored, could be filesystem, HDFS, etc. Location to create an external table.</dd>
</dl>


### Examples
{% highlight sql %}

--Create table using an exsisting table
CREATE TABLE Student_Dupli like Student;

--Table is created as external table at the location specified
CREATE TABLE Student_Dupli like Student location  '/root1/home';

{% endhighlight %}

### Related Statements
* [CREATE TABLE USING DATASOURCE](sql-ref-syntax-ddl-create-table-datasource.html)
* [CREATE TABLE USING HIVE FORMAT](sql-ref-syntax-ddl-create-table-hiveformat.html)

