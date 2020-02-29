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
The `INSERT OVERWRITE DIRECTORY` statement overwrites the existing data in the directory with the new values using Spark native format. The inserted rows can be specified by value expressions or result from a query.

### Syntax
{% highlight sql %}
INSERT OVERWRITE [ LOCAL ] DIRECTORY [ directory_path ]
    USING file_format [ OPTIONS ( key = val [ , ... ] ) ]
    { { VALUES ( { value | NULL } [ , ... ] ) [ , ( ... ) ] } | query }
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>directory_path</em></code></dt>
  <dd>
  Specifies the destination directory. It can also be specified in <code>OPTIONS</code> using <code>path</code>. The <code>LOCAL</code> keyword is used to specify that the directory is on the local file system.
  </dd>
</dl>

<dl>
  <dt><code><em>file_format</em></code></dt>
  <dd>
  Specifies the file format to use for the insert. Valid options are <code>TEXT</code>, <code>CSV</code>, <code>JSON</code>, <code>JDBC</code>, <code>PARQUET</code>, <code>ORC</code>, <code>HIVE</code>, <code>DELTA</code>, <code>LIBSVM</code>, or a fully qualified class name of a custom implementation of <code>org.apache.spark.sql.sources.DataSourceRegister</code>.
  </dd>
</dl>

<dl>
  <dt><code><em>OPTIONS ( key = val [ , ... ] )</em></code></dt>
  <dd>Specifies one or more table property key and value pairs.</dd>
</dl>

<dl>
  <dt><code><em>VALUES ( { value | NULL } [ , ... ] ) [ , ( ... ) ]</em></code></dt>
  <dd>
  Specifies the values to be inserted. Either an explicitly specified value or a NULL can be inserted. A comma must be used to separate each value in the clause. More than one set of values can be specified to insert multiple rows.
  </dd>
</dl>

<dl>
  <dt><code><em>query</em></code></dt>
  <dd>A query that produces the rows to be inserted. It can be in one of following formats:
    <ul>
      <li>a <code>SELECT</code> statement</li>
      <li>a <code>TABLE</code> statement</li>
      <li>a <code>FROM</code> statement</li>
    </ul>
   </dd>
</dl>

### Examples
{% highlight sql %}
INSERT OVERWRITE DIRECTORY '/tmp/destination'
    USING parquet
    OPTIONS (col1 1, col2 2, col3 'test')
    SELECT * FROM test_table;

INSERT OVERWRITE DIRECTORY
    USING parquet
    OPTIONS ('path' '/tmp/destination', col1 1, col2 2, col3 'test')
    SELECT * FROM test_table;
{% endhighlight %}

### Related Statements
  * [INSERT INTO statement](sql-ref-syntax-dml-insert-into.html)
  * [INSERT OVERWRITE statement](sql-ref-syntax-dml-insert-overwrite-table.html)
  * [INSERT OVERWRITE DIRECTORY with Hive format statement](sql-ref-syntax-dml-insert-overwrite-directory-hive.html)
