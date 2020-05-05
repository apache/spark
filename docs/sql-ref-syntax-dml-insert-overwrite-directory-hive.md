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

The `INSERT OVERWRITE DIRECTORY` with Hive format overwrites the existing data in the directory with the new values using Hive `SerDe`.
Hive support must be enabled to use this command. The inserted rows can be specified by value expressions or result from a query.

### Syntax

{% highlight sql %}
INSERT OVERWRITE [ LOCAL ] DIRECTORY directory_path
    [ ROW FORMAT row_format ] [ STORED AS file_format ]
    { VALUES ( { value | NULL } [ , ... ] ) [ , ( ... ) ] | query }
{% endhighlight %}

### Parameters

<dl>
  <dt><code><em>directory_path</em></code></dt>
  <dd>
  Specifies the destination directory. The <code>LOCAL</code> keyword is used to specify that the directory is on the local file system.
  </dd>
</dl>

<dl>
  <dt><code><em>row_format</em></code></dt>
  <dd>
  Specifies the row format for this insert. Valid options are <code>SERDE</code> clause and <code>DELIMITED</code> clause. <code>SERDE</code> clause can be used to specify a custom <code>SerDe</code> for this insert. Alternatively, <code>DELIMITED</code> clause can be used to specify the native <code>SerDe</code> and state the delimiter, escape character, null character, and so on.
  </dd>
</dl>

<dl>
  <dt><code><em>file_format</em></code></dt>
  <dd>
  Specifies the file format for this insert. Valid options are <code>TEXTFILE</code>, <code>SEQUENCEFILE</code>, <code>RCFILE</code>, <code>ORC</code>, <code>PARQUET</code>, and <code>AVRO</code>. You can also specify your own input and output format using <code>INPUTFORMAT</code> and <code>OUTPUTFORMAT</code>. <code>ROW FORMAT SERDE</code> can only be used with <code>TEXTFILE</code>, <code>SEQUENCEFILE</code>, or <code>RCFILE</code>, while <code>ROW FORMAT DELIMITED</code> can only be used with <code>TEXTFILE</code>.
  </dd>
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
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination'
    STORED AS orc
    SELECT * FROM test_table;

INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    SELECT * FROM test_table;
{% endhighlight %}

### Related Statements

 * [INSERT INTO statement](sql-ref-syntax-dml-insert-into.html)
 * [INSERT OVERWRITE statement](sql-ref-syntax-dml-insert-overwrite-table.html)
 * [INSERT OVERWRITE DIRECTORY statement](sql-ref-syntax-dml-insert-overwrite-directory.html)
