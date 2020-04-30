---
layout: global
title: Table-valued Functions (TVF)
displayTitle: Table-valued Functions (TVF)
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

A table-valued function (TVF) is a function that returns a relation or a set of rows.

### Syntax

{% highlight sql %}
function_name ( expression [ , ... ] ) [ table_alias ]
{% endhighlight %}

### Parameters

<dl>
  <dt><code><em>expression</em></code></dt>
  <dd>
    Specifies a combination of one or more values, operators and SQL functions that results in a value.
  </dd>
</dl>
<dl>
  <dt><code><em>table_alias</em></code></dt>
  <dd>
    Specifies a temporary name with an optional column name list. <br><br>
    <b>Syntax:</b>
      <code>
        [ AS ] table_name [ ( column_name [ , ... ] ) ]
      </code>
  </dd>
</dl>

### Supported Table-valued Functions

<table class="table">
  <thead>
    <tr><th style="width:25%">Function</th><th>Argument Type(s)</th><th>Description</th></tr>
  </thead>
    <tr>
      <td><b> range </b>( <i>end</i> )</td>
      <td> Long </td>
      <td>Creates a table with a single <code>LongType</code> column named <code>id</code>, containing rows in a range from 0 to <code>end</code> (exclusive) with step value 1.</td>
    </tr>
    <tr>
      <td><b> range </b>( <i> start, end</i> )</td>
      <td> Long, Long </td>
      <td width="60%">Creates a table with a single <code>LongType</code> column named <code>id</code>, containing rows in a range from <code>start</code> to <code>end</code> (exclusive) with step value 1.</td>
    </tr>
    <tr>
      <td><b> range </b>( <i> start, end, step</i> )</td>
      <td> Long, Long, Long </td>
      <td width="60%">Creates a table with a single <code>LongType</code> column named <code>id</code>, containing rows in a range from <code>start</code> to <code>end</code> (exclusive) with <code>step</code> value.</td>
     </tr>
    <tr>
      <td><b> range </b>( <i> start, end, step, numPartitions</i> )</td>
      <td> Long, Long, Long, Int </td>
      <td width="60%">Creates a table with a single <code>LongType</code> column named <code>id</code>, containing rows in a range from <code>start</code> to <code>end</code> (exclusive) with <code>step</code> value, with partition number <code>numPartitions</code> specified. </td>
    </tr>
</table>

### Examples

{% highlight sql %}
-- range call with end
SELECT * FROM range(6 + cos(3));
  +---+
  | id|
  +---+
  |  0|
  |  1|
  |  2|
  |  3|
  |  4|
  +---+

-- range call with start and end
SELECT * FROM range(5, 10);
  +---+
  | id|
  +---+
  |  5|
  |  6|
  |  7|
  |  8|
  |  9|
  +---+

-- range call with numPartitions
SELECT * FROM range(0, 10, 2, 200);
  +---+
  | id|
  +---+
  |  0|
  |  2|
  |  4|
  |  6|
  |  8|
  +---+

-- range call with a table alias
SELECT * FROM range(5, 8) AS test;
  +---+
  | id|
  +---+
  |  5|
  |  6|
  |  7|
  +---+
{% endhighlight %}

### Related Statement

 * [SELECT](sql-ref-syntax-qry-select.html)
