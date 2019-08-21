---
layout: global
title: INSERT OVERWRITE TABLE
displayTitle: INSERT OVERWRITE TABLE
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

Overwrite existing data in the table using the new values.

### Syntax
{% highlight sql %}
INSERT OVERWRITE [TABLE] [db_name.]table_name [partition_spec [IF NOT EXISTS]]
  valueClause | query

partition_spec:
    (part_col_name1[=val1] [, part_col_name2[=val2], ...])

value_clause:
    : VALUES values_row [, values_row ...]

    values_row:
        : (val1 [, val2, ...])
{% endhighlight %}

### Examples
{% highlight sql %}
 CREATE TABLE employees (name VARCHAR(64), age INT, salary DECIMAL(9,2))
   USING PARQUET PARTITIONED BY (age)

 INSERT OVERWRITE TABLE employees
   VALUES ('Cathy Johnson', 35, 200000.00)

 INSERT OVERWRITE TABLE employees PARTITION (age = 35)
   SELECT * FROM candidates WHERE name = "Bob Doe"

{% endhighlight %}
### Parameters

#### ***table_name***:
The name of an existing table.

#### ***partition_spec***:
Partition column specification.

**Note:** When the partition values are not provided, these columns are referred to as dynamic partition columns and such inserts are called dynamic partition inserts, also called multi-partition inserts. Please refer to [Dynamic Partition Inserts](sql-ref-syntax-dml-dynamic-partition-insert.html).

#### ***value_clause***:
Specify the values to be inserted.

#### ***query***:
A query (SELECT statement) that provides the rows to be inserted.