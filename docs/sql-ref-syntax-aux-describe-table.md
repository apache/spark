---
layout: global
title: DESCRIBE TABLE
displayTitle: DESCRIBE TABLE
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
DESCRIBE TABLE statement is used to return the metadata information of a
table. The basic metadata information includes column name, column type
and column comment. If EXTENDED mode is specified then additional
metadata such as parent database, owner, created and last access time etc.
are returned. Optionally a partition spec or column name can be specified
to return the metadata pertaining to the partition or column respectively.

### Syntax
{% highlight sql %}
{DESC | DESCRIBE} [TABLE] [format] tableIdentifier [partition-spec] [desc-col]

format:= EXTENDED | FORMATTED
tableIdentifier:= [db.][table]
parition-spec:= PARTITION (part_col1 = value1, part_col2 = value2, ...) 
desc-col:= [db.][table.]column-name
{% endhighlight %}
**Note**<br>
* FORMATTED and EXTENDED are interchangeble and mean the same thing.
* DESC and DESCRIBE are interchangeble and mean the same thing.
* TABLE is optional.

### Examples
{% highlight sql %}
-- Returns column name, column type and comment info for customer table.
-- The table is resolved from the current database as the table name is unqualified.
DESCRIBE TABLE customer;
-- Returns column name, column type and nullability info for customer table.
-- The table is resolved from salesdb database  as the table name is qualified.
DESCRIBE TABLE salesdb.customer;
-- Returns additional metadata such as parent database, owner, access time, 
-- location, serde info, input and output formats.
DESCRIBE TABLE EXTENDED customer;
-- Returns partition metadata such as partitioning column name, column type and comment.
DESCRIBE TABLE customer PARTITION (cust_id = 100);
-- Returns column metadata such as column name, column type and comment. 
-- Optional TABLE clause is omitted. Column name is fully qualified.
DESCRIBE customer salesdb.customer.name;
-- Returns column metadata such as column name, column type and comment. 
-- Table is qualified with database name and Column name is not qualified.
DESCRIBE salesdb.customer address;
{% endhighlight %}

### Current Restriction
* DESC COLUMN does not support nested column names.

### Related Statements
- [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
- [DESCRIBE QUERY](sql-ref-syntax-aux-describe-query.html)
- [DESCRIBE FUNCTION](sql-ref-syntax-aux-describe-function.html)
