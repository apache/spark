---
layout: global
title: SHOW COLUMNS
displayTitle: SHOW COLUMNS
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
Return the list of columns in a table. If the table does not exist, an exception is thrown.

### Syntax
{% highlight sql %}
SHOW COLUMNS table_identifier [ database ]
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>table_identifier</em></code></dt>
  <dd>
    Specifies the table name of an existing table. The table may be optionally qualified
    with a database name.<br><br>
    <b>Syntax:</b>
      <code>
        { IN | FROM } [ database_name . ] table_name
      </code><br><br>
    <b>Note:</b>
    Keywords <code>IN</code> and <code>FROM</code> are interchangeable.
  </dd>
  <dt><code><em>database</em></code></dt>
  <dd>
    Specifies an optional database name. The table is resolved from this database when it
    is specified. Please note that when this parameter is specified then table
    name should not be qualified with a different database name. <br><br>
    <b>Syntax:</b>
      <code>
        { IN | FROM } database_name
      </code><br><br>
    <b>Note:</b>
    Keywords <code>IN</code> and <code>FROM</code> are interchangeable.
  </dd>
</dl>

### Examples
{% highlight sql %}
-- Create `customer` table in `salesdb` database;
USE salesdb;
CREATE TABLE customer(cust_cd INT,
  name VARCHAR(100),
  cust_addr STRING);

-- List the columns of `customer` table in current database.
SHOW COLUMNS IN customer;
  +---------+
  |col_name |
  +---------+
  |cust_cd  |
  |name     |
  |cust_addr|
  +---------+

-- List the columns of `customer` table in `salesdb` database.
SHOW COLUMNS IN salesdb.customer;
  +---------+
  |col_name |
  +---------+
  |cust_cd  |
  |name     |
  |cust_addr|
  +---------+

-- List the columns of `customer` table in `salesdb` database
SHOW COLUMNS IN customer IN salesdb;
  +---------+
  |col_name |
  +---------+
  |cust_cd  |
  |name     |
  |cust_addr|
  +---------+
{% endhighlight %}

### Related Statements
- [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
- [SHOW TABLE](sql-ref-syntax-aux-show-table.html)
