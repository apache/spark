---
layout: global
title: DELETE FROM
displayTitle: DELETE FROM
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

The `DELETE FROM` statement removes rows from a table that satisfy an optional condition. When no
condition is specified, every row is removed.

`DELETE FROM` is supported on tables backed by
[Data Source V2](sql-v2-data-sources.html#row-level-dml) connectors that support delete operations.

### Syntax

```sql
DELETE FROM table_identifier [ [ AS ] table_alias ]
    [ WITH ( key = value [ , ... ] ) ]
    [ WHERE boolean_expression ]
```

### Parameters

* **table_identifier**

    Specifies the table from which rows are deleted. The table name may be optionally qualified
    with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **table_alias**

    Specifies an optional alias for the target table. The alias may be introduced with or without
    the `AS` keyword.

* **WITH ( key = value [ , ... ] )**

    Specifies an optional list of dynamic table options passed to the Data Source V2 connector for
    this statement only. The options allow per-statement tuning without changing the table's
    persistent configuration. Keys and values are treated as strings; a key that is not a valid
    identifier can be quoted with backticks. Spark passes options through without validating their
    names, and connectors may ignore options they do not recognize.

* **WHERE boolean_expression**

    Specifies an optional condition that selects the rows to delete. If the `WHERE` clause is
    omitted, all rows are deleted.

### Examples

The following examples assume that an `employees` table has already been created and populated.

#### Delete Rows Matching a Condition

```sql
DELETE FROM employees WHERE status = 'inactive';
```

#### Delete Rows Using an Alias

```sql
DELETE FROM employees AS e
    WHERE e.department = 'Sales' AND e.last_active_date < DATE '2025-01-01';
```

#### Delete Using Dynamic Table Options

```sql
-- Option names and values are specific to the table's data source connector.
DELETE FROM employees WITH (`write.split-size` = 10)
    WHERE status = 'inactive';
```

#### Delete All Rows

```sql
DELETE FROM employees;
```

### Related Statements

* [INSERT TABLE statement](sql-ref-syntax-dml-insert-table.html)
* [MERGE INTO statement](sql-ref-syntax-dml-merge-into.html)
* [SELECT statement](sql-ref-syntax-qry-select.html)
* [UPDATE statement](sql-ref-syntax-dml-update.html)
