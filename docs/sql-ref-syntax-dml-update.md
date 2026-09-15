---
layout: global
title: UPDATE
displayTitle: UPDATE
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

The `UPDATE` statement changes the values of columns in rows that satisfy an optional condition.
When no condition is specified, every row is updated.

`UPDATE` is supported on tables backed by
[Data Source V2](sql-v2-data-sources.html#row-level-dml) connectors that support row-level
operations.

### Syntax

```sql
UPDATE table_identifier [ [ AS ] table_alias ]
    [ WITH ( key = value [ , ... ] ) ]
    SET column = value [ , ... ]
    [ WHERE boolean_expression ]
```

### Parameters

* **table_identifier**

    Specifies the table to update, which may be optionally qualified with a database name.

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

* **SET column = value [ , ... ]**

    Specifies the columns to update and the values to assign to them. Each `value` is an expression,
    typically referencing columns of the target table, but it may also be `DEFAULT` or an
    uncorrelated scalar subquery over another table. A comma separates each assignment. A nested
    field may be targeted by using a qualified column name.

* **WHERE boolean_expression**

    Specifies an optional condition that selects the rows to update. If the `WHERE` clause is
    omitted, all rows are updated.

### Examples

The following examples assume that an `employees` table has already been created and populated.

#### Update Rows Matching a Condition

```sql
UPDATE employees
    SET salary = salary + 1000
    WHERE department = 'Engineering';
```

#### Update Multiple Columns Using an Alias

```sql
UPDATE employees AS e
    SET e.salary = e.salary * 1.05, e.status = 'reviewed'
    WHERE e.department = 'Sales';
```

#### Update Using Dynamic Table Options

```sql
-- Option names and values are specific to the table's data source connector.
UPDATE employees WITH (`write.split-size` = 10)
    SET status = 'inactive'
    WHERE last_active_date < DATE '2025-01-01';
```

#### Update All Rows

```sql
UPDATE employees SET status = 'active';
```

### Related Statements

* [DELETE FROM statement](sql-ref-syntax-dml-delete-from.html)
* [INSERT TABLE statement](sql-ref-syntax-dml-insert-table.html)
* [MERGE INTO statement](sql-ref-syntax-dml-merge-into.html)
* [SELECT statement](sql-ref-syntax-qry-select.html)
