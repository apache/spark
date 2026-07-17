---
layout: global
title: COMMENT ON
displayTitle: COMMENT ON
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

The `COMMENT ON` statement sets or updates the comment on a database, a table, or
one or more table columns. Setting the comment to `NULL` removes it.

### Syntax

```sql
COMMENT ON { DATABASE | SCHEMA | NAMESPACE } database_name IS comment

COMMENT ON TABLE table_name IS comment

COMMENT ON COLUMN table_name.column_name IS comment

COMMENT ON TABLE table_name COLUMN ( column_name IS comment [ , ... ] )
```

### Parameters

* **database_name**

    Specifies the name of the database on which the comment is set.

* **table_name**

    Specifies the name of the table on which the comment is set. The name may be
    qualified with a catalog and/or database name.

* **column_name**

    Specifies the column on which the comment is set. In the single-column form
    (`COMMENT ON COLUMN`), the identifier must have at least two parts: the last
    part is the column name and the preceding parts qualify the table (for example
    `table_name.column_name` or `database_name.table_name.column_name`). This form
    can only reference a top-level column. To comment on a nested field, or on
    multiple columns at once, use the `COMMENT ON TABLE table_name COLUMN ( ... )`
    form, where each `column_name` may be a nested field path such as `col.field`.

    A parent field and one of its nested fields cannot be commented in the same
    `COMMENT ON TABLE ... COLUMN` statement.

    The target of a column comment must be a table; views are not supported.

* **comment**

    A `STRING` literal or `NULL`. Setting the comment to `NULL` removes an existing
    comment; an empty string `''` sets an empty comment.

### Examples

```sql
-- Set a comment on a database
COMMENT ON DATABASE some_db IS 'This is a database';

-- Set a comment on a table
COMMENT ON TABLE some_db.some_table IS 'This is a table';

-- Set a comment on a single column
COMMENT ON COLUMN some_db.some_table.some_column IS 'This is a column';

-- Remove a column comment
COMMENT ON COLUMN some_db.some_table.some_column IS NULL;

-- Set comments on multiple columns, including a nested field
COMMENT ON TABLE some_db.some_table COLUMN (
  id IS 'The identifier',
  address.city IS 'The city part of the address'
);
```

### Related Statements

* [ALTER TABLE](sql-ref-syntax-ddl-alter-table.html)
* [DESCRIBE TABLE](sql-ref-syntax-aux-describe-table.html)
