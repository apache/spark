---
layout: global
title: MERGE INTO
displayTitle: MERGE INTO
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

The `MERGE INTO` statement merges a source table or query into a target table. Rows of the
target table are matched against the source using a merge condition. Target rows are then
updated or deleted, and new rows are inserted from the source, according to the clauses that
apply. All of these row-level changes are performed as a single atomic operation.

`MERGE INTO` is supported on tables backed by
[Data Source V2](sql-v2-data-sources.html#row-level-dml) connectors that support row-level operations.

### Syntax

```sql
MERGE [ WITH SCHEMA EVOLUTION ] INTO target_table [ [ AS ] target_alias ]
    USING { source_table | ( source_query ) } [ [ AS ] source_alias ]
    ON merge_condition
    [ WHEN MATCHED [ AND matched_condition ] THEN matched_action ] [ ... ]
    [ WHEN NOT MATCHED [ BY TARGET ] [ AND not_matched_condition ] THEN not_matched_action ] [ ... ]
    [ WHEN NOT MATCHED BY SOURCE [ AND not_matched_by_source_condition ] THEN not_matched_by_source_action ] [ ... ]

matched_action
    { DELETE | UPDATE SET * | UPDATE SET { column = value } [ , ... ] }

not_matched_action
    { INSERT * | INSERT ( column [ , ... ] ) VALUES ( value [ , ... ] ) }

not_matched_by_source_action
    { DELETE | UPDATE SET { column = value } [ , ... ] }
```

### Parameters

* **WITH SCHEMA EVOLUTION**

    Enables automatic schema evolution for this `MERGE` operation. When enabled, the schema
    of the target table is automatically evolved to add new columns and widen data types based
    on the source table or query, subject to the capabilities of the underlying connector.

* **target_table**

    Specifies the table to be merged into, which may be optionally qualified with a database name.
    An optional alias may be provided with or without the `AS` keyword.

    **Syntax:** `[ database_name. ] table_name [ [ AS ] alias ]`

* **source_table** / **source_query**

    The source of the merge, specified either as a table or as a parenthesized query. An optional
    alias may be provided with or without the `AS` keyword.

* **merge_condition**

    A boolean expression, introduced by the `ON` keyword, that determines how rows from the source
    match rows in the target. It controls which `WHEN` clauses apply to each row.

* **WHEN MATCHED [ AND matched_condition ] THEN matched_action**

    Applies to rows that exist in both the source and the target (according to `merge_condition`).
    The optional `matched_condition` further restricts which matched rows the clause applies to.
    The `matched_condition` and the `matched_action` values can reference columns of both the
    target and the source tables.
    The `matched_action` can be one of:
    - `DELETE`: deletes the matched target row.
    - `UPDATE SET *`: updates the matched target row with all source columns, matched by name.
    - `UPDATE SET column = value [ , ... ]`: updates the listed target columns with the given values.

* **WHEN NOT MATCHED [ BY TARGET ] [ AND not_matched_condition ] THEN not_matched_action**

    Applies to source rows that have no matching row in the target. `BY TARGET` is optional and is
    the default interpretation of `WHEN NOT MATCHED`. The optional `not_matched_condition` further
    restricts which unmatched source rows the clause applies to. The `not_matched_condition` and the
    `not_matched_action` values can reference columns of the source table only.
    The `not_matched_action` can be one of:
    - `INSERT *`: inserts a new target row using all source columns, matched by name.
    - `INSERT ( column [ , ... ] ) VALUES ( value [ , ... ] )`: inserts a new target row, assigning
      the given values to the listed columns.

* **WHEN NOT MATCHED BY SOURCE [ AND not_matched_by_source_condition ] THEN not_matched_by_source_action**

    Applies to target rows that have no matching row in the source. The optional
    `not_matched_by_source_condition` further restricts which such target rows the clause applies to.
    The `not_matched_by_source_condition` and the `not_matched_by_source_action` values can reference
    columns of the target table only.
    The `not_matched_by_source_action` can be one of:
    - `DELETE`: deletes the unmatched target row.
    - `UPDATE SET column = value [ , ... ]`: updates the listed target columns with the given values.

**Note:**
- A `MERGE INTO` statement must contain at least one `WHEN` clause.
- Multiple clauses of the same type may be specified. For a given row, the clauses of the relevant
  type are evaluated in the order they are written, and the first clause whose condition is satisfied
  is applied. When multiple clauses of the same type are specified, only the last one may omit its
  `AND` condition.
- If a single target row matches more than one source row, the statement fails with a
  `MERGE_CARDINALITY_VIOLATION` error, since the row would otherwise be updated or deleted more than
  once. This check is skipped when there are no `WHEN MATCHED` clauses, or when the only `WHEN MATCHED`
  action is an unconditional `DELETE`, because in those cases the outcome does not depend on the number
  of matching source rows.
- When `WITH SCHEMA EVOLUTION` is specified, only the columns that are referenced in the merge clauses
  by a direct assignment are added or have their data type widened.
- `UPDATE SET *` and `INSERT *` count as direct assignments of every target column, so schema
  evolution is triggered for all of them.
- An `UPDATE` or `INSERT` that directly assigns a struct column counts as a direct assignment of every
  nested field in that struct, so schema evolution is triggered for all of those fields.

### Examples

The following examples assume that the `target` and `source` tables have already been created and
populated as shown below. Each example starts from this initial state.

```sql
SELECT * FROM target;
+--+------+-------+
|pk|salary|    dep|
+--+------+-------+
| 1|   300|     hr|
| 2|   160|    eng|
| 5|   100|finance|
+--+------+-------+

SELECT * FROM source;
+--+------+-----+
|pk|salary|  dep|
+--+------+-----+
| 1|   350|   hr|
| 2|   200|  eng|
| 3|   120|sales|
+--+------+-----+
```

#### Update Matched Rows and Insert New Rows

```sql
MERGE INTO target t
    USING source s
    ON t.pk = s.pk
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;

SELECT * FROM target;
+--+------+-------+
|pk|salary|    dep|
+--+------+-------+
| 1|   350|     hr|
| 2|   200|    eng|
| 3|   120|  sales|
| 5|   100|finance|
+--+------+-------+
```

#### Conditional Delete, Update, and Insert

```sql
-- Delete matched rows whose source salary exceeds 300, update the remaining matched rows,
-- and insert source rows that do not match any target row.
MERGE INTO target t
    USING source s
    ON t.pk = s.pk
    WHEN MATCHED AND s.salary > 300 THEN DELETE
    WHEN MATCHED THEN UPDATE SET salary = s.salary
    WHEN NOT MATCHED THEN INSERT (pk, salary, dep) VALUES (s.pk, s.salary, s.dep);

SELECT * FROM target;
+--+------+-------+
|pk|salary|    dep|
+--+------+-------+
| 2|   200|    eng|
| 3|   120|  sales|
| 5|   100|finance|
+--+------+-------+
```

#### Delete Target Rows Not Present in the Source

```sql
MERGE INTO target t
    USING source s
    ON t.pk = s.pk
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE;

SELECT * FROM target;
+--+------+-----+
|pk|salary|  dep|
+--+------+-----+
| 1|   350|   hr|
| 2|   200|  eng|
| 3|   120|sales|
+--+------+-----+
```

#### Update Target Rows Not Present in the Source

```sql
MERGE INTO target t
    USING source s
    ON t.pk = s.pk
    WHEN NOT MATCHED BY SOURCE THEN UPDATE SET dep = 'unassigned';

SELECT * FROM target;
+--+------+----------+
|pk|salary|       dep|
+--+------+----------+
| 1|   300|        hr|
| 2|   160|       eng|
| 5|   100|unassigned|
+--+------+----------+
```

#### Schema Evolution

```sql
SELECT * FROM students;
+-------------+--------------------------+----------+
|         name|                   address|student_id|
+-------------+--------------------------+----------+
|    Amy Smith|    123 Park Ave, San Jose|    111111|
|    Bob Brown|  456 Taylor St, Cupertino|    222222|
|  Helen Davis| 469 Mission St, San Diego|    999999|
+-------------+--------------------------+----------+

-- This example uses a source table that has an extra "email" column not present in the target.
SELECT * FROM new_students;
+-------------+--------------------------+----------+-----------------+
|         name|                   address|student_id|            email|
+-------------+--------------------------+----------+-----------------+
|    Amy Smith|    123 Park Ave, San Jose|    111111|  amy@example.com|
|Cathy Johnson|   789 Race Ave, Palo Alto|    333333|cathy@example.com|
+-------------+--------------------------+----------+-----------------+

-- Evolve the students table schema to add the new email column from the source.
MERGE WITH SCHEMA EVOLUTION INTO students t
    USING new_students s
    ON t.student_id = s.student_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;

SELECT * FROM students;
+-------------+--------------------------+----------+-----------------+
|         name|                   address|student_id|            email|
+-------------+--------------------------+----------+-----------------+
|    Amy Smith|    123 Park Ave, San Jose|    111111|  amy@example.com|
|    Bob Brown|  456 Taylor St, Cupertino|    222222|             NULL|
|Cathy Johnson|   789 Race Ave, Palo Alto|    333333|cathy@example.com|
|  Helen Davis| 469 Mission St, San Diego|    999999|             NULL|
+-------------+--------------------------+----------+-----------------+
```

Schema evolution also applies to nested fields. In the following example the source's `address`
struct has an extra `zip` field that is added to the target's `address` struct.

```sql
SELECT * FROM student_addresses;
+----------+-----------+
|student_id|    address|
+----------+-----------+
|    111111| {San Jose}|
|    222222|{Cupertino}|
+----------+-----------+

SELECT * FROM address_updates;
+----------+------------------+
|student_id|           address|
+----------+------------------+
|    111111| {San Jose, 95110}|
|    333333|{Palo Alto, 94301}|
+----------+------------------+

-- Evolve the address struct to add the new nested zip field.
MERGE WITH SCHEMA EVOLUTION INTO student_addresses t
    USING address_updates s
    ON t.student_id = s.student_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;

SELECT * FROM student_addresses;
+----------+------------------+
|student_id|           address|
+----------+------------------+
|    111111| {San Jose, 95110}|
|    222222| {Cupertino, NULL}|
|    333333|{Palo Alto, 94301}|
+----------+------------------+
```

Schema evolution can also widen the data type of an existing column. In the following example the
source's `credits` column is a `BIGINT` that holds a value too large for the target's `INT` column,
so the target's `credits` column is widened from `INT` to `BIGINT`.

```sql
SELECT * FROM student_credits;
+----------+-------+
|student_id|credits|
+----------+-------+
|    111111|     30|
|    222222|     45|
+----------+-------+

-- The source's credits column is a BIGINT and contains a value that does not fit in an INT.
SELECT * FROM credit_updates;
+----------+-----------+
|student_id|    credits|
+----------+-----------+
|    111111|10000000000|
|    333333|         60|
+----------+-----------+

-- Widen the target's credits column from INT to BIGINT to accommodate the source values.
MERGE WITH SCHEMA EVOLUTION INTO student_credits t
    USING credit_updates s
    ON t.student_id = s.student_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;

SELECT * FROM student_credits;
+----------+-----------+
|student_id|    credits|
+----------+-----------+
|    111111|10000000000|
|    222222|         45|
|    333333|         60|
+----------+-----------+
```

#### Schema Evolution Triggers

Schema evolution adds or widens a column only when a source column is referenced by a direct
assignment to a target column. Referencing a source column only inside an expression does not evolve
the target. Conversely, directly assigning a whole struct counts as referencing it, so all of its
new nested fields are added.

In the following example the source has an extra `bonus` column that is only used in an expression,
so the target table is not evolved and no `bonus` column is added.

```sql
SELECT * FROM student_credits;
+----------+-------+
|student_id|credits|
+----------+-------+
|    111111|     30|
|    222222|     45|
+----------+-------+

-- The source has an extra bonus column not present in the target.
SELECT * FROM credit_transfers;
+----------+-------+-----+
|student_id|credits|bonus|
+----------+-------+-----+
|    111111|     30|   12|
|    333333|     60|    5|
+----------+-------+-----+

-- bonus is only referenced inside an expression, so the target schema is left unchanged.
MERGE WITH SCHEMA EVOLUTION INTO student_credits t
    USING credit_transfers s
    ON t.student_id = s.student_id
    WHEN MATCHED THEN UPDATE SET credits = s.credits + s.bonus
    WHEN NOT MATCHED THEN INSERT (student_id, credits) VALUES (s.student_id, s.credits + s.bonus);

-- The target schema is unchanged: no bonus column is added.
SELECT * FROM student_credits;
+----------+-------+
|student_id|credits|
+----------+-------+
|    111111|     42|
|    222222|     45|
|    333333|     65|
+----------+-------+
```

Type widening follows the same rule. If a wider value is produced by an expression instead of a
direct assignment, the target column keeps its type, so a value that does not fit can no longer be
stored and the statement fails. The example below reuses the `credit_updates` table, whose `credits`
column is a `BIGINT` value too large for an `INT`.

```sql
-- credits is produced by an expression, so the target column is not widened from INT to BIGINT.
MERGE WITH SCHEMA EVOLUTION INTO student_credits t
    USING credit_updates s
    ON t.student_id = s.student_id
    WHEN MATCHED THEN UPDATE SET credits = s.credits + 100;
Error: CAST_OVERFLOW_IN_TABLE_INSERT
```

Directly assigning a struct column from the source, on the other hand, counts as referencing it, so
its new nested fields are added even though they are not named individually. In the following example
assigning the whole struct with `UPDATE SET address = s.address` adds the source's new `zip` field to
the target's `address` struct.

```sql
SELECT * FROM student_addresses;
+----------+-----------+
|student_id|    address|
+----------+-----------+
|    111111| {San Jose}|
|    222222|{Cupertino}|
+----------+-----------+

-- The source's address struct has an extra zip field not present in the target.
SELECT * FROM address_changes;
+----------+------------------+
|student_id|           address|
+----------+------------------+
|    222222|{Cupertino, 95014}|
+----------+------------------+

-- Assigning the whole struct directly adds the new zip nested field.
MERGE WITH SCHEMA EVOLUTION INTO student_addresses t
    USING address_changes s
    ON t.student_id = s.student_id
    WHEN MATCHED THEN UPDATE SET address = s.address;

SELECT * FROM student_addresses;
+----------+------------------+
|student_id|           address|
+----------+------------------+
|    111111|  {San Jose, NULL}|
|    222222|{Cupertino, 95014}|
+----------+------------------+
```

#### Schema Evolution When the Source Omits Columns

With schema evolution enabled, `UPDATE SET *` and `INSERT *` also tolerate a source that is missing
some of the target's columns. A missing column keeps its existing value on an update and is set to
its default value (or `NULL`) on an insert. Without schema evolution the same statement fails,
because `*` requires the source to provide every target column.

```sql
SELECT * FROM students;
+-------------+--------------------------+----------+
|         name|                   address|student_id|
+-------------+--------------------------+----------+
|    Amy Smith|    123 Park Ave, San Jose|    111111|
|    Bob Brown|  456 Taylor St, Cupertino|    222222|
|  Helen Davis| 469 Mission St, San Diego|    999999|
+-------------+--------------------------+----------+

-- The source is missing the address column that exists in the target.
SELECT * FROM name_updates;
+-------------+----------+
|         name|student_id|
+-------------+----------+
|    Amy Smith|    111111|
|Cathy Johnson|    333333|
+-------------+----------+

MERGE WITH SCHEMA EVOLUTION INTO students t
    USING name_updates s
    ON t.student_id = s.student_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;

-- Student 111111 keeps its address on update; student 333333 is inserted with a NULL address.
SELECT * FROM students;
+-------------+--------------------------+----------+
|         name|                   address|student_id|
+-------------+--------------------------+----------+
|    Amy Smith|    123 Park Ave, San Jose|    111111|
|    Bob Brown|  456 Taylor St, Cupertino|    222222|
|Cathy Johnson|                      NULL|    333333|
|  Helen Davis| 469 Mission St, San Diego|    999999|
+-------------+--------------------------+----------+
```

### Related Statements

* [INSERT TABLE statement](sql-ref-syntax-dml-insert-table.html)
* [SELECT statement](sql-ref-syntax-qry-select.html)
