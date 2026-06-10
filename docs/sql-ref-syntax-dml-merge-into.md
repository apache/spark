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
target table are matched against the source using a merge condition, and each row is then
updated, deleted, or inserted according to the clauses that apply to it. All of these row-level
changes are performed as a single atomic operation.

`MERGE INTO` is supported on tables backed by
[Data Source V2](sql-data-sources-v2.html#row-level-dml) connectors that support row-level operations.

### Syntax

```sql
MERGE INTO target_table [ [ AS ] target_alias ]
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
    The `matched_action` can be one of:
    - `DELETE`: deletes the matched target row.
    - `UPDATE SET *`: updates the matched target row with all source columns, matched by name.
    - `UPDATE SET column = value [ , ... ]`: updates the listed target columns with the given values.

* **WHEN NOT MATCHED [ BY TARGET ] [ AND not_matched_condition ] THEN not_matched_action**

    Applies to source rows that have no matching row in the target. `BY TARGET` is optional and is
    the default interpretation of `WHEN NOT MATCHED`. The optional `not_matched_condition` further
    restricts which unmatched source rows the clause applies to. The `not_matched_action` can be one of:
    - `INSERT *`: inserts a new target row using all source columns, matched by name.
    - `INSERT ( column [ , ... ] ) VALUES ( value [ , ... ] )`: inserts a new target row, assigning
      the given values to the listed columns.

* **WHEN NOT MATCHED BY SOURCE [ AND not_matched_by_source_condition ] THEN not_matched_by_source_action**

    Applies to target rows that have no matching row in the source. The optional
    `not_matched_by_source_condition` further restricts which such target rows the clause applies to.
    The `not_matched_by_source_action` can be one of:
    - `DELETE`: deletes the unmatched target row.
    - `UPDATE SET column = value [ , ... ]`: updates the listed target columns with the given values.

**Note:**
- A `MERGE INTO` statement must contain at least one `WHEN` clause.
- Multiple clauses of the same type may be specified. For a given row, the clauses of the relevant
  type are evaluated in the order they are written, and the first clause whose condition is satisfied
  is applied.
- If a single target row matches more than one source row, the statement fails with a
  `MERGE_CARDINALITY_VIOLATION` error, since the row would otherwise be updated or deleted more than
  once. This check is skipped when there are no `WHEN MATCHED` clauses, or when the only `WHEN MATCHED`
  action is an unconditional `DELETE`, because in those cases the outcome does not depend on the number
  of matching source rows.

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

### Related Statements

* [INSERT TABLE statement](sql-ref-syntax-dml-insert-table.html)
* [SELECT statement](sql-ref-syntax-qry-select.html)
