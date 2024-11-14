---
layout: global
title: SQL Pipe Syntax
displayTitle: SQL Pipe Syntax
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

### Syntax

#### Overview

Apache Spark supports SQL pipe syntax which allows composing queries from combinations of operators.

* Any query can have zero or more pipe operators as a suffix, delineated by the pipe character `|>`. 
* Each pipe operator starts with one or more SQL keywords followed by its own grammar as described
  in the table below.
* Most of these operators reuse existing grammar for standard SQL clauses.
* Operators can apply in any order, any number of times.

`FROM <tableName>` is now a supported standalone query which behaves the same as
`TABLE <tableName>`. This provides a convenient starting place to begin a chained pipe SQL query,
although it is possible to add one or more pipe operators to the end of any valid Spark SQL query
with the same consistent behavior as written here.

Please refer to the table at the end of this document for a full list of all supported operators
and their semantics.

#### Example

For example, this is query 13 from the TPC-H benchmark:

```sql
SELECT c_count, COUNT(*) AS custdist
FROM
  (SELECT c_custkey, COUNT(o_orderkey) c_count
   FROM customer
   LEFT OUTER JOIN orders ON c_custkey = o_custkey
     AND o_comment NOT LIKE '%unusual%packages%' GROUP BY c_custkey
  ) AS c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC;
```

To write the same logic using SQL pipe operators, we express it like this:

```sql
FROM customer
|> LEFT OUTER JOIN orders ON c_custkey = o_custkey
   AND o_comment NOT LIKE '%unusual%packages%'
|> AGGREGATE COUNT(o_orderkey) c_count
   GROUP BY c_custkey
|> AGGREGATE COUNT(*) AS custdist
   GROUP BY c_count
|> ORDER BY custdist DESC, c_count DESC;
```

#### Projection

The following ways to evaluate expressions within projections are supported.

* `SELECT` produces a new table by evaluating the provided expressions.<br>
  It is possible to use `DISTINCT` and `*` as needed.<br>
  This works like the outermost `SELECT` in a table subquery in regular Spark SQL.
* `EXTEND` adds new columns to the input table by evaluating the provided expressions.<br>
  This also preserves table aliases.<br>
  This works like `SELECT *, new_column` in regular Spark SQL.
* `DROP` removes columns from the input table.<br>
  This is similar to `SELECT * EXCEPT (column)` in regular Spark SQL.
* `SET` replaces column values from the input table.<br>
  This is similar to `SELECT * REPLACE (expression AS column)` in regular Spark SQL.
* `AS` forwards the input table and introduces a new alias for each row.

A major advantage of these projection features is that they support applying repeatedly to
incrementally compute new expressions based on previous ones in a composable way. No lateral column
references are needed here since each operator applies independently on its input table, regardless
of the order in which they appear. Each of these computed columns are then visible to use any
following operators.

#### Aggregation

Aggregation takes place differently using SQL pipe syntax as opposed to regular Spark SQL.

* To perform full-table aggregation, use the `AGGREGATE` operator with a list of aggregate
expressions to evaluate.<br>
  This returns one single row in the output table.
* To perform aggregation with grouping, use the `AGGREGATE` oeprator with a `GROUP BY` clause.<br>
  This returns one row for each unique combination of values of the grouping expressions.

In both cases, the output table contains the evaluated grouping expressions followed by the
evaluated aggregate functions. Grouping expressions support assigning aliases for purposes of
referring to them in future operators. In this way, it is not necessary to repeat entire expressions
between `GROUP BY` and `SELECT`, since `AGGREGATE` is a single operator that performs both.

#### Filtering

The pipe `WHERE` operator performs any necessary filtering operations.

Since it may appear anywhere, no separate `HAVING` or `QUALIFY` syntax is needed.

#### Other Operations

Each other operator applies in a straightforward manner over its input table (as well as zero or
more other table or table subquery arguments) as listed in the table below.

### Independence and Interoperability

SQL pipe syntax works in Spark without any backwards-compatibility concerns with existing SQL
queries; it is possible to write any query using regular Spark SQL, pipe syntax, or a combination of
the two. As a consequence, the following invariants always hold:

* Each pipe operator receives an input table and operates the same way on its rows regardless of how
  it was computed.
* For any valid chain of N SQL pipe operators, any subset of the first M <= N operators also
represents a valid query.<br>
  This property can be useful for introspection and debugging, such as by selected a subset of
  lines and using the "run highlighted text" feature of SQL editors like Jupyter notebooks.
* It is possible to append pipe operators to any valid query written in regular Spark SQL.<br>
  The canonical way of starting pipe syntax queries is with the `FROM <tableName>` clause.<br>
  Note that this is a valid standalone query and may be replaced with any other Spark SQL query
  without loss of generality.
* Table subqueries can be written using either regular Spark SQL syntax or pipe syntax.<br>
  They may appear inside enclosing queries written in either syntax.
* Other Spark SQL statements such as views and DDL and DML commands may include queries written
  using either syntax.

### Supported Operators

This table lists each of the supported pipe operators and describes the output rows they produce.
Note that each operator accepts an input relation comprising the rows generated by the query
preceding the `|>` symbol.

| Operator                                                                                  | Output rows                                                                                                                                                                                                                           |
|-------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SELECT <expr> [[AS] alias], ...`                                                         | Evaluates the provided expressions over each of the rows<br/>of the input table.                                                                                                                                                      |
| `EXTEND <expr> [[AS] alias], ...`                                                         | Appends new columns to the input table by evaluating the<br/>specified expressions over each of the input rows.                                                                                                                       |
| `SET <column> = <expression>, ...`                                                        | Updates columns of the input table by replacing them with<br/>the result of evaluating the provided expressions.                                                                                                                      |
| `DROP <column>, ...`                                                                      | Drops columns of the input table by name.                                                                                                                                                                                             |
| `AS <alias>`                                                                              | Retains the same rows and column names of the input table<br/>but with a new table alias.                                                                                                                                             |
| `WHERE <condition>`                                                                       | Returns the subset of input rows passing the condition.                                                                                                                                                                               |
| `LIMIT <n> [OFFSET <m>]`                                                                  | Returns the specified number of input rows, preserving ordering<br/>(if any).                                                                                                                                                         |
| `AGGREGATE <agg_expr> [[AS] alias], ...`                                                  | Performs full-table aggregation, returning one result row with<br/>a column for each aggregate expression.                                                                                                                            |
| `AGGREGATE [<agg_expr> [[AS] alias], ...]`<br/>`GROUP BY <grouping_expr> [AS alias], ...` | Performs aggregation with grouping, returning one row per group.<br/>The column list includes the grouping columns first and then the<br/>aggregate columns afterwards. Aliases can be assigned directly<br/>on grouping expressions. |
| `[LEFT \| ...] JOIN <relation>`<br/>` [ON <condition> \| USING(col, ...)]`                | Joins rows from both inputs, returning a filtered cross-product of<br/>the pipe input table and the table expression following the<br/>JOIN keyword.                                                                                  |
| `ORDER BY <expr> [ASC \| DESC], ...`                                                      | Returns the input rows after sorting as indicated.                                                                                                                                                                                    |
| `{UNION \| INTERSECT \| EXCEPT}`<br/>`{ALL \| DISTINCT}`<br/>`(<query>), (<query>), ...`  | Performs the union or other set operation over the combined<br/>rows from the input table plus one or more tables provided as<br/>input arguments.                                                                                    |
| `TABLESAMPLE <method>`<br/>`(<size> {ROWS \| PERCENT})`                                   | Returns the subset of rows chosen by the provided sampling algorithm.                                                                                                                                                                 |
| `PIVOT`<br/>`(agg_expr FOR col IN (val1, ...))`                                           | Returns a new table with the input rows pivoted to become columns.                                                                                                                                                                    |
| `UNPIVOT`<br/>`(value_col FOR key_col IN (col1, ...))`                                    | Returns a new table with the input columns pivoted to become rows.                                                                                                                                                                    |
