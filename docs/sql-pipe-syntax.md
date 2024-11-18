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

#### Source Tables

To start a new query using SQL pipe syntax, use the `FROM <tableName>` or `TABLE <tableName>`
clause, which creates a relation comprising all rows from the source table. Then append one or more
pipe operators to the end of this clause to perform further transformations.

#### Projections

SQL pipe syntax supports composable ways to evaluate expressions. A major advantage of these
projection features is that they support computing new expressions based on previous ones in an
incremental way. No lateral column references are needed here since each operator applies
independently on its input table, regardless of the order in which the operators appear. Each of
these computed columns then becomes visible to use with the following operator.

`SELECT` produces a new table by evaluating the provided expressions.<br>
It is possible to use `DISTINCT` and `*` as needed.<br>
This works like the outermost `SELECT` in a table subquery in regular Spark SQL.

`EXTEND` adds new columns to the input table by evaluating the provided expressions.<br>
This also preserves table aliases.<br>
This works like `SELECT *, new_column` in regular Spark SQL.

`DROP` removes columns from the input table.<br>
This is similar to `SELECT * EXCEPT (column)` in regular Spark SQL.

`SET` replaces column values from the input table.<br>
This is similar to `SELECT * REPLACE (expression AS column)` in regular Spark SQL.

`AS` forwards the input table and introduces a new alias for each row.

#### Aggregations

In general, aggregation takes place differently using SQL pipe syntax as opposed to regular Spark
SQL.

To perform full-table aggregation, use the `AGGREGATE` operator with a list of aggregate
expressions to evaluate. This returns one single row in the output table.

To perform aggregation with grouping, use the `AGGREGATE` operator with a `GROUP BY` clause.
This returns one row for each unique combination of values of the grouping expressions. The output
table contains the evaluated grouping expressions followed by the evaluated aggregate functions.
Grouping expressions support assigning aliases for purposes of referring to them in future
operators. In this way, it is not necessary to repeat entire expressions between `GROUP BY` and
`SELECT`, since `AGGREGATE` is a single operator that performs both.

#### Other Transformations

The remaining operators are used for other transformations, such as filtering, joining, sorting,
sampling, and set operations. These operators generally work in the same way as in regular Spark
SQL, as described in the table below.

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

| Operator                                          | Output rows                                                                                                         |
|---------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| [FROM](#from-or-table) or [TABLE](#from-or-table) | Returns all the output rows from the source table unmodified.                                                       |
| [SELECT](#select)                                 | Evaluates the provided expressions over each of the rows of the input table.                                        |
| [EXTEND](#extend)                                 | Appends new columns to the input table by evaluating the specified expressions over each of the input rows.         |
| [SET](#set)                                       | Updates columns of the input table by replacing them with the result of evaluating the provided expressions.        |
| [DROP](#drop)                                     | Drops columns of the input table by name.                                                                           |
| [AS](#as)                                         | Retains the same rows and column names of the input table but with a new table alias.                               |
| [WHERE](#where)                                   | Returns the subset of input rows passing the condition.                                                             |
| [LIMIT](#limit)                                   | Returns the specified number of input rows, preserving ordering (if any).                                           |
| [AGGREGATE](#aggregate)                           | Performs aggregation with or without grouping.                                                                      |
| [JOIN](#join)                                     | Joins rows from both inputs, returning a filtered cross-product of the input table and the table argument.          |
| [ORDER BY](#order-by)                             | Returns the input rows after sorting as indicated.                                                                  |
| [UNION ALL](#union-intersect-except)              | Performs the union or other set operation over the combined rows from the input table plus other table argument(s). |
| [TABLESAMPLE](#tablesample)                       | Returns the subset of rows chosen by the provided sampling algorithm.                                               |
| [PIVOT](#pivot)                                   | Returns a new table with the input rows pivoted to become columns.                                                  |
| [UNPIVOT](#unpivot)                               | Returns a new table with the input columns pivoted to become rows.                                                  |

This table lists each of the supported pipe operators and describes the output rows they produce.
Note that each operator accepts an input relation comprising the rows generated by the query
preceding the `|>` symbol.

#### FROM or TABLE

```sql
FROM <tableName>
```

```sql
TABLE <tableName>
```

Returns all the output rows from the source table unmodified.

For example:

```sql
CREATE TABLE t(a INT, b INT) AS VALUES (1, 2), (3, 4);
TABLE t;

+---+---+
|  a|  b|
+---+---+
|  1|  2|
|  3|  4|
+---+---+
```

#### SELECT

```sql
|> SELECT <expr> [[AS] alias], ...
```

Evaluates the provided expressions over each of the rows of the input table.                                                                                                                                                    

It is possible to use `DISTINCT` and `*` as needed.<br>
This works like the outermost `SELECT` in a table subquery in regular Spark SQL.

Window functions are supported in the `SELECT` list as well. To use them, the `OVER` clause must be
provided. You may provide the window specification in the `WINDOW` clause.

For example:

```sql
CREATE TABLE t(col INT) AS VALUES (0), (1);

FROM t
|> SELECT col * 2 AS result;

+------+
|result|
+------+
|     0|
|     2|
+------+
```

#### EXTEND

```sql
|> EXTEND <expr> [[AS] alias], ...
```

Appends new columns to the input table by evaluating the specified expressions over each of the input rows.

For example:

```sql
VALUES (0), (1) tab(col)
|> EXTEND col * 2 AS result;

+---+------+
|col|result|
+---+------+
|  0|     0|
|  1|     2|
+---+------+
```

#### SET

```sql
|> SET <column> = <expression>, ...
```

Updates columns of the input table by replacing them with the result of evaluating the provided expressions.

For example:

```sql
VALUES (0), (1) tab(col)
|> SET col = col * 2;

+---+
|col|
+---+
|  0|
|  2|
+---+
```

#### DROP

```sql
|> DROP <column>, ...
```

Drops columns of the input table by name.

For example:

```sql
VALUES (0, 1) tab(col1, col2)
|> DROP col1;

+----+
|col2|
+----+
|   1|
+----+
```

#### AS

```sql
|> AS <alias>
```

Retains the same rows and column names of the input table but with a new table alias.

For example:

```sql
VALUES (0, 1) tab(col1, col2)
|> AS new_tab;
|> SELECT * FROM new_tab;

+----+----+
|col1|col2|
+----+----+
|   0|   1|
+----+----+
```

#### WHERE

```sql
|> WHERE <condition>
```

Returns the subset of input rows passing the condition.

Since this operator may appear anywhere, no separate `HAVING` or `QUALIFY` syntax is needed.

For example:

```sql
VALUES (0), (1) tab(col)
|> WHERE col = 1;

+---+
|col|
+---+
|  1|
+---+
```

#### LIMIT

```sql
|> [LIMIT <n>] [OFFSET <m>]
```

Returns the specified number of input rows, preserving ordering (if any).

`LIMIT` and `OFFSET` are supported together. The `LIMIT` clause can also be used without the
`OFFSET` clause, and the `OFFSET` clause can be used without the `LIMIT` clause.

For example:

```sql
VALUES (0), (0) tab(col)
|> LIMIT 1;

+---+
|col|
+---+
|  0|
+---+
```

#### AGGREGATE

```sql
|> AGGREGATE <agg_expr> [[AS] alias], ...
```

Performs full-table aggregation, returning one result row with a column for each aggregate expression.

```sql
|> AGGREGATE [<agg_expr> [[AS] alias], ...] GROUP BY <grouping_expr> [AS alias], ...
```

Performs aggregation with grouping, returning one row per group. The column list includes the
grouping columns first and then the aggregate columns afterward. Aliases can be assigned directly
on grouping expressions.

For example:

```sql
VALUES (0), (1) tab(col)
|> AGGREGATE COUNT(col) AS count;

+-----+
|count|
+-----+
|    2|
+-----+

VALUES (0, 1), (0, 2) tab(col1, col2)
|> AGGREGATE COUNT(col2) AS count GROUP BY col1;

+----+-----+
|col1|count|
+----+-----+
|   0|    2|
+----+-----+
```

#### JOIN

```sql
|> [LEFT | RIGHT | FULL | CROSS | SEMI | ANTI | NATURAL | LATERAL] JOIN <table> [ON <condition> | USING(col, ...)]
```

Joins rows from both inputs, returning a filtered cross-product of the pipe input table and the table expression following the JOIN keyword.

For example:

```sql
VALUES (0, 1) tab(a, b)
|> JOIN VALUES (0, 2) tab(c, d) ON a = c;

+---+---+---+---+
|  a|  b|  c|  d|
+---+---+---+---+
|  0|  1|  0|  2|
+---+---+---+---+
```

#### ORDER BY

```sql
|> ORDER BY <expr> [ASC | DESC], ...
```

Returns the input rows after sorting as indicated. Standard modifiers are supported including NULLS FIRST/LAST.

For example:

```sql
VALUES (0), (1) tab(col)
|> ORDER BY col DESC;

+---+
|col|
+---+
|  1|
|  0|
+---+
```

#### UNION, INTERSECT, EXCEPT

```sql
|> {UNION | INTERSECT | EXCEPT} {ALL | DISTINCT} (<query>), (<query>), ...
```

Performs the union or other set operation over the combined rows from the input table plus one or more tables provided as input arguments.

For example:

```sql
VALUES (0), (1) tab(a, b)
|> UNION ALL VALUES (2), (3) tab(c, d);

+---+----+
|  a|   b|
+---+----+
|  0|   1|
|  2|   3|
+---+----+
```

#### TABLESAMPLE

```sql
|> TABLESAMPLE <method>(<size> {ROWS | PERCENT})
```

Returns the subset of rows chosen by the provided sampling algorithm.

For example:

```sql
VALUES (0), (0), (0), (0) tab(col)
|> TABLESAMPLE BERNOULLI(1 ROWS);

+---+
|col|
+---+
|  0|
+---+
```

#### PIVOT

```sql
|> PIVOT (agg_expr FOR col IN (val1, ...))
```

Returns a new table with the input rows pivoted to become columns.

For example:

```sql
VALUES
  ("dotNET", 2012, 10000),
  ("Java", 2012, 20000),
  ("dotNET", 2012, 5000),
  ("dotNET", 2013, 48000),
  ("Java", 2013, 30000)
  courseSales(course, year, earnings)
|> PIVOT (
     SUM(earnings)
     FOR COURSE IN ('dotNET', 'Java')
  )

+----+------+------+
|year|dotNET|  Java|
+----+------+------+
|2012| 15000| 20000|
|2013| 48000| 30000|
+----+------+------+
```

#### UNPIVOT

```sql
|> UNPIVOT (value_col FOR key_col IN (col1, ...))
```

Returns a new table with the input columns pivoted to become rows.

For example:

```sql
VALUES
  ("dotNET", 2012, 10000),
  ("Java", 2012, 20000),
  ("dotNET", 2012, 5000),
  ("dotNET", 2013, 48000),
  ("Java", 2013, 30000)
  courseSales(course, year, earnings)
|> UNPIVOT (
  earningsYear FOR `year` IN (`2012`, `2013`, `2014`)

+--------+------+--------+
|  course|  year|earnings|
+--------+------+--------+
|    Java|  2012|   20000|
|    Java|  2013|   30000|
|  dotNET|  2012|   15000|
|  dotNET|  2013|   48000|
|  dotNET|  2014|   22500|
+--------+------+--------+
```

