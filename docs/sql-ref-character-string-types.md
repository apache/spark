---
layout: global
title: Character String Types
displayTitle: Character String Types
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

Spark SQL has a family of three character string types:

```text
CHAR(n) -> VARCHAR(n) -> STRING
```

The arrow denotes type precedence from the most constrained type to the least constrained type.
This is analogous to the precedence of integral numeric types such as
`TINYINT -> SMALLINT -> INT -> BIGINT`. `CHAR`, `VARCHAR`, and `STRING` are distinct SQL types in
the same family; `CHAR` and `VARCHAR` are not special uses of `STRING`.

## Enable character string type semantics

Set `spark.sql.charVarchar.standardSemantics.enabled` to `true` to use the semantics on this page:

```sql
SET spark.sql.charVarchar.standardSemantics.enabled = true;
```

The setting is `false` by default in Spark 4.4. When it is enabled, `CHAR` and `VARCHAR` remain
first-class types in schemas and query plans, explicit casts can produce them, and type coercion
uses the character string type family. This is an opt-in semantic change that can change expression
types and query results. The setting is persisted with view definitions, so a view continues to
use the semantics under which it was created.

The older `spark.sql.preserveCharVarcharTypeInfo` setting is an experimental compatibility mode.
It preserves type information, but does not apply all the type precedence and expression-result
rules described here. Standard semantics take precedence over
`spark.sql.legacy.charVarcharAsString` when both settings are `true`.

## Type definitions

| SQL type | Value constraint | Behavior |
|----------|------------------|----------|
| `CHAR(n)` | Exactly `n` characters | Values shorter than `n` characters are padded on the right with spaces. |
| `VARCHAR(n)` | At most `n` characters | Values retain their length and are not padded. |
| `STRING` | Unbounded length | Values have no declared length constraint. |

The length is required for `CHAR` and `VARCHAR`. `CHARACTER(n)` is a synonym for `CHAR(n)`.
The length `n` counts characters, not UTF-8 bytes. All three types can have a
[collation](sql-ref-syntax-aux-show-collations.html), which controls comparison, ordering, and
other collation-sensitive operations.

For example:

```sql
SELECT typeof(CAST('ab' AS CHAR(5)));
-- char(5)

SELECT concat('<', CAST('ab' AS CHAR(5)), '>');
-- <ab   >

SELECT typeof(CAST('ab' AS VARCHAR(5)));
-- varchar(5)
```

## Type precedence and least common type

Spark finds the least common type when an operation needs one type that can represent multiple
inputs. Within the character string family, Spark uses these rules:

| Input types | Least common type |
|-------------|-------------------|
| `CHAR(n)`, `CHAR(m)` | `CHAR(max(n, m))` |
| `CHAR(n)`, `VARCHAR(m)` | `VARCHAR(max(n, m))` |
| `VARCHAR(n)`, `VARCHAR(m)` | `VARCHAR(max(n, m))` |
| Any character string type and `STRING` | `STRING` |
| Any character string type `T` and untyped `NULL` | `T` |

These rules apply recursively to arrays, maps, and structs. They are used by expressions and
operations such as `CASE`, `COALESCE`, `IN`, `UNION`, `INTERSECT`, `EXCEPT`, multi-row `VALUES`,
and collection constructors.

```sql
SELECT typeof(coalesce(
  CAST('a' AS CHAR(2)),
  CAST('bb' AS CHAR(4))));
-- char(4)

SELECT typeof(coalesce(
  CAST('a' AS CHAR(2)),
  CAST('bb' AS VARCHAR(4))));
-- varchar(4)

SELECT typeof(coalesce(
  CAST('a' AS VARCHAR(4)),
  CAST('unbounded' AS STRING)));
-- string
```

Collation is also part of a character string type. The least-common-type rules above apply after
Spark resolves the input collations. When the result remains `CHAR` or `VARCHAR`, it retains the
resolved collation.

## Casts

An explicit `CAST` or `TRY_CAST` can introduce a `CHAR` or `VARCHAR` value:

```sql
SELECT CAST('abc' AS CHAR(5));
-- 'abc  '

SELECT CAST('abc' AS VARCHAR(5));
-- 'abc'
```

For an explicit cast from one character string type to another:

* Casting to `CHAR(n)` truncates values longer than `n` characters and pads values shorter than
  `n` characters.
* Casting to `VARCHAR(n)` truncates values longer than `n` characters.

For a cast from a non-character value, Spark first converts the value to characters and then
checks the target length. `CAST` raises `EXCEED_LIMIT_LENGTH` if the value does not fit, while
`TRY_CAST` returns `NULL`.

```sql
SELECT CAST('abcdef' AS VARCHAR(3));
-- 'abc'

SELECT TRY_CAST(12345 AS VARCHAR(4));
-- NULL
```

## Assignment and reading

Assignment to a declared `CHAR` or `VARCHAR` type is stricter than an explicit
character-to-character cast. Assignment occurs for operations such as table writes, routine
arguments and return values, session variables, and schema-driven Dataset or DataFrame conversion.

* A value that fits is stored unchanged, except that `CHAR(n)` is padded on the right to `n`
  characters.
* If only trailing spaces exceed the declared length, Spark removes only the number of trailing
  spaces needed to fit.
* If non-space characters exceed the declared length, Spark raises `EXCEED_LIMIT_LENGTH`.

Reads apply the same checks. In particular, a `CHAR(n)` value read from external or otherwise
untrusted storage is padded if short and rejected if its non-space content is too long.

```sql
CREATE TABLE contacts (code CHAR(4), name VARCHAR(10)) USING parquet;

INSERT INTO contacts VALUES ('ab', 'Ada');

SELECT concat('<', code, '>'), name FROM contacts;
-- <ab  >  Ada
```

## Expression result types

A reference to a `CHAR` or `VARCHAR` column preserves its declared type. Expressions that select
among values without changing their character content, such as `CASE` and `COALESCE`, use the
least common type rules above.

An expression that transforms character content returns `STRING`, because its result length is
not constrained by an input declaration. This includes string functions and operators such as
`upper`, `lower`, `substring`, `concat`, `||`, `regexp_replace`, `trim`, `split`, and `mask`.

```sql
SELECT typeof(upper(CAST('ab' AS CHAR(2))));
-- string

SELECT typeof(CAST('a' AS CHAR(1)) || CAST('b' AS VARCHAR(1)));
-- string
```

This rule prevents a length constraint from propagating to newly computed content. To constrain
the result, cast it explicitly to `CHAR(n)` or `VARCHAR(n)`.

## Comparisons and trailing spaces

Spark coerces character string operands to a least common type before comparing them. Casting to a
wider `CHAR` pads the value, while casting to `VARCHAR` or `STRING` preserves spaces already
present in a `CHAR` value.

```sql
SELECT CAST('a' AS CHAR(2)) = CAST('a' AS CHAR(4));
-- true

SELECT CAST('a' AS CHAR(2)) = CAST('a' AS VARCHAR(2));
-- false

SELECT CAST('a' AS CHAR(2)) = CAST('a ' AS VARCHAR(2));
-- true
```

Ignoring trailing spaces is a collation property, not an implicit property of every `CHAR`
comparison. Use an `RTRIM` collation when comparisons should ignore trailing spaces:

```sql
SELECT CAST('a' AS CHAR(2) COLLATE UTF8_BINARY_RTRIM) =
       'a' COLLATE UTF8_BINARY_RTRIM;
-- true
```

## Schema propagation

With standard semantics enabled, declared character string types remain visible through Spark SQL
and DataFrame schemas. This includes:

* table, view, and common table expression columns;
* `CREATE TABLE AS SELECT` results;
* SQL routine parameters and return types;
* session and scripting variables;
* nested array, map, and struct fields; and
* schemas supplied to Dataset and DataFrame APIs.

Storage formats and catalogs differ in how they persist logical type metadata. Whenever a schema
declares `CHAR(n)` or `VARCHAR(n)`, Spark applies the length, padding, assignment, and read checks
described on this page.

### Data source behavior

Catalog tables retain `CHAR` and `VARCHAR` declarations in their catalog schema. For file-only
schema inference, support depends on the format:

| Data source | Behavior |
|-------------|----------|
| Parquet catalog table | Uses the catalog schema to retain the declared type. |
| ORC | Stores logical type metadata on physical string fields so schema inference can restore `CHAR` and `VARCHAR`. |
| Avro | Stores logical type metadata on string fields and map keys so schema inference can restore `CHAR` and `VARCHAR`. |
| JSON and CSV | Apply `CHAR` and `VARCHAR` semantics when the reader is given an explicit schema; text inference alone does not infer a length constraint. |

When standard semantics are disabled, file-only inference reads the constrained fields as
`STRING`.

### Client metadata

Spark Connect and the Spark Thrift Server expose the declared character string type and length to
clients. JDBC metadata reports `java.sql.Types.CHAR` or `java.sql.Types.VARCHAR`, the declared
length as the precision, and four times the declared length as `CHAR_OCTET_LENGTH`, reflecting the
maximum UTF-8 bytes per character.
