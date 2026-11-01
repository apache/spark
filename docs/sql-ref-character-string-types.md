---
layout: global
title: Character String Types
displayTitle: Character String Types (Preview)
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

First-class `CHAR` / `VARCHAR` / `STRING` semantics are a preview feature in Spark 4.4 and may
change in future releases. They are off by default. This page describes that preview mode only.

Enable the preview feature before using the behavior below:

```sql
SET spark.sql.charVarchar.standardSemantics.enabled = true;
```

By default, `CHAR` and `VARCHAR` are table-schema overlays on `STRING`. DataFrame schemas
show `STRING`, `CAST(... AS CHAR(n))` is `STRING`, and function schemas raise
`UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING`. See [Data Types](sql-ref-datatypes.html).

With the preview enabled, Spark SQL has a family of three character string types:

```text
CHAR(n) -> VARCHAR(n) -> STRING
```

The arrow denotes type precedence from the most constrained type to the least constrained type.
This is analogous to the precedence of integral numeric types such as
`TINYINT -> SMALLINT -> INT -> BIGINT`. `CHAR`, `VARCHAR`, and `STRING` are distinct SQL types in
the same family; `CHAR` and `VARCHAR` are not special uses of `STRING`.

## Related settings

The preview flag is persisted with view definitions for analysis of the view body, so a view
continues to use the semantics under which it was created. `from_json` and `from_avro` capture
the flag at analysis. JSON file scans and `from_xml` read the executing session's flag.

The older `spark.sql.preserveCharVarcharTypeInfo` setting is an experimental compatibility mode.
It preserves type information, but does not apply all the type precedence and expression-result
rules described here. Standard semantics take precedence over
`spark.sql.legacy.charVarcharAsString` when both settings are `true`.

Two other settings interact with standard semantics:

* `spark.sql.charAsVarchar` still rewrites `CHAR` to `VARCHAR` in `CREATE`/`REPLACE`/`ALTER TABLE`.
  `CAST(... AS CHAR(n))` stays `CHAR(n)`.
* `spark.sql.readSideCharPadding` is ignored. Read-side CHAR padding and length checks always run;
  setting it to `false` logs a warning.

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
and collection constructors. The least common type is computed across all inputs together.
Adding a `VARCHAR` input can change equality: `CHAR` vs `CHAR` pads, but widening to
`VARCHAR` keeps each original pad.

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
-- string collate UTF8_BINARY

SELECT CAST('a' AS CHAR(2)) IN (CAST('a' AS CHAR(4)));
-- true

SELECT CAST('a' AS CHAR(2)) IN (
  CAST('a' AS CHAR(4)), CAST('b' AS VARCHAR(3)));
-- false  (all widen to VARCHAR(4): 'a ' vs 'a   ')
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
* Casting to `VARCHAR(n)` truncates values longer than `n` characters. Spaces already present in
  a `CHAR` value are kept, so `CAST(CAST('ab' AS CHAR(4)) AS VARCHAR(4))` is `'ab  '`.

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
arguments and return values, session variables, schema-driven Dataset or DataFrame conversion, and
the output columns of a script `TRANSFORM` clause.

* A value that fits is stored unchanged, except that `CHAR(n)` is padded on the right to `n`
  characters.
* If only trailing spaces exceed the declared length, Spark removes only the number of trailing
  spaces needed to fit.
* If non-space characters exceed the declared length, Spark raises `EXCEED_LIMIT_LENGTH`.

Reads apply the same checks. In particular, a `CHAR(n)` value read from external or otherwise
untrusted storage is padded if short and rejected if its non-space content is too long. The same
`EXCEED_LIMIT_LENGTH` check applies to oversize partition values.

```sql
CREATE TABLE contacts (code CHAR(4), name VARCHAR(10)) USING parquet;

INSERT INTO contacts VALUES ('ab', 'Ada');

SELECT concat('<', code, '>'), name FROM contacts;
-- <ab  >  Ada
```

A script `TRANSFORM` clause (including `MAP` and `REDUCE`) exchanges rows with the external script
as unbounded `STRING`, so a `CHAR(n)` or `VARCHAR(n)` in its `AS` output list does not constrain
what the script emits. Spark instead applies the assignment above to the script output in a
projection: a declared `CHAR(n)` is padded and an over-length value raises `EXCEED_LIMIT_LENGTH`.
The declared type is kept only when these semantics (or `spark.sql.preserveCharVarcharTypeInfo`)
are enabled; otherwise an `AS` type such as `CHAR(n)` is treated as plain `STRING` with no padding
or length check.

## Expression result types

A reference to a `CHAR` or `VARCHAR` column preserves its declared type. Expressions that select
among values without changing their character content, such as `CASE` and `COALESCE`, use the
least common type rules above.

An expression that transforms character content returns `STRING`, because its result length is
not constrained by an input declaration. This includes string functions and operators such as
`upper`, `lower`, `substring`, `concat`, `||`, `regexp_replace`, `trim`, and `mask`.
`split` returns `ARRAY<STRING>`.

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

A [string literal](sql-ref-literals.html#string-literal) is `STRING`, not `CHAR` or `VARCHAR`.
Comparing it with `CHAR` is the same widening: the `CHAR` pad is kept, and the comparison is
exact.

```sql
SELECT CAST('a' AS CHAR(2)) = 'a';
-- false

SELECT CAST('a' AS CHAR(2)) = 'a ';
-- true
```

The same widening applies to a `CHAR` table column. By default the column is typed `STRING`, so
Spark pads the literal to the column length and `WHERE code = 'ab'` matches. Under the preview
the column is `CHAR(n)`, the comparison casts the column to `STRING`, and the literal is not
padded, so a stored `'ab  '` does not match `'ab'`. The same applies to `IN ('ab', ...)` and
to predicates on CHAR partition columns.

```sql
SELECT name FROM contacts WHERE code = 'ab';
-- no rows

SELECT name FROM contacts WHERE code = 'ab  ';
-- Ada
```

A [parameter marker](sql-ref-parameter-markers.html) bound to a string value is also `STRING`.
The bound value is not padded unless you cast it to `CHAR`.

```sql
EXECUTE IMMEDIATE 'SELECT CAST(''a'' AS CHAR(2)) = :p' USING 'a' AS p;
-- false

EXECUTE IMMEDIATE 'SELECT CAST(''a'' AS CHAR(2)) = :p' USING 'a ' AS p;
-- true

EXECUTE IMMEDIATE
  'SELECT CAST(''a'' AS CHAR(2)) = CAST(:p AS CHAR(2))' USING 'a' AS p;
-- true
```

Ignoring trailing spaces is a collation property, not an implicit property of every `CHAR`
comparison. Use an `RTRIM` collation when comparisons should ignore trailing spaces:

```sql
SELECT CAST('a' AS CHAR(2) COLLATE UTF8_BINARY_RTRIM) =
       'a' COLLATE UTF8_BINARY_RTRIM;
-- true
```

## Parsing functions and map keys

The functions `from_json`, `from_csv`, `from_xml`, and `from_avro` have two CHAR/VARCHAR surfaces:
the document they parse, and the schema they parse into. File scans use the same schema rules as
the corresponding `from_*` function except where noted.

### Document arguments

`from_json`, `from_csv`, `from_xml`, and `schema_of_json` / `schema_of_csv` / `schema_of_xml` take a
STRING document. A `CHAR` argument is not rtrimmed. Padding is part of the value after CAST to
STRING, so it is parsed as trailing spaces in the document:

```sql
SELECT from_json(CAST('{"a":1}' AS CHAR(12)), 'a INT');
-- same as from_json('{"a":1}     ', 'a INT')
```

JSON and XML ignore that trailing whitespace. CSV can tokenize it: with a space delimiter, padding
becomes extra empty fields; with the default comma delimiter, padding lands on the last unquoted
field. When CHAR is first-class, `schema_of_csv(CAST('1' AS CHAR(3)))` infers
`STRUCT<_c0: DOUBLE>` because the padded last field is not a clean integer. Under default
settings that CAST is STRING and the inferred field is INT.
`map('ignoreTrailingWhiteSpace', 'true')` restores a trimmed last-field parse.

`from_avro` takes binary Avro, not a character document, so CHAR padding of a SQL string does not
apply there.

### Parsed values

With a reader schema that declares `CHAR`/`VARCHAR` fields, parsed string values are assigned
when CHAR/VARCHAR are first-class (this preview flag or
`spark.sql.preserveCharVarcharTypeInfo`): `CHAR(n)` is padded, and an over-length value is a
bad record. For `from_json`, `from_csv`, and `from_xml`, `FAILFAST` fails with
`MALFORMED_RECORD_IN_PARSING` caused by `EXCEED_LIMIT_LENGTH`, and `PERMISSIVE` nulls the
failed field. CSV field overflow follows the CSV parse mode the same way.

`from_avro` assigns STRING values only. Avro ENUM symbols are not padded or length-checked, so a
padded CHAR cannot invent a symbol. Overflow is a malformed record handled by the `mode`
option: `FAILFAST` fails with `EXCEED_LIMIT_LENGTH` as the cause; `PERMISSIVE` returns a
struct with every field NULL (NULL for a non-struct schema). Avro file scans leave CHAR
padding of STRING values to read-side CHAR padding rather than assigning in the deserializer.
`to_json`, `to_csv`, `to_xml`, and `to_avro` serialize the current string content; `to_avro`
does not itself pad or trim.

### Map keys

A `CHAR(n)` or `VARCHAR(n)` map key is length-checked but never padded or trimmed: a `CHAR(n)` key
must already be exactly `n` characters, and a `VARCHAR(n)` key at most `n`. Padding or trimming a
key would change its identity and could silently merge distinct keys.

* JSON (`from_json` and JSON scans): `UNSUPPORTED_JSON_CHAR_VARCHAR_MAP_KEY` (SQLSTATE `0A000`).
  `from_json` captures the standard-semantics flag at analysis, so a view keeps the check from
  when it was created. JSON file scans, including scans inside persisted views, read the flag
  from the executing session. Duplicate names are kept, exactly as for STRING keys;
  `spark.sql.mapKeyDedupPolicy` is not applied at parse time. Keys rebuilt read-side on a file
  scan can still raise `DUPLICATED_MAP_KEY`. `from_json(to_json(m))` is not folded away when `m`
  has a CHAR/VARCHAR map key.
* XML (`from_xml` and XML scans): `UNSUPPORTED_XML_CHAR_VARCHAR_MAP_KEY`. Checked names are
  element names, `attributePrefix` plus attribute names (default prefix `_`), and the `valueTag`
  (default `_VALUE`) when mixed text is present. Exact repeated names last-win.
  `spark.sql.mapKeyDedupPolicy` is not applied. Empty, attribute-only, and text-only map elements
  stay SQL NULL or a malformed record; they do not become an empty map or a `valueTag` entry.
  Unlike `from_json`, `from_xml` reads the executing session's flag, so a persisted view created
  with the flag on uses pad-and-`EXCEED_LIMIT_LENGTH` for a caller with the flag off. When
  standard semantics are off and `spark.sql.preserveCharVarcharTypeInfo` is on, short CHAR
  keys are padded and over-length keys raise `EXCEED_LIMIT_LENGTH`.
* Avro (`from_avro` and Avro scans): `UNSUPPORTED_AVRO_CHAR_VARCHAR_MAP_KEY`. `from_avro`
  PERMISSIVE returns a struct with every field NULL (NULL for a non-struct schema); Avro file
  scans do not wrap per-row PERMISSIVE around the error.
* CSV has no object-name map keys; `from_csv` schemas are structs of fields.

For JSON and XML, `PERMISSIVE` sets the nearest enclosing field to `null`; `FAILFAST` fails
with the error above as the cause of `MALFORMED_RECORD_IN_PARSING`. `from_avro` FAILFAST
throws a malformed-record `SparkException` with no error class. `variant_get`,
`to_variant_object`, and `CAST(... AS VARIANT)` reject CHAR/VARCHAR anywhere in the type,
not only as map keys.

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
| Parquet | Stores logical type metadata so file-only schema inference can restore `CHAR` and `VARCHAR`, same as ORC. |
| ORC | Stores logical type metadata on physical string fields so schema inference can restore `CHAR` and `VARCHAR`. |
| Avro | Stores logical type metadata on string fields and map keys so schema inference can restore `CHAR` and `VARCHAR`. |
| JSON, CSV, and XML | Apply `CHAR` and `VARCHAR` semantics when the reader is given an explicit schema; text inference alone does not infer a length constraint. See [Parsing functions and map keys](#parsing-functions-and-map-keys) for map-key handling. CSV overflow is a bad record under the CSV parse mode. |

By default, when CHAR/VARCHAR are not first-class, file-only inference reads the constrained
fields as `STRING`.

### Client metadata

Spark Connect and the Spark Thrift Server expose the declared character string type and length to
clients. JDBC metadata reports `java.sql.Types.CHAR` or `java.sql.Types.VARCHAR`, the declared
length as the precision, and four times the declared length as `CHAR_OCTET_LENGTH`, reflecting the
maximum UTF-8 bytes per character. Spark Connect `collect()` returns CHAR/VARCHAR values.
Classic `toPandas()` and `createDataFrame` from pandas fall back off Arrow by default when
Python's `CharType` cannot convert; Python UDF return types, UDAF buffer schemas, and UDTF
return types raise `CHAR_VARCHAR_NOT_SUPPORTED_IN_PYTHON`.
