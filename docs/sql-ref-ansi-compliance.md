---
layout: global
title: ANSI Compliance
displayTitle: ANSI Compliance
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

In Spark SQL, there are two options to comply with the SQL standard: `spark.sql.ansi.enabled` and `spark.sql.storeAssignmentPolicy` (See a table below for details).

By default, `spark.sql.ansi.enabled` is `true` and Spark SQL uses an ANSI compliant dialect instead of being Hive compliant. For example, Spark will throw an exception at runtime instead of returning null results if the inputs to a SQL operator/function are invalid. Some ANSI dialect features may be not from the ANSI SQL standard directly, but their behaviors align with ANSI SQL's style.

Moreover, Spark SQL has an independent option to control implicit casting behaviours when inserting rows in a table.
The casting behaviours are defined as store assignment rules in the standard.

By default, `spark.sql.storeAssignmentPolicy` is `ANSI` and Spark SQL complies with the ANSI store assignment rules.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.sql.ansi.enabled</code></td>
  <td>true</td>
  <td>
    When true, Spark tries to conform to the ANSI SQL specification: <br/>
    1. Spark SQL will throw runtime exceptions on invalid operations, including integer overflow
    errors, string parsing errors, etc. <br/>
    2. Spark will use different type coercion rules for resolving conflicts among data types.
    The rules are consistently based on data type precedence.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.sql.storeAssignmentPolicy</code></td>
  <td>ANSI</td>
  <td>
    When inserting a value into a column with different data type, Spark will perform type
    conversion. Currently, we support 3 policies for the type coercion rules: ANSI, legacy and
    strict.<br/>
    1. With ANSI policy, Spark performs the type coercion as per ANSI SQL. In practice, the behavior
    is mostly the same as PostgreSQL. It disallows certain unreasonable type conversions such as
    converting string to int or double to boolean. On inserting a numeric type column, an overflow
    error will be thrown if the value is out of the target data type's range.<br/>
    2. With legacy policy, Spark allows the type coercion as long as it is a valid Cast, which is
    very loose. e.g. converting string to int or double to boolean is allowed. It is also the only
    behavior in Spark 2.x and it is compatible with Hive.<br/>
    3. With strict policy, Spark doesn't allow any possible precision loss or data truncation in
    type coercion, e.g. converting double to int or decimal to double is not allowed.
  </td>
  <td>3.0.0</td>
</tr>
</table>

The following subsections present behaviour changes in arithmetic operations, type conversions, and SQL parsing when the ANSI mode enabled. For type conversions in Spark SQL, there are three kinds of them and this article will introduce them one by one: cast, store assignment and type coercion. 

### Arithmetic Operations

In Spark SQL, by default, Spark throws an arithmetic exception at runtime for both interval and numeric type overflows.
If `spark.sql.ansi.enabled` is `false`, then the decimal type will produce `null` values and other numeric types will behave in the same way as the corresponding operation in a Java/Scala program (e.g., if the sum of 2 integers is higher than the maximum value representable, the result is a negative number) which is the behavior of Spark 3 or older.

```sql
-- `spark.sql.ansi.enabled=true`
SELECT 2147483647 + 1;
org.apache.spark.SparkArithmeticException: [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and return NULL instead. If necessary set spark.sql.ansi.enabled to "false" to bypass this error.
== SQL(line 1, position 8) ==
SELECT 2147483647 + 1
       ^^^^^^^^^^^^^^

SELECT abs(-2147483648);
org.apache.spark.SparkArithmeticException: [ARITHMETIC_OVERFLOW] integer overflow. If necessary set spark.sql.ansi.enabled to "false" to bypass this error.

-- `spark.sql.ansi.enabled=false`
SELECT 2147483647 + 1;
+----------------+
|(2147483647 + 1)|
+----------------+
|     -2147483648|
+----------------+

SELECT abs(-2147483648);
+----------------+
|abs(-2147483648)|
+----------------+
|     -2147483648|
+----------------+
```

### Cast

When `spark.sql.ansi.enabled` is set to `true`, explicit casting by `CAST` syntax throws a runtime exception for illegal cast patterns defined in the standard, e.g. casts from a string to an integer.

Besides, the ANSI SQL mode disallows the following type conversions which are allowed when ANSI mode is off:
* Numeric <=> Binary
* Date <=> Boolean
* Timestamp <=> Boolean
* Date => Numeric

 The valid combinations of source and target data type in a `CAST` expression are given by the following table.
“Y” indicates that the combination is syntactically valid without restriction and “N” indicates that the combination is not valid.

| Source\Target | Numeric                              | String                               | Date                                 | Timestamp                            | Timestamp_NTZ                        | Interval                             | Boolean                              | Binary | Array                                | Map                                  | Struct                               |
|---------------|--------------------------------------|--------------------------------------|--------------------------------------|--------------------------------------|--------------------------------------|--------------------------------------|--------------------------------------|--------|--------------------------------------|--------------------------------------|--------------------------------------|
| Numeric       | <span style="color:red">**Y**</span> | <span style="color:red">**Y**</span> | N                                    | <span style="color:red">**Y**</span> | N                                    | <span style="color:red">**Y**</span> | Y                                    | N      | N                                    | N                                    | N                                    |
| String        | <span style="color:red">**Y**</span> | Y                                    | <span style="color:red">**Y**</span> | <span style="color:red">**Y**</span> | <span style="color:red">**Y**</span> | <span style="color:red">**Y**</span> | <span style="color:red">**Y**</span> | Y      | N                                    | N                                    | N                                    |
| Date          | N                                    | Y                                    | Y                                    | Y                                    | Y                                    | N                                    | N                                    | N      | N                                    | N                                    | N                                    |
| Timestamp     | <span style="color:red">**Y**</span> | Y                                    | Y                                    | Y                                    | Y                                    | N                                    | N                                    | N      | N                                    | N                                    | N                                    |
| Timestamp_NTZ | N                                    | Y                                    | Y                                    | Y                                    | Y                                    | N                                    | N                                    | N      | N                                    | N                                    | N                                    |
| Interval      | <span style="color:red">**Y**</span> | Y                                    | N                                    | N                                    | N                                    | Y                                    | N                                    | N      | N                                    | N                                    | N                                    |
| Boolean       | Y                                    | Y                                    | N                                    | N                                    | N                                    | N                                    | Y                                    | N      | N                                    | N                                    | N                                    |
| Binary        | N                                    | Y                                    | N                                    | N                                    | N                                    | N                                    | N                                    | Y      | N                                    | N                                    | N                                    |
| Array         | N                                    | Y                                    | N                                    | N                                    | N                                    | N                                    | N                                    | N      | <span style="color:red">**Y**</span> | N                                    | N                                    |
| Map           | N                                    | Y                                    | N                                    | N                                    | N                                    | N                                    | N                                    | N      | N                                    | <span style="color:red">**Y**</span> | N                                    |
| Struct        | N                                    | Y                                    | N                                    | N                                    | N                                    | N                                    | N                                    | N      | N                                    | N                                    | <span style="color:red">**Y**</span> |

In the table above, all the `CAST`s with new syntax are marked as red <span style="color:red">**Y**</span>:
* CAST(Numeric AS Numeric): raise an overflow exception if the value is out of the target data type's range.
* CAST(String AS (Numeric/Date/Timestamp/Timestamp_NTZ/Interval/Boolean)): raise a runtime exception if the value can't be parsed as the target data type.
* CAST(Timestamp AS Numeric): raise an overflow exception if the number of seconds since epoch is out of the target data type's range.
* CAST(Numeric AS Timestamp): raise an overflow exception if numeric value times 1000000(microseconds per second) is out of the range of Long type. 
* CAST(Array AS Array): raise an exception if there is any on the conversion of the elements.
* CAST(Map AS Map): raise an exception if there is any on the conversion of the keys and the values.
* CAST(Struct AS Struct): raise an exception if there is any on the conversion of the struct fields.
* CAST(Numeric AS String): Always use plain string representation on casting decimal values to strings, instead of using scientific notation if an exponent is needed
* CAST(Interval AS Numeric): raise an overflow exception if the number of microseconds of the day-time interval or months of year-month interval is out of the target data type's range.
* CAST(Numeric AS Interval): raise an overflow exception if numeric value times by the target interval's end-unit is out of the range of the Int type for year-month intervals or the Long type for day-time intervals.

```sql
-- Examples of explicit casting

-- `spark.sql.ansi.enabled=true` (This is a default behaviour)
SELECT CAST('a' AS INT);
org.apache.spark.SparkNumberFormatException: [CAST_INVALID_INPUT] The value 'a' of the type "STRING" cannot be cast to "INT" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead.
== SQL(line 1, position 8) ==
SELECT CAST('a' AS INT)
       ^^^^^^^^^^^^^^^^

SELECT CAST(2147483648L AS INT);
org.apache.spark.SparkArithmeticException: [CAST_OVERFLOW] The value 2147483648L of the type "BIGINT" cannot be cast to "INT" due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead.

SELECT CAST(DATE'2020-01-01' AS INT);
org.apache.spark.sql.AnalysisException: cannot resolve 'CAST(DATE '2020-01-01' AS INT)' due to data type mismatch: cannot cast date to int.
To convert values from date to int, you can use function UNIX_DATE instead.

-- `spark.sql.ansi.enabled=false`
SELECT CAST('a' AS INT);
+--------------+
|CAST(a AS INT)|
+--------------+
|          null|
+--------------+

SELECT CAST(2147483648L AS INT);
+-----------------------+
|CAST(2147483648 AS INT)|
+-----------------------+
|            -2147483648|
+-----------------------+

SELECT CAST(DATE'2020-01-01' AS INT)
+------------------------------+
|CAST(DATE '2020-01-01' AS INT)|
+------------------------------+
|                          null|
+------------------------------+

-- Examples of store assignment rules
CREATE TABLE t (v INT);

-- `spark.sql.storeAssignmentPolicy=ANSI`
INSERT INTO t VALUES ('1');
org.apache.spark.sql.AnalysisException: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for table `spark_catalog`.`default`.`t`: Cannot safely cast `v`: "STRING" to "INT".

-- `spark.sql.storeAssignmentPolicy=LEGACY` (This is a legacy behaviour until Spark 2.x)
INSERT INTO t VALUES ('1');
SELECT * FROM t;
+---+
|  v|
+---+
|  1|
+---+
```

#### Rounding in cast
While casting of a decimal with a fraction to an interval type with SECOND as the end-unit like INTERVAL HOUR TO SECOND, Spark rounds the fractional part towards "nearest neighbor" unless both neighbors are equidistant, in which case round up.

### Store assignment
As mentioned at the beginning, when `spark.sql.storeAssignmentPolicy` is set to `ANSI`(which is the default value), Spark SQL complies with the ANSI store assignment rules on table insertions. The valid combinations of source and target data type in table insertions are given by the following table.

| Source\Target | Numeric | String | Date | Timestamp | Timestamp_NTZ | Interval | Boolean | Binary | Array | Map | Struct |
|:-------------:|:-------:|:------:|:----:|:---------:|---------------|:--------:|:-------:|:------:|:-----:|:---:|:------:|
|    Numeric    |    Y    |   Y    |  N   |     N     | N             |    N     |    N    |   N    |   N   |  N  |   N    |
|    String     |    N    |   Y    |  N   |     N     | N             |    N     |    N    |   N    |   N   |  N  |   N    |
|     Date      |    N    |   Y    |  Y   |     Y     | Y             |    N     |    N    |   N    |   N   |  N  |   N    |
|   Timestamp   |    N    |   Y    |  Y   |     Y     | Y             |    N     |    N    |   N    |   N   |  N  |   N    |
| Timestamp_NTZ |    N    |   Y    |  Y   |     Y     | Y             |    N     |    N    |   N    |   N   |  N  |   N    |
|   Interval    |    N    |   Y    |  N   |     N     | N             |    N*    |    N    |   N    |   N   |  N  |   N    |
|    Boolean    |    N    |   Y    |  N   |     N     | N             |    N     |    Y    |   N    |   N   |  N  |   N    |
|    Binary     |    N    |   Y    |  N   |     N     | N             |    N     |    N    |   Y    |   N   |  N  |   N    |
|     Array     |    N    |   N    |  N   |     N     | N             |    N     |    N    |   N    |  Y**  |  N  |   N    |
|      Map      |    N    |   N    |  N   |     N     | N             |    N     |    N    |   N    |   N   | Y** |   N    |
|    Struct     |    N    |   N    |  N   |     N     | N             |    N     |    N    |   N    |   N   |  N  |  Y**   |

\* Spark doesn't support interval type table column.

\*\* For Array/Map/Struct types, the data type check rule applies recursively to its component elements.

During table insertion, Spark will throw exception on numeric value overflow.
```sql
CREATE TABLE test(i INT);

INSERT INTO test VALUES (2147483648L);
org.apache.spark.SparkArithmeticException: [CAST_OVERFLOW_IN_TABLE_INSERT] Fail to insert a value of "BIGINT" type into the "INT" type column `i` due to an overflow. Use `try_cast` on the input value to tolerate overflow and return NULL instead.
```

### Type coercion
#### Type Promotion and Precedence
When `spark.sql.ansi.enabled` is set to `true`, Spark SQL uses several rules that govern how conflicts between data types are resolved.
At the heart of this conflict resolution is the Type Precedence List which defines whether values of a given data type can be promoted to another data type implicitly.

| Data type | precedence list(from narrowest to widest)                                       |
|-----------|---------------------------------------------------------------------------------|
| Byte      | Byte -> Short -> Int -> Long -> Decimal -> Float* -> Double                     |
| Short     | Short -> Int -> Long -> Decimal-> Float* -> Double                              |
| Int       | Int -> Long -> Decimal -> Float* -> Double                                      |
| Long      | Long -> Decimal -> Float* -> Double                                             |
| Decimal   | Decimal -> Float* -> Double                                                     |
| Float     | Float -> Double                                                                 |
| Double    | Double                                                                          |
| Date      | Date -> Timestamp_NTZ -> Timestamp                                              |
| Timestamp | Timestamp                                                                       |
| String    | String, Long -> Double, Date -> Timestamp_NTZ -> Timestamp , Boolean, Binary ** |
| Binary    | Binary                                                                          |
| Boolean   | Boolean                                                                         |
| Interval  | Interval                                                                        |
| Map       | Map***                                                                          |
| Array     | Array***                                                                        |
| Struct    | Struct***                                                                       |

\* For least common type resolution float is skipped to avoid loss of precision.

\*\* String can be promoted to multiple kinds of data types. Note that Byte/Short/Int/Decimal/Float is not on this precedent list. The least common type between Byte/Short/Int and String is Long, while the least common type between Decimal/Float is Double.

\*\*\* For a complex type, the precedence rule applies recursively to its component elements.

Special rules apply for untyped NULL. A NULL can be promoted to any other type.

This is a graphical depiction of the precedence list as a directed tree:
<img src="img/type-precedence-list.png" width="60%" title="Type Precedence List" alt="Type Precedence List">

#### Least Common Type Resolution
The least common type from a set of types is the narrowest type reachable from the precedence list by all elements of the set of types.

The least common type resolution is used to:
- Derive the argument type for functions which expect a shared argument type for multiple parameters, such as coalesce, least, or greatest.
- Derive the operand types for operators such as arithmetic operations or comparisons.
- Derive the result type for expressions such as the case expression.
- Derive the element, key, or value types for array and map constructors.
Special rules are applied if the least common type resolves to FLOAT. With float type values, if any of the types is INT, BIGINT, or DECIMAL the least common type is pushed to DOUBLE to avoid potential loss of digits.

Decimal type is a bit more complicated here, as it's not a simple type but has parameters: precision and scale.
A `decimal(precision, scale)` means the value can have at most `precision - scale` digits in the integral part and `scale` digits in the fractional part.
A least common type between decimal types should have enough digits in both integral and fractional parts to represent all values.
More precisely, a least common type between `decimal(p1, s1)` and `decimal(p2, s2)` has the scale of `max(s1, s2)` and precision of `max(s1, s2) + max(p1 - s1, p2 - s2)`.
However, decimal types in Spark have a maximum precision: 38. If the final decimal type need more precision, we must do truncation.
Since the digits in the integral part are more significant, Spark truncates the digits in the fractional part first. For example, `decimal(48, 20)` will be reduced to `decimal(38, 10)`.

Note, arithmetic operations have special rules to calculate the least common type for decimal inputs:

| Operation  | Result precision                         | Result scale        |
|------------|------------------------------------------|---------------------|
| e1 + e2    | max(s1, s2) + max(p1 - s1, p2 - s2) + 1  | max(s1, s2)         |
| e1 - e2    | max(s1, s2) + max(p1 - s1, p2 - s2) + 1	| max(s1, s2)         |
| e1 * e2    | p1 + p2 + 1	                        | s1 + s2             |
| e1 / e2    | p1 - s1 + s2 + max(6, s1 + p2 + 1)       | max(6, s1 + p2 + 1) |
| e1 % e2    | min(p1 - s1, p2 - s2) + max(s1, s2)      | max(s1, s2)         |

The truncation rule is also different for arithmetic operations: they retain at least 6 digits in the fractional part, which means we can only reduce `scale` to 6. Overflow may happen in this case.
  
```sql
-- The coalesce function accepts any set of argument types as long as they share a least common type. 
-- The result type is the least common type of the arguments.
> SET spark.sql.ansi.enabled=true;
> SELECT typeof(coalesce(1Y, 1L, NULL));
BIGINT
> SELECT typeof(coalesce(1, DATE'2020-01-01'));
Error: Incompatible types [INT, DATE]

> SELECT typeof(coalesce(ARRAY(1Y), ARRAY(1L)));
ARRAY<BIGINT>
> SELECT typeof(coalesce(1, 1F));
DOUBLE
> SELECT typeof(coalesce(1L, 1F));
DOUBLE
> SELECT (typeof(coalesce(1BD, 1F)));
DOUBLE

> SELECT typeof(coalesce(1, '2147483648'))
BIGINT
> SELECT typeof(coalesce(1.0, '2147483648'))
DOUBLE
> SELECT typeof(coalesce(DATE'2021-01-01', '2022-01-01'))
DATE
```

### SQL Functions
#### Function invocation
Under ANSI mode(spark.sql.ansi.enabled=true), the function invocation of Spark SQL:
- In general, it follows the `Store assignment` rules as storing the input values as the declared parameter type of the SQL functions
- Special rules apply for untyped NULL. A NULL can be promoted to any other type.

```sql
> SET spark.sql.ansi.enabled=true;
-- implicitly cast Int to String type
> SELECT concat('total number: ', 1);
total number: 1
-- implicitly cast Timestamp to Date type
> select datediff(now(), current_date);
0

-- implicitly cast String to Double type
> SELECT ceil('0.1');
1
-- special rule: implicitly cast NULL to Date type
> SELECT year(null);
NULL

> CREATE TABLE t(s string);
-- Can't store String column as Numeric types.
> SELECT ceil(s) from t;
Error in query: cannot resolve 'CEIL(spark_catalog.default.t.s)' due to data type mismatch
-- Can't store String column as Date type.
> select year(s) from t;
Error in query: cannot resolve 'year(spark_catalog.default.t.s)' due to data type mismatch
```

#### Functions with different behaviors
The behavior of some SQL functions can be different under ANSI mode (`spark.sql.ansi.enabled=true`).
  - `size`: This function returns null for null input.
  - `element_at`:
    - This function throws `ArrayIndexOutOfBoundsException` if using invalid indices.
  - `elt`: This function throws `ArrayIndexOutOfBoundsException` if using invalid indices.
  - `parse_url`: This function throws `IllegalArgumentException` if an input string is not a valid url.
  - `to_date`: This function should fail with an exception if the input string can't be parsed, or the pattern string is invalid.
  - `to_timestamp`: This function should fail with an exception if the input string can't be parsed, or the pattern string is invalid.
  - `unix_timestamp`: This function should fail with an exception if the input string can't be parsed, or the pattern string is invalid.
  - `to_unix_timestamp`: This function should fail with an exception if the input string can't be parsed, or the pattern string is invalid.
  - `make_date`: This function should fail with an exception if the result date is invalid.
  - `make_timestamp`: This function should fail with an exception if the result timestamp is invalid.
  - `make_interval`:  This function should fail with an exception if the result interval is invalid.
  - `next_day`: This function throws `IllegalArgumentException` if input is not a valid day of week.

### SQL Operators

The behavior of some SQL operators can be different under ANSI mode (`spark.sql.ansi.enabled=true`).
  - `array_col[index]`: This operator throws `ArrayIndexOutOfBoundsException` if using invalid indices.

### Useful Functions for ANSI Mode

When ANSI mode is on, it throws exceptions for invalid operations. You can use the following SQL functions to suppress such exceptions.
  - `try_cast`: identical to `CAST`, except that it returns `NULL` result instead of throwing an exception on runtime error.
  - `try_add`: identical to the add operator `+`, except that it returns `NULL` result instead of throwing an exception on integral value overflow.
  - `try_subtract`: identical to the add operator `-`, except that it returns `NULL` result instead of throwing an exception on integral value overflow.
  - `try_multiply`: identical to the add operator `*`, except that it returns `NULL` result instead of throwing an exception on integral value overflow.
  - `try_divide`: identical to the division operator `/`, except that it returns `NULL` result instead of throwing an exception on dividing 0.
  - `try_mod`: identical to the remainder operator `%`, except that it returns `NULL` result instead of throwing an exception on dividing 0.
  - `try_sum`: identical to the function `sum`, except that it returns `NULL` result instead of throwing an exception on integral/decimal/interval value overflow.
  - `try_avg`: identical to the function `avg`, except that it returns `NULL` result instead of throwing an exception on decimal/interval value overflow.
  - `try_element_at`: identical to the function `element_at`, except that it returns `NULL` result instead of throwing an exception on array's index out of bound.
  - `try_to_timestamp`: identical to the function `to_timestamp`, except that it returns `NULL` result instead of throwing an exception on string parsing error.
  - `try_parse_url`: identical to the function `parse_url`, except that it returns `NULL` result instead of throwing an exception on url parsing error.
  - `try_make_timestamp`: identical to the function `make_timestamp`, except that it returns `NULL` result instead of throwing an exception on error.
  - `try_make_timestamp_ltz`: identical to the function `make_timestamp_ltz`, except that it returns `NULL` result instead of throwing an exception on error.
  - `try_make_timestamp_ntz`: identical to the function `make_timestamp_ntz`, except that it returns `NULL` result instead of throwing an exception on error.
  - `try_make_interval`: identical to the function `make_interval`, except that it returns `NULL` result instead of throwing an exception on invalid interval.

### SQL Keywords (optional, disabled by default)

When both `spark.sql.ansi.enabled` and `spark.sql.ansi.enforceReservedKeywords` are true, Spark SQL will use the ANSI mode parser.

With the ANSI mode parser, Spark SQL has two kinds of keywords:
* Non-reserved keywords: Keywords that have a special meaning only in particular contexts and can be used as identifiers in other contexts. For example, `EXPLAIN SELECT ...` is a command, but EXPLAIN can be used as identifiers in other places.
* Reserved keywords: Keywords that are reserved and can't be used as identifiers for table, view, column, function, alias, etc.

With the default parser, Spark SQL has two kinds of keywords:
* Non-reserved keywords: Same definition as the one when the ANSI mode enabled.
* Strict-non-reserved keywords: A strict version of non-reserved keywords, which can not be used as table alias.

By default, `spark.sql.ansi.enforceReservedKeywords` is false.

Below is a list of all the keywords in Spark SQL.

|Keyword|Spark SQL<br/>ANSI Mode|Spark SQL<br/>NonANSI Mode|SQL-2016|
|--|----------------------|-------------------------|--------|
|ADD|non-reserved|non-reserved|non-reserved|
|AFTER|non-reserved|non-reserved|non-reserved|
|AGGREGATE|non-reserved|non-reserved|non-reserved|
|ALL|reserved|non-reserved|reserved|
|ALTER|non-reserved|non-reserved|reserved|
|ALWAYS|non-reserved|non-reserved|non-reserved|
|ANALYZE|non-reserved|non-reserved|non-reserved|
|AND|reserved|non-reserved|reserved|
|ANTI|non-reserved|strict-non-reserved|non-reserved|
|ANY|reserved|non-reserved|reserved|
|ANY_VALUE|non-reserved|non-reserved|non-reserved|
|ARCHIVE|non-reserved|non-reserved|non-reserved|
|ARRAY|non-reserved|non-reserved|reserved|
|AS|reserved|non-reserved|reserved|
|ASC|non-reserved|non-reserved|non-reserved|
|AT|non-reserved|non-reserved|reserved|
|AUTHORIZATION|reserved|non-reserved|reserved|
|BEGIN|non-reserved|non-reserved|non-reserved|
|BETWEEN|non-reserved|non-reserved|reserved|
|BIGINT|non-reserved|non-reserved|reserved|
|BINARY|non-reserved|non-reserved|reserved|
|BINDING|non-reserved|non-reserved|non-reserved|
|BOOLEAN|non-reserved|non-reserved|reserved|
|BOTH|reserved|non-reserved|reserved|
|BUCKET|non-reserved|non-reserved|non-reserved|
|BUCKETS|non-reserved|non-reserved|non-reserved|
|BY|non-reserved|non-reserved|reserved|
|BYTE|non-reserved|non-reserved|non-reserved|
|CACHE|non-reserved|non-reserved|non-reserved|
|CALL|reserved|non-reserved|reserved|
|CALLED|non-reserved|non-reserved|non-reserved|
|CASCADE|non-reserved|non-reserved|non-reserved|
|CASE|reserved|non-reserved|reserved|
|CAST|reserved|non-reserved|reserved|
|CATALOG|non-reserved|non-reserved|non-reserved|
|CATALOGS|non-reserved|non-reserved|non-reserved|
|CHANGE|non-reserved|non-reserved|non-reserved|
|CHAR|non-reserved|non-reserved|reserved|
|CHARACTER|non-reserved|non-reserved|reserved|
|CHECK|reserved|non-reserved|reserved|
|CLEAR|non-reserved|non-reserved|non-reserved|
|CLUSTER|non-reserved|non-reserved|non-reserved|
|CLUSTERED|non-reserved|non-reserved|non-reserved|
|CODEGEN|non-reserved|non-reserved|non-reserved|
|COLLATE|reserved|non-reserved|reserved|
|COLLATION|reserved|non-reserved|reserved|
|COLLECTION|non-reserved|non-reserved|non-reserved|
|COLUMN|reserved|non-reserved|reserved|
|COLUMNS|non-reserved|non-reserved|non-reserved|
|COMMENT|non-reserved|non-reserved|non-reserved|
|COMMIT|non-reserved|non-reserved|reserved|
|COMPACT|non-reserved|non-reserved|non-reserved|
|COMPACTIONS|non-reserved|non-reserved|non-reserved|
|COMPENSATION|non-reserved|non-reserved|non-reserved|
|COMPUTE|non-reserved|non-reserved|non-reserved|
|CONCATENATE|non-reserved|non-reserved|non-reserved|
|CONSTRAINT|reserved|non-reserved|reserved|
|CONTAINS|non-reserved|non-reserved|non-reserved|
|COST|non-reserved|non-reserved|non-reserved|
|CREATE|reserved|non-reserved|reserved|
|CROSS|reserved|strict-non-reserved|reserved|
|CUBE|non-reserved|non-reserved|reserved|
|CURRENT|non-reserved|non-reserved|reserved|
|CURRENT_DATE|reserved|non-reserved|reserved|
|CURRENT_TIME|reserved|non-reserved|reserved|
|CURRENT_TIMESTAMP|reserved|non-reserved|reserved|
|CURRENT_USER|reserved|non-reserved|reserved|
|DATA|non-reserved|non-reserved|non-reserved|
|DATE|non-reserved|non-reserved|reserved|
|DATABASE|non-reserved|non-reserved|non-reserved|
|DATABASES|non-reserved|non-reserved|non-reserved|
|DATEADD|non-reserved|non-reserved|non-reserved|
|DATE_ADD|non-reserved|non-reserved|non-reserved|
|DATEDIFF|non-reserved|non-reserved|non-reserved|
|DATE_DIFF|non-reserved|non-reserved|non-reserved|
|DAY|non-reserved|non-reserved|non-reserved|
|DAYS|non-reserved|non-reserved|non-reserved|
|DAYOFYEAR|non-reserved|non-reserved|non-reserved|
|DBPROPERTIES|non-reserved|non-reserved|non-reserved|
|DEC|non-reserved|non-reserved|reserved|
|DECIMAL|non-reserved|non-reserved|reserved|
|DECLARE|non-reserved|non-reserved|non-reserved|
|DEFAULT|non-reserved|non-reserved|non-reserved|
|DEFINED|non-reserved|non-reserved|non-reserved|
|DEFINER|non-reserved|non-reserved|non-reserved|
|DELETE|non-reserved|non-reserved|reserved|
|DELIMITED|non-reserved|non-reserved|non-reserved|
|DESC|non-reserved|non-reserved|non-reserved|
|DESCRIBE|non-reserved|non-reserved|reserved|
|DETERMINISTIC|non-reserved|non-reserved|reserved|
|DFS|non-reserved|non-reserved|non-reserved|
|DIRECTORIES|non-reserved|non-reserved|non-reserved|
|DIRECTORY|non-reserved|non-reserved|non-reserved|
|DISTINCT|reserved|non-reserved|reserved|
|DISTRIBUTE|non-reserved|non-reserved|non-reserved|
|DIV|non-reserved|non-reserved|not a keyword|
|DO|non-reserved|non-reserved|non-reserved|
|DOUBLE|non-reserved|non-reserved|reserved|
|DROP|non-reserved|non-reserved|reserved|
|ELSE|reserved|non-reserved|reserved|
|END|reserved|non-reserved|reserved|
|ESCAPE|reserved|non-reserved|reserved|
|ESCAPED|non-reserved|non-reserved|non-reserved|
|EVOLUTION|non-reserved|non-reserved|non-reserved|
|EXCEPT|reserved|strict-non-reserved|reserved|
|EXCHANGE|non-reserved|non-reserved|non-reserved|
|EXCLUDE|non-reserved|non-reserved|non-reserved|
|EXECUTE|reserved|non-reserved|reserved|
|EXISTS|non-reserved|non-reserved|reserved|
|EXPLAIN|non-reserved|non-reserved|non-reserved|
|EXPORT|non-reserved|non-reserved|non-reserved|
|EXTEND|non-reserved|non-reserved|non-reserved|
|EXTENDED|non-reserved|non-reserved|non-reserved|
|EXTERNAL|non-reserved|non-reserved|reserved|
|EXTRACT|non-reserved|non-reserved|reserved|
|FALSE|reserved|non-reserved|reserved|
|FETCH|reserved|non-reserved|reserved|
|FIELDS|non-reserved|non-reserved|non-reserved|
|FILTER|reserved|non-reserved|reserved|
|FILEFORMAT|non-reserved|non-reserved|non-reserved|
|FIRST|non-reserved|non-reserved|non-reserved|
|FLOAT|non-reserved|non-reserved|reserved|
|FOLLOWING|non-reserved|non-reserved|non-reserved|
|FOR|reserved|non-reserved|reserved|
|FOREIGN|reserved|non-reserved|reserved|
|FORMAT|non-reserved|non-reserved|non-reserved|
|FORMATTED|non-reserved|non-reserved|non-reserved|
|FROM|reserved|non-reserved|reserved|
|FULL|reserved|strict-non-reserved|reserved|
|FUNCTION|non-reserved|non-reserved|reserved|
|FUNCTIONS|non-reserved|non-reserved|non-reserved|
|GENERATED|non-reserved|non-reserved|non-reserved|
|GLOBAL|non-reserved|non-reserved|reserved|
|GRANT|reserved|non-reserved|reserved|
|GROUP|reserved|non-reserved|reserved|
|GROUPING|non-reserved|non-reserved|reserved|
|HAVING|reserved|non-reserved|reserved|
|HOUR|non-reserved|non-reserved|non-reserved|
|HOURS|non-reserved|non-reserved|non-reserved|
|IDENTIFIER|non-reserved|non-reserved|non-reserved|
|IDENTITY|non-reserved|non-reserved|non-reserved|
|IF|non-reserved|non-reserved|not a keyword|
|IGNORE|non-reserved|non-reserved|non-reserved|
|IMMEDIATE|non-reserved|non-reserved|non-reserved|
|IMPORT|non-reserved|non-reserved|non-reserved|
|IN|reserved|non-reserved|reserved|
|INCLUDE|non-reserved|non-reserved|non-reserved|
|INCREMENT|non-reserved|non-reserved|non-reserved|
|INDEX|non-reserved|non-reserved|non-reserved|
|INDEXES|non-reserved|non-reserved|non-reserved|
|INNER|reserved|strict-non-reserved|reserved|
|INPATH|non-reserved|non-reserved|non-reserved|
|INPUT|non-reserved|non-reserved|non-reserved|
|INPUTFORMAT|non-reserved|non-reserved|non-reserved|
|INSERT|non-reserved|non-reserved|reserved|
|INT|non-reserved|non-reserved|reserved|
|INTEGER|non-reserved|non-reserved|reserved|
|INTERSECT|reserved|strict-non-reserved|reserved|
|INTERVAL|non-reserved|non-reserved|reserved|
|INTO|reserved|non-reserved|reserved|
|INVOKER|non-reserved|non-reserved|non-reserved|
|IS|reserved|non-reserved|reserved|
|ITEMS|non-reserved|non-reserved|non-reserved|
|ITERATE|non-reserved|non-reserved|non-reserved|
|JOIN|reserved|strict-non-reserved|reserved|
|KEYS|non-reserved|non-reserved|non-reserved|
|LANGUAGE|non-reserved|non-reserved|reserved|
|LAST|non-reserved|non-reserved|non-reserved|
|LATERAL|reserved|strict-non-reserved|reserved|
|LAZY|non-reserved|non-reserved|non-reserved|
|LEADING|reserved|non-reserved|reserved|
|LEAVE|non-reserved|non-reserved|non-reserved|
|LEFT|reserved|strict-non-reserved|reserved|
|LIKE|non-reserved|non-reserved|reserved|
|ILIKE|non-reserved|non-reserved|non-reserved|
|LIMIT|non-reserved|non-reserved|non-reserved|
|LINES|non-reserved|non-reserved|non-reserved|
|LIST|non-reserved|non-reserved|non-reserved|
|LOAD|non-reserved|non-reserved|non-reserved|
|LOCAL|non-reserved|non-reserved|reserved|
|LOCATION|non-reserved|non-reserved|non-reserved|
|LOCK|non-reserved|non-reserved|non-reserved|
|LOCKS|non-reserved|non-reserved|non-reserved|
|LOGICAL|non-reserved|non-reserved|non-reserved|
|LONG|non-reserved|non-reserved|non-reserved|
|LOOP|non-reserved|non-reserved|non-reserved|
|MACRO|non-reserved|non-reserved|non-reserved|
|MAP|non-reserved|non-reserved|non-reserved|
|MATCHED|non-reserved|non-reserved|non-reserved|
|MERGE|non-reserved|non-reserved|non-reserved|
|MICROSECOND|non-reserved|non-reserved|non-reserved|
|MICROSECONDS|non-reserved|non-reserved|non-reserved|
|MILLISECOND|non-reserved|non-reserved|non-reserved|
|MILLISECONDS|non-reserved|non-reserved|non-reserved|
|MINUTE|non-reserved|non-reserved|non-reserved|
|MINUTES|non-reserved|non-reserved|non-reserved|
|MINUS|non-reserved|strict-non-reserved|non-reserved|
|MODIFIES|non-reserved|non-reserved|non-reserved|
|MONTH|non-reserved|non-reserved|non-reserved|
|MONTHS|non-reserved|non-reserved|non-reserved|
|MSCK|non-reserved|non-reserved|non-reserved|
|NAME|non-reserved|non-reserved|non-reserved|
|NAMESPACE|non-reserved|non-reserved|non-reserved|
|NAMESPACES|non-reserved|non-reserved|non-reserved|
|NANOSECOND|non-reserved|non-reserved|non-reserved|
|NANOSECONDS|non-reserved|non-reserved|non-reserved|
|NATURAL|reserved|strict-non-reserved|reserved|
|NO|non-reserved|non-reserved|reserved|
|NONE|non-reserved|non-reserved|reserved|
|NOT|reserved|non-reserved|reserved|
|NULL|reserved|non-reserved|reserved|
|NULLS|non-reserved|non-reserved|non-reserved|
|NUMERIC|non-reserved|non-reserved|non-reserved|
|OF|non-reserved|non-reserved|reserved|
|OFFSET|reserved|non-reserved|reserved|
|ON|reserved|strict-non-reserved|reserved|
|ONLY|reserved|non-reserved|reserved|
|OPTION|non-reserved|non-reserved|non-reserved|
|OPTIONS|non-reserved|non-reserved|non-reserved|
|OR|reserved|non-reserved|reserved|
|ORDER|reserved|non-reserved|reserved|
|OUT|non-reserved|non-reserved|reserved|
|OUTER|reserved|non-reserved|reserved|
|OUTPUTFORMAT|non-reserved|non-reserved|non-reserved|
|OVER|non-reserved|non-reserved|non-reserved|
|OVERLAPS|reserved|non-reserved|reserved|
|OVERLAY|non-reserved|non-reserved|non-reserved|
|OVERWRITE|non-reserved|non-reserved|non-reserved|
|PARTITION|non-reserved|non-reserved|reserved|
|PARTITIONED|non-reserved|non-reserved|non-reserved|
|PARTITIONS|non-reserved|non-reserved|non-reserved|
|PERCENT|non-reserved|non-reserved|non-reserved|
|PIVOT|non-reserved|non-reserved|non-reserved|
|PLACING|non-reserved|non-reserved|non-reserved|
|POSITION|non-reserved|non-reserved|reserved|
|PRECEDING|non-reserved|non-reserved|non-reserved|
|PRIMARY|reserved|non-reserved|reserved|
|PRINCIPALS|non-reserved|non-reserved|non-reserved|
|PROPERTIES|non-reserved|non-reserved|non-reserved|
|PURGE|non-reserved|non-reserved|non-reserved|
|QUARTER|non-reserved|non-reserved|non-reserved|
|QUERY|non-reserved|non-reserved|non-reserved|
|RANGE|non-reserved|non-reserved|reserved|
|READS|non-reserved|non-reserved|non-reserved|
|REAL|non-reserved|non-reserved|reserved|
|RECORDREADER|non-reserved|non-reserved|non-reserved|
|RECORDWRITER|non-reserved|non-reserved|non-reserved|
|RECOVER|non-reserved|non-reserved|non-reserved|
|REDUCE|non-reserved|non-reserved|non-reserved|
|REFERENCES|reserved|non-reserved|reserved|
|REFRESH|non-reserved|non-reserved|non-reserved|
|REGEXP|non-reserved|non-reserved|not a keyword|
|RENAME|non-reserved|non-reserved|non-reserved|
|REPAIR|non-reserved|non-reserved|non-reserved|
|REPEAT|non-reserved|non-reserved|non-reserved|
|REPEATABLE|non-reserved|non-reserved|non-reserved|
|REPLACE|non-reserved|non-reserved|non-reserved|
|RESET|non-reserved|non-reserved|non-reserved|
|RESPECT|non-reserved|non-reserved|non-reserved|
|RESTRICT|non-reserved|non-reserved|non-reserved|
|RETURN|non-reserved|non-reserved|reserved|
|RETURNS|non-reserved|non-reserved|reserved|
|REVOKE|non-reserved|non-reserved|reserved|
|RIGHT|reserved|strict-non-reserved|reserved|
|RLIKE|non-reserved|non-reserved|non-reserved|
|ROLE|non-reserved|non-reserved|non-reserved|
|ROLES|non-reserved|non-reserved|non-reserved|
|ROLLBACK|non-reserved|non-reserved|reserved|
|ROLLUP|non-reserved|non-reserved|reserved|
|ROW|non-reserved|non-reserved|reserved|
|ROWS|non-reserved|non-reserved|reserved|
|SCHEMA|non-reserved|non-reserved|non-reserved|
|SCHEMAS|non-reserved|non-reserved|non-reserved|
|SECOND|non-reserved|non-reserved|non-reserved|
|SECONDS|non-reserved|non-reserved|non-reserved|
|SECURITY|non-reserved|non-reserved|non-reserved|
|SELECT|reserved|non-reserved|reserved|
|SEMI|non-reserved|strict-non-reserved|non-reserved|
|SEPARATED|non-reserved|non-reserved|non-reserved|
|SERDE|non-reserved|non-reserved|non-reserved|
|SERDEPROPERTIES|non-reserved|non-reserved|non-reserved|
|SESSION_USER|reserved|non-reserved|reserved|
|SET|non-reserved|non-reserved|reserved|
|SETS|non-reserved|non-reserved|non-reserved|
|SHORT|non-reserved|non-reserved|non-reserved|
|SHOW|non-reserved|non-reserved|non-reserved|
|SINGLE|non-reserved|non-reserved|non-reserved|
|SKEWED|non-reserved|non-reserved|non-reserved|
|SMALLINT|non-reserved|non-reserved|reserved|
|SOME|reserved|non-reserved|reserved|
|SORT|non-reserved|non-reserved|non-reserved|
|SORTED|non-reserved|non-reserved|non-reserved|
|SOURCE|non-reserved|non-reserved|non-reserved|
|SPECIFIC|non-reserved|non-reserved|reserved|
|SQL|reserved|non-reserved|reserved|
|START|non-reserved|non-reserved|reserved|
|STATISTICS|non-reserved|non-reserved|non-reserved|
|STORED|non-reserved|non-reserved|non-reserved|
|STRATIFY|non-reserved|non-reserved|non-reserved|
|STRING|non-reserved|non-reserved|non-reserved|
|STRUCT|non-reserved|non-reserved|non-reserved|
|SUBSTR|non-reserved|non-reserved|non-reserved|
|SUBSTRING|non-reserved|non-reserved|non-reserved|
|SYNC|non-reserved|non-reserved|non-reserved|
|SYSTEM_TIME|non-reserved|non-reserved|non-reserved|
|SYSTEM_VERSION|non-reserved|non-reserved|non-reserved|
|TABLE|reserved|non-reserved|reserved|
|TABLES|non-reserved|non-reserved|non-reserved|
|TABLESAMPLE|non-reserved|non-reserved|reserved|
|TARGET|non-reserved|non-reserved|non-reserved|
|TBLPROPERTIES|non-reserved|non-reserved|non-reserved|
|TEMP|non-reserved|non-reserved|not a keyword|
|TEMPORARY|non-reserved|non-reserved|non-reserved|
|TERMINATED|non-reserved|non-reserved|non-reserved|
|THEN|reserved|non-reserved|reserved|
|TIME|reserved|non-reserved|reserved|
|TIMEDIFF|non-reserved|non-reserved|non-reserved|
|TIMESTAMP|non-reserved|non-reserved|non-reserved|
|TIMESTAMP_LTZ|non-reserved|non-reserved|non-reserved|
|TIMESTAMP_NTZ|non-reserved|non-reserved|non-reserved|
|TIMESTAMPADD|non-reserved|non-reserved|non-reserved|
|TIMESTAMPDIFF|non-reserved|non-reserved|non-reserved|
|TINYINT|non-reserved|non-reserved|non-reserved|
|TO|reserved|non-reserved|reserved|
|TOUCH|non-reserved|non-reserved|non-reserved|
|TRAILING|reserved|non-reserved|reserved|
|TRANSACTION|non-reserved|non-reserved|non-reserved|
|TRANSACTIONS|non-reserved|non-reserved|non-reserved|
|TRANSFORM|non-reserved|non-reserved|non-reserved|
|TRIM|non-reserved|non-reserved|non-reserved|
|TRUE|non-reserved|non-reserved|reserved|
|TRUNCATE|non-reserved|non-reserved|reserved|
|TRY_CAST|non-reserved|non-reserved|non-reserved|
|TYPE|non-reserved|non-reserved|non-reserved|
|UNARCHIVE|non-reserved|non-reserved|non-reserved|
|UNBOUNDED|non-reserved|non-reserved|non-reserved|
|UNCACHE|non-reserved|non-reserved|non-reserved|
|UNION|reserved|strict-non-reserved|reserved|
|UNIQUE|reserved|non-reserved|reserved|
|UNKNOWN|reserved|non-reserved|reserved|
|UNLOCK|non-reserved|non-reserved|non-reserved|
|UNPIVOT|non-reserved|non-reserved|non-reserved|
|UNSET|non-reserved|non-reserved|non-reserved|
|UNTIL|non-reserved|non-reserved|non-reserved|
|UPDATE|non-reserved|non-reserved|reserved|
|USE|non-reserved|non-reserved|non-reserved|
|USER|reserved|non-reserved|reserved|
|USING|reserved|strict-non-reserved|reserved|
|VALUES|non-reserved|non-reserved|reserved|
|VARCHAR|non-reserved|non-reserved|reserved|
|VAR|non-reserved|non-reserved|non-reserved|
|VARIABLE|non-reserved|non-reserved|non-reserved|
|VARIANT|non-reserved|non-reserved|reserved|
|VERSION|non-reserved|non-reserved|non-reserved|
|VIEW|non-reserved|non-reserved|non-reserved|
|VIEWS|non-reserved|non-reserved|non-reserved|
|VOID|non-reserved|non-reserved|non-reserved|
|WEEK|non-reserved|non-reserved|non-reserved|
|WEEKS|non-reserved|non-reserved|non-reserved|
|WHEN|reserved|non-reserved|reserved|
|WHERE|reserved|non-reserved|reserved|
|WHILE|non-reserved|non-reserved|non-reserved|
|WINDOW|non-reserved|non-reserved|reserved|
|WITH|reserved|non-reserved|reserved|
|WITHIN|reserved|non-reserved|reserved|
|X|non-reserved|non-reserved|non-reserved|
|YEAR|non-reserved|non-reserved|non-reserved|
|YEARS|non-reserved|non-reserved|non-reserved|
|ZONE|non-reserved|non-reserved|non-reserved|
