---
layout: global
title: Literals
displayTitle: Literals
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

A literal (also known as a constant) represents a fixed data value. Spark SQL supports the following literals:

 * [String Literal](#string-literal)
 * [Binary Literal](#binary-literal)
 * [Null Literal](#null-literal)
 * [Boolean Literal](#boolean-literal)
 * [Numeric Literal](#numeric-literal)
 * [Datetime Literal](#datetime-literal)
 * [Interval Literal](#interval-literal)

### String Literal

A string literal is used to specify a character string value.

#### Syntax

```sql
[ r ] { 'char [ ... ]' | "char [ ... ]" }
```

#### Parameters

* **char**

    One character from the character set. Use `\` to escape special characters (e.g., `'` or `\`).
    To represent unicode characters, use 16-bit or 32-bit unicode escape of the form `\uxxxx` or `\Uxxxxxxxx`,
    where xxxx and xxxxxxxx are 16-bit and 32-bit code points in hexadecimal respectively (e.g., `\u3042` for `あ` and `\U0001F44D` for `👍`).
    An ASCII character can also be represented as an octal number preceded by `\` like `\101`, which represents `A`.

* **r**

    Case insensitive, indicates `RAW`. If a string literal starts with `r` prefix, neither special characters nor unicode characters are escaped by `\`.

The following escape sequences are recognized in regular string literals (without the `r` prefix), and replaced according to the following rules:
- `\0` -> `\u0000`, unicode character with the code 0;
- `\b` -> `\u0008`, backspace;
- `\n` -> `\u000a`, linefeed;
- `\r` -> `\u000d`, carriage return;
- `\t` -> `\u0009`, horizontal tab;
- `\Z` -> `\u001A`, substitute;
- `\%` -> `\%`;
- `\_` -> `\_`;
- `\<other char>` -> `<other char>`, skip the slash and leave the character as is.

The unescaping rules above can be turned off by setting the SQL config `spark.sql.parser.escapedStringLiterals` to `true`.

#### Examples

```sql
SELECT 'Hello, World!' AS col;
+-------------+
|          col|
+-------------+
|Hello, World!|
+-------------+

SELECT "SPARK SQL" AS col;
+---------+
|      col|
+---------+
|SPARK SQL|
+---------+

SELECT 'it\'s $10.' AS col;
+---------+
|      col|
+---------+
|it's $10.|
+---------+

SELECT r"'\n' represents newline character." AS col;
+----------------------------------+
|                               col|
+----------------------------------+
|'\n' represents newline character.|
+----------------------------------+
```

### Binary Literal

A binary literal is used to specify a byte sequence value.

#### Syntax

```sql
X { 'num [ ... ]' | "num [ ... ]" }
```

#### Parameters

* **num**

    Any hexadecimal number from 0 to F.

#### Examples

```sql
SELECT X'123456' AS col;
+----------+
|       col|
+----------+
|[12 34 56]|
+----------+
```

### Null Literal

A null literal is used to specify a null value.

#### Syntax

```sql
NULL
```

#### Examples

```sql
SELECT NULL AS col;
+----+
| col|
+----+
|NULL|
+----+
```

### Boolean Literal

A boolean literal is used to specify a boolean value.

#### Syntax

```sql
TRUE | FALSE
```

#### Examples

```sql
SELECT TRUE AS col;
+----+
| col|
+----+
|true|
+----+
```

### Numeric Literal

A numeric literal is used to specify a fixed or floating-point number.
There are two kinds of numeric literals: integral literal and fractional literal.

#### Integral Literal Syntax

```sql
[ + | - ] digit [ ... ] [ L | S | Y ]
```

#### Integral Literal Parameters

* **digit**

    Any numeral from 0 to 9.

* **L**

    Case insensitive, indicates `BIGINT`, which is an 8-byte signed integer number.

* **S**

    Case insensitive, indicates `SMALLINT`, which is a 2-byte signed integer number.

* **Y**

    Case insensitive, indicates `TINYINT`, which is a 1-byte signed integer number.

* **default (no postfix)**

    Indicates a 4-byte signed integer number.

#### Integral Literal Examples

```sql
SELECT -2147483648 AS col;
+-----------+
|        col|
+-----------+
|-2147483648|
+-----------+

SELECT 9223372036854775807l AS col;
+-------------------+
|                col|
+-------------------+
|9223372036854775807|
+-------------------+

SELECT -32Y AS col;
+---+
|col|
+---+
|-32|
+---+

SELECT 482S AS col;
+---+
|col|
+---+
|482|
+---+
```

#### Fractional Literals Syntax

decimal literals:
```sql
decimal_digits { [ BD ] | [ exponent BD ] } | digit [ ... ] [ exponent ] BD
```

double literals:
```sql
decimal_digits  { D | exponent [ D ] }  | digit [ ... ] { exponent [ D ] | [ exponent ] D }
```

float literals:
```sql
decimal_digits  { F | exponent [ F ] }  | digit [ ... ] { exponent [ F ] | [ exponent ] F }
```

While decimal_digits is defined as
```sql
[ + | - ] { digit [ ... ] . [ digit [ ... ] ] | . digit [ ... ] }
```

and exponent is defined as
```sql
E [ + | - ] digit [ ... ]
```

#### Fractional Literals Parameters

* **digit**

    Any numeral from 0 to 9.

* **D**

    Case insensitive, indicates `DOUBLE`, which is an 8-byte double-precision floating point number.

* **F**

    Case insensitive, indicates `FLOAT`, which is a 4-byte single-precision floating point number.

* **BD**

    Case insensitive, indicates `DECIMAL`, with the total number of digits as precision and the number of digits to right of decimal point as scale.

#### Fractional Literals Examples

```sql
SELECT 12.578 AS col, TYPEOF(12.578) AS type;
+------+------------+
|   col|        type|
+------+------------+
|12.578|decimal(5,3)|
+------+------------+

SELECT 12.578E0 AS col, TYPEOF(12.578E0) AS type;
+------+------+
|   col|  type|
+------+------+
|12.578|double|
+------+------+

SELECT -0.1234567 AS col;
+----------+
|       col|
+----------+
|-0.1234567|
+----------+

SELECT -.1234567 AS col;
+----------+
|       col|
+----------+
|-0.1234567|
+----------+

SELECT 123. AS col;
+---+
|col|
+---+
|123|
+---+

SELECT 123.BD AS col;
+---+
|col|
+---+
|123|
+---+

SELECT 5E2 AS col;
+-----+
|  col|
+-----+
|500.0|
+-----+

SELECT 5D AS col;
+---+
|col|
+---+
|5.0|
+---+

SELECT -5BD AS col;
+---+
|col|
+---+
| -5|
+---+

SELECT 12.578e-2d AS col;
+-------+
|    col|
+-------+
|0.12578|
+-------+

SELECT -.1234567E+2BD AS col;
+---------+
|      col|
+---------+
|-12.34567|
+---------+

SELECT +3.e+3 AS col;
+------+
|   col|
+------+
|3000.0|
+------+

SELECT -3.E-3D AS col;
+------+
|   col|
+------+
|-0.003|
+------+
```

### Datetime Literal

A datetime literal is used to specify a date or timestamp value.

#### Date Syntax

```sql
DATE { 'yyyy' |
       'yyyy-[m]m' |
       'yyyy-[m]m-[d]d' |
       'yyyy-[m]m-[d]d[T]' }
```
**Note:** defaults to `01` if month or day is not specified.

#### Date Examples

```sql
SELECT DATE '1997' AS col;
+----------+
|       col|
+----------+
|1997-01-01|
+----------+

SELECT DATE '1997-01' AS col;
+----------+
|       col|
+----------+
|1997-01-01|
+----------+

SELECT DATE '2011-11-11' AS col;
+----------+
|       col|
+----------+
|2011-11-11|
+----------+
```

#### Timestamp Syntax

```sql
TIMESTAMP { 'yyyy' |
            'yyyy-[m]m' |
            'yyyy-[m]m-[d]d' |
            'yyyy-[m]m-[d]d ' |
            'yyyy-[m]m-[d]d[T][h]h[:]' |
            'yyyy-[m]m-[d]d[T][h]h:[m]m[:]' |
            'yyyy-[m]m-[d]d[T][h]h:[m]m:[s]s[.]' |
            'yyyy-[m]m-[d]d[T][h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]'}
```
**Note:** defaults to `00` if hour, minute or second is not specified.
`zone_id` should have one of the forms:
* Z - Zulu time zone UTC+0
* `+|-[h]h:[m]m`
* An id with one of the prefixes UTC+, UTC-, GMT+, GMT-, UT+ or UT-, and a suffix in the formats:
  * `+|-h[h]`
  * `+|-hh[:]mm`
  * `+|-hh:mm:ss`
  * `+|-hhmmss`
* Region-based zone IDs in the form `area/city`, such as `Europe/Paris`

**Note:** defaults to the session local timezone (set via `spark.sql.session.timeZone`) if `zone_id` is not specified.

#### Timestamp Examples

```sql
SELECT TIMESTAMP '1997-01-31 09:26:56.123' AS col;
+-----------------------+
|                    col|
+-----------------------+
|1997-01-31 09:26:56.123|
+-----------------------+

SELECT TIMESTAMP '1997-01-31 09:26:56.66666666UTC+08:00' AS col;
+--------------------------+
|                      col |
+--------------------------+
|1997-01-30 17:26:56.666666|
+--------------------------+

SELECT TIMESTAMP '1997-01' AS col;
+-------------------+
|                col|
+-------------------+
|1997-01-01 00:00:00|
+-------------------+
```

### Interval Literal

An interval literal is used to specify a fixed period of time.
The interval literal supports two syntaxes: ANSI syntax and multi-units syntax.

#### ANSI Syntax

The ANSI SQL standard defines interval literals in the form:
```sql
INTERVAL [ <sign> ] <interval string> <interval qualifier>
```
where `<interval qualifier>` can be a single field or in the field-to-field form:
```sql
<interval qualifier> ::= <start field> TO <end field> | <single field>
```
The field name is case-insensitive, and can be one of `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE` and `SECOND`.

An interval literal can have either year-month or day-time interval type. The interval sub-type defines format of `<interval string>`:
```sql
<interval string> ::= <quote> [ <sign> ] { <year-month literal> | <day-time literal> } <quote>
<year-month literal> ::= <years value> [ <minus sign> <months value> ] | <months value>
<day-time literal> ::= <day-time interval> | <time interval>
<day-time interval> ::= <days value> [ <space> <hours value> [ <colon> <minutes value> [ <colon> <seconds value> ] ] ]
<time interval> ::= <hours value> [ <colon> <minutes value> [ <colon> <seconds value> ] ]
  | <minutes value> [ <colon> <seconds value> ]
  | <seconds value>
```

Supported year-month interval literals and theirs formats:

|`<interval qualifier>`|Interval string pattern|An instance of the literal|
|---------|-------|------------|
|YEAR|`[+|-]'[+|-]y'`|`INTERVAL -'2021' YEAR`|
|YEAR TO MONTH|`[+|-]'[+|-]y-m'`|`INTERVAL '-2021-07' YEAR TO MONTH`|
|MONTH|`[+|-]'[+|-]m'`|`interval '10' month`|

Formats of supported day-time interval literals:

|`<interval qualifier>`|Interval string pattern|An instance of the literal|
|---------|----|-------------------|
|DAY|`[+|-]'[+|-]d'`|`INTERVAL -'100' DAY`|
|DAY TO HOUR|`[+|-]'[+|-]d h'`|`INTERVAL '-100 10' DAY TO HOUR`|
|DAY TO MINUTE|`[+|-]'[+|-]d h:m'`|`INTERVAL '100 10:30' DAY TO MINUTE`|
|DAY TO SECOND|`[+|-]'[+|-]d h:m:s.n'`|`INTERVAL '100 10:30:40.999999' DAY TO SECOND`|
|HOUR|`[+|-]'[+|-]h'`|`INTERVAL '123' HOUR`|
|HOUR TO MINUTE|`[+|-]'[+|-]h:m'`|`INTERVAL -'-123:10' HOUR TO MINUTE`|
|HOUR TO SECOND|`[+|-]'[+|-]h:m:s.n'`|`INTERVAL '123:10:59' HOUR TO SECOND`|
|MINUTE|`[+|-]'[+|-]m'`|`interval '1000' minute`|
|MINUTE TO SECOND|`[+|-]'[+|-]m:s.n'`|`INTERVAL '1000:01.001' MINUTE TO SECOND`|
|SECOND|`[+|-]'[+|-]s.n'`|`INTERVAL '1000.000001' SECOND`|

#### ANSI Examples

```sql
SELECT INTERVAL '2-3' YEAR TO MONTH AS col;
+----------------------------+
|col                         |
+----------------------------+
|INTERVAL '2-3' YEAR TO MONTH|
+----------------------------+

SELECT INTERVAL -'20 15:40:32.99899999' DAY TO SECOND AS col;
+--------------------------------------------+
|col                                         |
+--------------------------------------------+
|INTERVAL '-20 15:40:32.998999' DAY TO SECOND|
+--------------------------------------------+
```

#### Multi-units Syntax

```sql
INTERVAL interval_value interval_unit [ interval_value interval_unit ... ] |
INTERVAL 'interval_value interval_unit [ interval_value interval_unit ... ]' |
```

#### Multi-units Parameters

* **interval_value**

    **Syntax:**

      [ + | - ] number_value | '[ + | - ] number_value'

* **interval_unit**

    **Syntax:**

      YEAR[S] | MONTH[S] | WEEK[S] | DAY[S] | HOUR[S] | MINUTE[S] | SECOND[S] |
      MILLISECOND[S] | MICROSECOND[S]

      Mix of the YEAR[S] or MONTH[S] interval units with other units is not allowed.

#### Multi-units Examples

```sql
SELECT INTERVAL 3 YEAR AS col;
+-------+
|    col|
+-------+
|3 years|
+-------+

SELECT INTERVAL -2 HOUR '3' MINUTE AS col;
+--------------------+
|                 col|
+--------------------+
|-1 hours -57 minutes|
+--------------------+

SELECT INTERVAL '1 YEAR 2 DAYS 3 HOURS';
+----------------------+
|                   col|
+----------------------+
|1 years 2 days 3 hours|
+----------------------+

SELECT INTERVAL 1 YEARS 2 MONTH 3 WEEK 4 DAYS 5 HOUR 6 MINUTES 7 SECOND 8
    MILLISECOND 9 MICROSECONDS AS col;
+-----------------------------------------------------------+
|                                                        col|
+-----------------------------------------------------------+
|1 years 2 months 25 days 5 hours 6 minutes 7.008009 seconds|
+-----------------------------------------------------------+
```
