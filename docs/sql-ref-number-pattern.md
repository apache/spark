---
layout: global
title: Number patterns
displayTitle: Number Patterns for Formatting and Parsing
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

Functions such as `to_number` and `to_char` support converting between values of string and
Decimal type. Such functions accept format strings indicating how to map between these types.

### Syntax

Number format strings support the following syntax:

```
  { ' [ MI | S ] [ $ ] 
      [ 0 | 9 | G | , ] [...] 
      [ . | D ] 
      [ 0 | 9 ] [...] 
      [ $ ] [ PR | MI | S ] ' }
```

### Elements

Each number format string can contain the following elements (case insensitive):

- **`0`** or **`9`**

  Specifies an expected digit between `0` and `9`.

  A sequence of 0 or 9 in the format string matches a sequence of digits with the same or smaller
  size. If the 0/9 sequence starts with 0 and is before the decimal point, it requires matching the
  number of digits exactly: when parsing, it matches only a digit sequence of the same size; when
  formatting, the result string adds left-padding with zeros to the digit sequence to reach the
  same size. Otherwise, the 0/9 sequence matches any digit sequence with the same or smaller size
  when parsing, and pads the digit sequence with spaces (if before the decimal point) or zeros (if
  after the decimal point) in the result string when formatting. Note that the digit sequence will
  become a '#' sequence when formatting if the size is larger than the 0/9 sequence.

- **`.`** or **`D`**

  Specifies the position of the decimal point. This character may only be specified once.

  When parsing, the input string does not need to include a decimal point.

- **`,`** or **`G`**

  Specifies the position of the `,` grouping (thousands) separator.

  There must be a `0` or `9` to the left and right of each grouping separator. When parsing,
  the input string must match the grouping separator relevant for the size of the number.

- **`$`**

  Specifies the location of the `$` currency sign. This character may only be specified once.

- **`S`** 

  Specifies the position of an optional '+' or '-' sign. This character may only be specified once.

- **`MI`**

  Specifies the position of an optional '-' sign (no '+'). This character may only be specified once.

  When formatting, it prints a space for positive values.

- **`PR`**

  Maps negative input values to wrapping angle brackets (`<1>`) in the corresponding string.

  Positive input values do not receive wrapping angle brackets.

### Function types and error handling

* The `to_number` function accepts an input string and a format string argument. It requires that
the input string matches the provided format and raises an error otherwise. The function then
returns the corresponding Decimal value.
* The `try_to_number` function accepts an input string and a format string argument. It works the
same as the `to_number` function except that it returns NULL instead of raising an error if the
input string does not match the given number format.
* The `to_char` function accepts an input decimal and a format string argument. The function then
returns the corresponding string value.
* All functions will fail if the given format string is invalid.

### Examples

The following examples use the `to_number`, `try_to_number`, and `to_char` SQL
functions.

Note that the format string used in most of these examples expects:
* an optional sign at the beginning,
* followed by a dollar sign,
* followed by a number between 3 and 6 digits long,
* thousands separators,
* up to two digits beyond the decimal point.

#### The `to_number` function

```sql
-- The negative number with currency symbol maps to characters in the format string.
> SELECT to_number('-$12,345.67', 'S$999,099.99');
  -12345.67
 
-- The '$' sign is not optional.
> SELECT to_number('5', '$9');
  Error: the input string does not match the given number format
 
-- The plus sign is optional, and so are fractional digits.
> SELECT to_number('$345', 'S$999,099.99');
  345.00
 
-- The format requires at least three digits.
> SELECT to_number('$45', 'S$999,099.99');
  Error: the input string does not match the given number format
 
-- The format requires at least three digits.
> SELECT to_number('$045', 'S$999,099.99');
  45.00
 
-- MI indicates an optional minus sign at the beginning or end of the input string.
> SELECT to_number('1234-', '999999MI');
  -1234
 
-- PR indicates optional wrapping angel brackets.
> SELECT to_number('9', '999PR')
  9
```

#### The `try_to_number` function:

```sql
-- The '$' sign is not optional.
> SELECT try_to_number('5', '$9');
  NULL
 
-- The format requires at least three digits.
> SELECT try_to_number('$45', 'S$999,099.99');
  NULL
```

#### The `to_char` function:

```sql
> SELECT to_char(decimal(454), '999');
  "454"

-- '99' can format digit sequence with a smaller size.
> SELECT to_char(decimal(1), '99.9');
  " 1.0"

-- '000' left-pads 0 for digit sequence with a smaller size.
> SELECT to_char(decimal(45.1), '000.00');
  "045.10"

> SELECT to_char(decimal(12454), '99,999');
  "12,454"

-- digit sequence with a larger size leads to '#' sequence.
> SELECT to_char(decimal(78.12), '$9.99');
  "$#.##"

-- 'S' can be at the end.
> SELECT to_char(decimal(-12454.8), '99,999.9S');
  "12,454.8-"

> SELECT to_char(decimal(12454.8), 'L99,999.9');
  Error: cannot resolve 'to_char(Decimal(12454.8), 'L99,999.9')' due to data type mismatch:
  Unexpected character 'L' found in the format string 'L99,999.9'; the structure of the format
  string must match: [MI|S] [$] [0|9|G|,]* [.|D] [0|9]* [$] [PR|MI|S]; line 1 pos 25
```
