---
layout: global
title: Operators
displayTitle: Operators
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

An SQL operator is a symbol specifying an action that is performed on one or more expressions. Operators are represented by special characters or by keywords.

### Operator Precedence

When a complex expression has multiple operators, operator precedence determines the sequence of operations in the expression,
e.g. in expression `1 + 2 * 3`, `*` has higher precedence than `+`, so the expression is evaluated as `1 + (2 * 3) = 7`.
The order of execution can significantly affect the resulting value.

Operators have the precedence levels shown in the following table.
An operator on higher precedence is evaluated before an operator on a lower level.
In the following table, the operators in descending order of precedence, a.k.a. 0 is the highest level and 11 is the lowest.
Operators listed on the same table cell have the same precedence and are evaluated from left to right or right to left based on the associativity.

| Precedence | Operator                                                                                            | Operation                                                                   | Associativity |
|------------|-----------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|---------------|
| 1          | ., []                                                                                               | member access                                                               | Left to right |
| 2          | ::                                                                                                  | cast                                                                        | Left to right |
| 3          | +<br/>-<br/>~                                                                                       | unary plus<br/>unary minus<br/>bitwise NOT                                  | Right to left |
| 4          | *<br/>/<br/>%<br/>DIV                                                                               | multiplication<br/>division, modulo<br/>integral division                   | Left to right |
| 5          | +<br/>-<br/>\|\|                                                                                    | addition<br/>subtraction<br/>concatenation                                  | Left to right |
| 6          | \<\< <br/> \>\> <br/> \>\>\>                                                                        | bitwise shift left<br/>bitwise shift right<br/>bitwise shift right unsigned | Left to right |
| 7          | &                                                                                                   | bitwise AND                                                                 | Left to right |
| 8          | ^                                                                                                   | bitwise XOR(exclusive or)                                                   | Left to right |
| 9          | \|                                                                                                  | bitwise OR(inclusive or)                                                    | Left to right |
| 10         | =, ==<br/>&lt;&gt;, !=<br/>&lt;, &lt;=<br/>&gt;, &gt;=<br/>                                         | comparison operators                                                        | Left to right |
| 11         | NOT, !<br/>EXISTS                                                                                   | logical NOT<br/>existence                                                   | Right to left |
| 12         | BETWEEN<br/>IN<br/>RLIKE, REGEXP<br/>ILIKE<br/>LIKE<br/>IS [NULL, TRUE, FALSE]<br/>IS DISTINCT FROM | other predicates                                                            | Left to right |
| 13         | AND                                                                                                 | conjunction                                                                 | Left to right |
| 14         | OR                                                                                                  | disjunction                                                                 | Left to right |
