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
In the following table, the operators in descending order of precedence, a.k.a. 1 is the highest level.
Operators listed on the same table cell have the same precedence and are evaluated from left to right or right to left based on the associativity.

<table>
  <thead>
    <tr>
      <th>Precedence</th>
      <th>Operator</th>
      <th>Operation</th>
      <th>Associativity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>1</td>
      <td>.<br/>[]<br/>::</td>
      <td>member access<br/>element access<br/>cast</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>2</td>
      <td>+<br/>-<br/>~</td>
      <td>unary plus<br/>unary minus<br/>bitwise NOT</td>
      <td>Right to left</td>
    </tr>
    <tr>
      <td>3</td>
      <td>*<br/>/<br/>%<br/>DIV</td>
      <td>multiplication<br/>division, modulo<br/>integral division</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>4</td>
      <td>+<br/>-<br/>||</td>
      <td>addition<br/>subtraction<br/>concatenation</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>5</td>
      <td>&lt;&lt;<br/>&gt;&gt;<br/>&gt;&gt;&gt;</td>
      <td>bitwise shift left<br/>bitwise shift right<br/>bitwise shift right unsigned</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>6</td>
      <td>&</td>
      <td>bitwise AND</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>7</td>
      <td>^</td>
      <td>bitwise XOR(exclusive or)</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>8</td>
      <td>|</td>
      <td>bitwise OR(inclusive or)</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>9</td>
      <td>=, ==<br/>&lt;&gt;, !=<br/>&lt;, &lt;=<br/>&gt;, &gt;=</td>
      <td>comparison operators</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>10</td>
      <td>NOT, !<br/>EXISTS</td>
      <td>logical NOT<br/>existence</td>
      <td>Right to left</td>
    </tr>
    <tr>
      <td>11</td>
      <td>BETWEEN<br/>IN<br/>RLIKE, REGEXP<br/>ILIKE<br/>LIKE<br/>IS [NULL, TRUE, FALSE]<br/>IS DISTINCT FROM</td>
      <td>other predicates</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>12</td>
      <td>AND</td>
      <td>conjunction</td>
      <td>Left to right</td>
    </tr>
    <tr>
      <td>13</td>
      <td>OR</td>
      <td>disjunction</td>
      <td>Left to right</td>
    </tr>
  </tbody>
</table>
