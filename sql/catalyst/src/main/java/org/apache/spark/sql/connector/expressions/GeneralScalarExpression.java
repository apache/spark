/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.expressions;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder;

// scalastyle:off line.size.limit
/**
 * The general representation of SQL scalar expressions, which contains the upper-cased
 * expression name and all the children expressions.
 * <p>
 * The currently supported SQL scalar expressions:
 * <ol>
 *  <li>Name: IS_NULL
 *   <ul>
 *    <li>SQL semantic: `expr IS NULL`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: IS_NOT_NULL
 *   <ul>
 *    <li>SQL semantic: `expr IS NOT NULL`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: =
 *   <ul>
 *    <li>SQL semantic: `expr1 = expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: !=
 *   <ul>
 *    <li>SQL semantic: `expr1 != expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: &lt;&gt;
 *   <ul>
 *    <li>SQL semantic: `expr1 &lt;&gt; expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: &lt;=&gt;
 *   <ul>
 *    <li>SQL semantic: `expr1 &lt;=&gt; expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: &lt;
 *   <ul>
 *    <li>SQL semantic: `expr1 &lt; expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: &lt;=
 *   <ul>
 *    <li>SQL semantic: `expr1 &lt;= expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: &gt;
 *   <ul>
 *    <li>SQL semantic: `expr1 &gt; expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: &gt;=
 *   <ul>
 *    <li>SQL semantic: `expr1 &gt;= expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: +
 *   <ul>
 *    <li>SQL semantic: `expr1 + expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: -
 *   <ul>
 *    <li>SQL semantic: `expr1 - expr2` or `- expr`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: *
 *   <ul>
 *    <li>SQL semantic: `expr1 * expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: /
 *   <ul>
 *    <li>SQL semantic: `expr1 / expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: %
 *   <ul>
 *    <li>SQL semantic: `expr1 % expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: &amp;
 *   <ul>
 *    <li>SQL semantic: `expr1 &amp; expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: |
 *   <ul>
 *    <li>SQL semantic: `expr1 | expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: ^
 *   <ul>
 *    <li>SQL semantic: `expr1 ^ expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: AND
 *   <ul>
 *    <li>SQL semantic: `expr1 AND expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: OR
 *   <ul>
 *    <li>SQL semantic: `expr1 OR expr2`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: NOT
 *   <ul>
 *    <li>SQL semantic: `NOT expr`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: ~
 *   <ul>
 *    <li>SQL semantic: `~ expr`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: CASE_WHEN
 *   <ul>
 *    <li>SQL semantic: `CASE WHEN expr1 THEN expr2 [WHEN expr3 THEN expr4]* [ELSE expr5] END`</li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 * </ol>
 * Note: SQL semantic conforms ANSI standard, so some expressions are not supported when ANSI off,
 * including: add, subtract, multiply, divide, remainder, pmod.
 *
 * @since 3.3.0
 */
// scalastyle:on line.size.limit
@Evolving
public class GeneralScalarExpression implements Expression, Serializable {
  private String name;
  private Expression[] children;

  public GeneralScalarExpression(String name, Expression[] children) {
    this.name = name;
    this.children = children;
  }

  public String name() { return name; }
  public Expression[] children() { return children; }

  @Override
  public String toString() {
    V2ExpressionSQLBuilder builder = new V2ExpressionSQLBuilder();
    try {
      return builder.build(this);
    } catch (Throwable e) {
      return name + "(" +
        Arrays.stream(children).map(child -> child.toString()).reduce((a,b) -> a + "," + b) + ")";
    }
  }
}
