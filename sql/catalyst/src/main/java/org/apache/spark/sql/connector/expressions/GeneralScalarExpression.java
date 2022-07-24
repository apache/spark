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
import java.util.Objects;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.internal.connector.ToStringSQLBuilder;

/**
 * The general representation of SQL scalar expressions, which contains the upper-cased
 * expression name and all the children expressions. Please also see {@link Predicate}
 * for the supported predicate expressions.
 * <p>
 * The currently supported SQL scalar expressions:
 * <ol>
 *  <li>Name: <code>+</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 + expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>-</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 - expr2</code> or <code>- expr</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>*</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 * expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>/</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 / expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>%</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 % expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>&amp;</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 &amp; expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>|</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 | expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>^</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 ^ expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>~</code>
 *   <ul>
 *    <li>SQL semantic: <code>~ expr</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>CASE_WHEN</code>
 *   <ul>
 *    <li>SQL semantic:
 *     <code>CASE WHEN expr1 THEN expr2 [WHEN expr3 THEN expr4]* [ELSE expr5] END</code>
 *    </li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ABS</code>
 *   <ul>
 *    <li>SQL semantic: <code>ABS(expr)</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>COALESCE</code>
 *   <ul>
 *    <li>SQL semantic: <code>COALESCE(expr1, expr2)</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>GREATEST</code>
 *   <ul>
 *    <li>SQL semantic: <code>GREATEST(expr, ...)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>LEAST</code>
 *   <ul>
 *    <li>SQL semantic: <code>LEAST(expr, ...)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>RAND</code>
 *   <ul>
 *    <li>SQL semantic: <code>RAND([seed])</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>LOG</code>
 *   <ul>
 *    <li>SQL semantic: <code>LOG(base, expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>LOG10</code>
 *   <ul>
 *    <li>SQL semantic: <code>LOG10(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>LOG2</code>
 *   <ul>
 *    <li>SQL semantic: <code>LOG2(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>LN</code>
 *   <ul>
 *    <li>SQL semantic: <code>LN(expr)</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>EXP</code>
 *   <ul>
 *    <li>SQL semantic: <code>EXP(expr)</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>POWER</code>
 *   <ul>
 *    <li>SQL semantic: <code>POWER(expr, number)</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>SQRT</code>
 *   <ul>
 *    <li>SQL semantic: <code>SQRT(expr)</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>FLOOR</code>
 *   <ul>
 *    <li>SQL semantic: <code>FLOOR(expr)</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>CEIL</code>
 *   <ul>
 *    <li>SQL semantic: <code>CEIL(expr)</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ROUND</code>
 *   <ul>
 *    <li>SQL semantic: <code>ROUND(expr, [scale])</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>SIN</code>
 *   <ul>
 *    <li>SQL semantic: <code>SIN(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>SINH</code>
 *   <ul>
 *    <li>SQL semantic: <code>SINH(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>COS</code>
 *   <ul>
 *    <li>SQL semantic: <code>COS(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>COSH</code>
 *   <ul>
 *    <li>SQL semantic: <code>COSH(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>TAN</code>
 *   <ul>
 *    <li>SQL semantic: <code>TAN(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>TANH</code>
 *   <ul>
 *    <li>SQL semantic: <code>TANH(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>COT</code>
 *   <ul>
 *    <li>SQL semantic: <code>COT(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ASIN</code>
 *   <ul>
 *    <li>SQL semantic: <code>ASIN(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ASINH</code>
 *   <ul>
 *    <li>SQL semantic: <code>ASINH(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ACOS</code>
 *   <ul>
 *    <li>SQL semantic: <code>ACOS(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ACOSH</code>
 *   <ul>
 *    <li>SQL semantic: <code>ACOSH(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ATAN</code>
 *   <ul>
 *    <li>SQL semantic: <code>ATAN(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ATANH</code>
 *   <ul>
 *    <li>SQL semantic: <code>ATANH(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ATAN2</code>
 *   <ul>
 *    <li>SQL semantic: <code>ATAN2(exprY, exprX)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>CBRT</code>
 *   <ul>
 *    <li>SQL semantic: <code>CBRT(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>DEGREES</code>
 *   <ul>
 *    <li>SQL semantic: <code>DEGREES(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>RADIANS</code>
 *   <ul>
 *    <li>SQL semantic: <code>RADIANS(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>SIGN</code>
 *   <ul>
 *    <li>SQL semantic: <code>SIGN(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>WIDTH_BUCKET</code>
 *   <ul>
 *    <li>SQL semantic: <code>WIDTH_BUCKET(expr)</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>SUBSTRING</code>
 *   <ul>
 *    <li>SQL semantic: <code>SUBSTRING(str, pos[, len])</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>UPPER</code>
 *   <ul>
 *    <li>SQL semantic: <code>UPPER(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>LOWER</code>
 *   <ul>
 *    <li>SQL semantic: <code>LOWER(expr)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>TRANSLATE</code>
 *   <ul>
 *    <li>SQL semantic: <code>TRANSLATE(input, from, to)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>TRIM</code>
 *   <ul>
 *    <li>SQL semantic: <code>TRIM(src, trim)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>LTRIM</code>
 *   <ul>
 *    <li>SQL semantic: <code>LTRIM(src, trim)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>RTRIM</code>
 *   <ul>
 *    <li>SQL semantic: <code>RTRIM(src, trim)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>OVERLAY</code>
 *   <ul>
 *    <li>SQL semantic: <code>OVERLAY(string, replace, position[, length])</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>DATE_ADD</code>
 *   <ul>
 *    <li>SQL semantic: <code>DATE_ADD(start_date, num_days)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>DATE_DIFF</code>
 *   <ul>
 *    <li>SQL semantic: <code>DATE_DIFF(end_date, start_date)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>TRUNC</code>
 *   <ul>
 *    <li>SQL semantic: <code>TRUNC(date, format)</code></li>
 *    <li>Since version: 3.4.0</li>
 *   </ul>
 *  </li>
 * </ol>
 * Note: SQL semantic conforms ANSI standard, so some expressions are not supported when ANSI off,
 * including: add, subtract, multiply, divide, remainder, pmod.
 *
 * @since 3.3.0
 */
@Evolving
public class GeneralScalarExpression implements Expression, Serializable {
  private String name;
  private Expression[] children;

  public GeneralScalarExpression(String name, Expression[] children) {
    this.name = name;
    this.children = children;
  }

  public String name() { return name; }
  @Override
  public Expression[] children() { return children; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GeneralScalarExpression that = (GeneralScalarExpression) o;
    return Objects.equals(name, that.name) && Arrays.equals(children, that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, children);
  }

  @Override
  public String toString() {
    ToStringSQLBuilder builder = new ToStringSQLBuilder();
    return builder.build(this);
  }
}
