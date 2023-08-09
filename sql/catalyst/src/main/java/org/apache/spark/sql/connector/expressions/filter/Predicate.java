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

package org.apache.spark.sql.connector.expressions.filter;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.GeneralScalarExpression;

/**
 * The general representation of predicate expressions, which contains the upper-cased expression
 * name and all the children expressions. You can also use these concrete subclasses for better
 * type safety: {@link And}, {@link Or}, {@link Not}, {@link AlwaysTrue}, {@link AlwaysFalse}.
 * <p>
 * The currently supported predicate expressions:
 * <ol>
 *  <li>Name: <code>IS_NULL</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr IS NULL</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>IS_NOT_NULL</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr IS NOT NULL</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>STARTS_WITH</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 LIKE 'expr2%'</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ENDS_WITH</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 LIKE '%expr2'</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>CONTAINS</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 LIKE '%expr2%'</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>IN</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr IN (expr1, expr2, ...)</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>=</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 = expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>&lt;&gt;</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 &lt;&gt; expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>&lt;=&gt;</code>
 *   <ul>
 *    <li>SQL semantic: null-safe version of <code>expr1 = expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>&lt;</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 &lt; expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>&lt;=</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 &lt;= expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>&gt;</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 &gt; expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>&gt;=</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 &gt;= expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>AND</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 AND expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>OR</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 OR expr2</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>NOT</code>
 *   <ul>
 *    <li>SQL semantic: <code>NOT expr</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ALWAYS_TRUE</code>
 *   <ul>
 *    <li>SQL semantic: <code>TRUE</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ALWAYS_FALSE</code>
 *   <ul>
 *    <li>SQL semantic: <code>FALSE</code></li>
 *    <li>Since version: 3.3.0</li>
 *   </ul>
 *  </li>
 * </ol>
 *
 * @since 3.3.0
 */
@Evolving
public class Predicate extends GeneralScalarExpression {

  public Predicate(String name, Expression[] children) {
    super(name, children);
  }
}
