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
 *  <li>Name: <code>BOOLEAN_EXPRESSION</code>
 *   <ul>
 *    <li>A simple wrapper for any expression that returns boolean type.</li>
 *    <li>Since version: 4.1.0</li>
 *   </ul>
 *  </li>
 * </ol>
 *
 * <p>The following predicate names are <em>extended/opt-in</em> and are only translated when a
 * data source declares support via
 * {@link org.apache.spark.sql.connector.read.SupportsPushDownPredicateCapabilities}:
 * <ol>
 *  <li>Name: <code>LIKE</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 LIKE expr2</code> (full pattern matching)</li>
 *    <li>Since version: 4.1.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>RLIKE</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 RLIKE expr2</code> (regex matching)</li>
 *    <li>Since version: 4.1.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ILIKE</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr1 ILIKE expr2</code> (case-insensitive LIKE)</li>
 *    <li>Since version: 4.1.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>IS_NAN</code>
 *   <ul>
 *    <li>SQL semantic: <code>isnan(expr)</code></li>
 *    <li>Since version: 4.1.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ARRAY_CONTAINS</code>
 *   <ul>
 *    <li>SQL semantic: <code>array_contains(expr1, expr2)</code></li>
 *    <li>Since version: 4.1.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>MAP_CONTAINS_KEY</code>
 *   <ul>
 *    <li>SQL semantic: <code>map_contains_key(expr1, expr2)</code></li>
 *    <li>Since version: 4.1.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>ARRAYS_OVERLAP</code>
 *   <ul>
 *    <li>SQL semantic: <code>arrays_overlap(arr1, arr2)</code></li>
 *    <li>Since version: 4.1.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>LIKE_ALL</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr LIKE ALL (p1, p2, ...)</code></li>
 *    <li>Children: <code>[expr, pattern1, pattern2, ...]</code></li>
 *    <li>Since version: 4.1.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>LIKE_ANY</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr LIKE ANY (p1, p2, ...)</code></li>
 *    <li>Children: <code>[expr, pattern1, pattern2, ...]</code></li>
 *    <li>Since version: 4.1.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>NOT_LIKE_ALL</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr NOT LIKE ALL (p1, p2, ...)</code></li>
 *    <li>Children: <code>[expr, pattern1, pattern2, ...]</code></li>
 *    <li>Since version: 4.1.0</li>
 *   </ul>
 *  </li>
 *  <li>Name: <code>NOT_LIKE_ANY</code>
 *   <ul>
 *    <li>SQL semantic: <code>expr NOT LIKE ANY (p1, p2, ...)</code></li>
 *    <li>Children: <code>[expr, pattern1, pattern2, ...]</code></li>
 *    <li>Since version: 4.1.0</li>
 *   </ul>
 *  </li>
 * </ol>
 *
 * <p>Data sources may also declare <em>custom predicate functions</em> via
 * {@link org.apache.spark.sql.connector.catalog.SupportsCustomPredicates}.
 * Custom predicates use dot-qualified canonical names to avoid collisions
 * with built-in predicate names. For example:
 * <pre>{@code
 * Predicate("com.mycompany.INDEXQUERY", [col_ref, literal])
 * Predicate("com.mycompany.MY_SEARCH", [col1, col2])
 * }</pre>
 *
 * <p>Custom predicate names must contain at least one '.' character.
 * Spark's built-in names are unqualified upper-case tokens, so there is
 * no collision risk. Data sources match on {@code predicate.name()}
 * directly in their {@code pushPredicates()} implementation.
 *
 * @since 3.3.0
 */
@Evolving
public class Predicate extends GeneralScalarExpression {

  public Predicate(String name, Expression[] children) {
    super(name, children);
    if ("BOOLEAN_EXPRESSION".equals(name)) {
      assert children.length == 1;
    }
  }
}
