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

/**
 * The general representation of SQL scalar expressions, which contains the upper-cased
 * expression name and all the children expressions.
 * <p>
 * <table border="1">
 *  <caption>The currently supported expressions:</caption>
 *  <tr>
 *   <th>Expression name</th>
 *   <th>SQL scalar expression</th>
 *   <th>ANSI enabled</th>
 *   <th>Since version</th>
 *  </tr>
 *  <tr>
 *   <td><pre>IS_NULL</pre></td>
 *   <td><pre>expr IS NULL</pre></td>
 *   <td>No</td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td><pre>IS_NOT_NULL</pre></td>
 *   <td><pre>expr IS NOT NULL</pre></td>
 *   <td>No</td>
 *   <td>3.3.0</td>
 *  <tr>
 *   <td><pre>=</pre></td>
 *   <td><pre>expr1 = expr2</pre></td>
 *   <td>No</td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td><pre>!=</pre></td>
 *   <td><pre>expr1 != expr2</pre></td>
 *   <td>No</td>
 *   <td>3.3.0</td>
 *  </tr>
 *  </tr>
 * </table>
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
