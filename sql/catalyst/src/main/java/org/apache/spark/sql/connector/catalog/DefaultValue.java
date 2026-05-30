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

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Expression;

/**
 * A class that represents default values.
 * <p>
 * Connectors can define default values using either a SQL string or a connector
 * {@link Expression expression}. If both are provided, Spark first attempts to convert the given
 * expression to its internal representation. If the expression cannot be converted, and a SQL
 * string is provided, Spark will fall back to parsing the SQL string.
 * <p>
 * The SQL string form is provided mainly for backward compatibility. It is parsed and analyzed
 * lazily, so its meaning can depend on the session configuration in effect at that time (for
 * example, ANSI mode or the session time zone) and may therefore be semantically ambiguous. The
 * {@link Expression} form captures the semantics fully and unambiguously (similar to how a view
 * captures the configs it was created with), and is the recommended way to define a default value
 * when it can be represented as a supported connector expression.
 *
 * @since 4.1.0
 */
@Evolving
public class DefaultValue extends ColumnExpressionBase {

  public DefaultValue(String sql) {
    this(sql, null /* no expression */);
  }

  public DefaultValue(Expression expr) {
    this(null /* no sql */, expr);
  }

  public DefaultValue(String sql, Expression expr) {
    super(sql, expr);
  }

  @Override
  public String toString() {
    return String.format("DefaultValue{sql=%s, expression=%s}", getSql(), getExpression());
  }
}
