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
 * A class that represents generated column expressions.
 * <p>
 * Connectors can define generated columns using either a SQL string (Spark SQL dialect) or a more
 * portable connector {@link Expression expression}, whose semantics follow the ANSI SQL standard.
 * <p>
 * Spark computes a generated value on a write only from the SQL string, so a column without one is
 * not auto-filled. To check a provided value, Spark parses the SQL string if one is provided, also
 * when an expression is provided. Otherwise, Spark converts the given expression to its internal
 * representation.
 *
 * @since 4.3.0
 */
@Evolving
public class GenerationExpression extends ColumnExpressionBase {

  public GenerationExpression(String sql) {
    this(sql, null /* no expression */);
  }

  public GenerationExpression(Expression expr) {
    this(null /* no sql */, expr);
  }

  public GenerationExpression(String sql, Expression expr) {
    super(sql, expr);
  }

  @Override
  public String toString() {
    return String.format(
        "GenerationExpression{sql=%s, expression=%s}", getSql(), getExpression());
  }
}

