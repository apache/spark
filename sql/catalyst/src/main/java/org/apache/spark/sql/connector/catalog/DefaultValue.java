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

import java.util.Objects;
import javax.annotation.Nullable;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Expression;

@Evolving
public class DefaultValue {
  private final String sql;
  private final Expression expr;

  public DefaultValue(String sql) {
    this(sql, null /* no expression */);
  }

  public DefaultValue(Expression expr) {
    this(null /* no sql */, expr);
  }

  public DefaultValue(String sql, Expression expr) {
    if (sql == null && expr == null) {
      throw new IllegalArgumentException("SQL and expression can't be both null");
    }
    this.sql = sql;
    this.expr = expr;
  }

  @Nullable
  public String getSql() {
    return sql;
  }

  @Nullable
  public Expression getExpression() {
    return expr;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    DefaultValue that = (DefaultValue) other;
    return Objects.equals(sql, that.sql) && Objects.equals(expr, that.expr);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sql, expr);
  }

  @Override
  public String toString() {
    return String.format("DefaultValue{sql=%s, expression=%s}", sql, expr);
  }
}
