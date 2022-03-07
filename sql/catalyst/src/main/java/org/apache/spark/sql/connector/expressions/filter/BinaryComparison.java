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

import java.util.Objects;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Expression;

/**
 * Base class for {@link EqualNullSafe}, {@link EqualTo}, {@link GreaterThan},
 * {@link GreaterThanOrEqual}, {@link LessThan}, {@link LessThanOrEqual}
 *
 * @since 3.3.0
 */
@Evolving
abstract class BinaryComparison extends Filter {
  protected final Expression column;
  protected final Expression value;

  protected BinaryComparison(Expression column, Expression value) {
    this.column = column;
    this.value = value;
  }

  public Expression column() { return column; }
  public Expression value() { return value; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BinaryComparison that = (BinaryComparison) o;
    return Objects.equals(column, that.column) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(column, value);
  }

  @Override
  public Expression[] references() { return new Expression[] { column }; }
}
