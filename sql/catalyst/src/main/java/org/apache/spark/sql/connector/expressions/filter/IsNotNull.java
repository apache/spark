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
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * A filter that evaluates to {@code true} iff the {@code column} evaluates to a non-null value.
 *
 * @since 3.3.0
 */
@Evolving
public final class IsNotNull extends Filter {
  private final NamedReference column;

  public IsNotNull(NamedReference column) {
    this.column = column;
  }

  public NamedReference column() { return column; }

  @Override
  public String toString() { return column.describe() + " IS NOT NULL"; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IsNotNull isNotNull = (IsNotNull) o;
    return Objects.equals(column, isNotNull.column);
  }

  @Override
  public int hashCode() {
    return Objects.hash(column);
  }

  @Override
  public NamedReference[] references() { return new NamedReference[] { column }; }
}
