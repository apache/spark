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

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * A filter that evaluates to `true` iff the field evaluates to one of the values in the array.
 *
 * @since 3.3.0
 */
@Evolving
public final class In<T> extends Filter {
  private final FieldReference column;
  private final Literal<T>[] values;

  public In(FieldReference column, Literal<T>[] values) {
    this.column = column;
    this.values = values;
  }

  public FieldReference column() { return column; }
  public Literal<T>[] values() { return values; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    In<?> in = (In<?>) o;
    return Objects.equals(column, in.column) && Arrays.equals(values, in.values);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(column);
    result = 31 * result + Arrays.hashCode(values);
    return result;
  }

  @Override
  public String toString() {
    String res = Arrays.stream(values).map(Literal::describe).collect(Collectors.joining(", "));
    return column.describe() + " IN (" + res + ")";
  }

  @Override
  public NamedReference[] references() { return new NamedReference[] { column }; }
}
