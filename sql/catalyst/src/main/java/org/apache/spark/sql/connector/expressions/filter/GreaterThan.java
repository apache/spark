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
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * A filter that evaluates to `true` iff the field evaluates to a value
 * greater than `value`.
 *
 * @since 3.3.0
 */
@Evolving
public final class GreaterThan extends Filter {
  private final FieldReference column;
  private final Expression value;

  public GreaterThan(FieldReference column, Expression value) {
    this.column = column;
    this.value = value;
  }

  public FieldReference column() { return column; }
  public Expression value() { return value; }

  @Override
  public String toString() {
    return column.describe() + " GreaterThan: (" + value.describe() + ")";
  }

  @Override
  public String describe() { return this.toString(); }

  @Override
  public NamedReference[] references() {
    NamedReference[] refs = findReferences(value);
    NamedReference[] arr = new NamedReference[1 + refs.length];
    arr[0] = column;
    System.arraycopy(refs, 0, arr, 1, refs.length);
    return arr;
  }
}
