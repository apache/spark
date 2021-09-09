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
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Base class for {@link StringContains}, {@link StringStartsWith},
 * {@link StringEndsWith}
 *
 * @since 3.3.0
 */
@Evolving
abstract class StringPredicate extends Filter {
  protected final NamedReference column;
  protected final UTF8String value;

  protected StringPredicate(NamedReference column, UTF8String value) {
    this.column = column;
    this.value = value;
  }

  public NamedReference column() { return column; }
  public UTF8String value() { return value; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StringPredicate that = (StringPredicate) o;
    return Objects.equals(column, that.column) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(column, value);
  }

  @Override
  public NamedReference[] references() { return new NamedReference[] { column }; }
}
