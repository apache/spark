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

package org.apache.spark.sql.connector.catalog.constraints;

import org.apache.spark.sql.connector.expressions.NamedReference;

import java.util.Arrays;
import java.util.Objects;

/**
 * A PRIMARY KEY constraint.
 * <p>
 * A PRIMARY KEY constraint specifies ore or more columns as a primary key. Such constraint is
 * satisfied if and only if no two rows in a table have the same non-null values in the primary
 * key columns and none of the values in the specified column or columns are {@code NULL}.
 *
 * @since 4.1.0
 */
public class PrimaryKey extends BaseConstraint {

  private final NamedReference[] columns;

  PrimaryKey(
      String name,
      NamedReference[] columns,
      ConstraintState state) {
    super(name, state);
    this.columns = columns;
  }

  /**
   * Returns the columns that comprise the primary key.
   */
  public NamedReference[] columns() {
    return columns;
  }

  @Override
  protected String definition() {
    return String.format("PRIMARY KEY (%s)", toSQL(columns));
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    PrimaryKey that = (PrimaryKey) other;
    return Objects.equals(name(), that.name()) &&
        Arrays.equals(columns, that.columns()) &&
        Objects.equals(state(), that.state());
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(name(), state());
    result = 31 * result + Arrays.hashCode(columns);
    return result;
  }
}
