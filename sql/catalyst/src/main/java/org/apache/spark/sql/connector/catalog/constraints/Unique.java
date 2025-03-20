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

import java.util.Arrays;
import java.util.Objects;

import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * A UNIQUE constraint.
 * <p>
 * A UNIQUE constraint specifies one or more columns as unique columns. Such constraint is satisfied
 * if and only if no two rows in a table have the same non-null values in the unique columns.
 *
 * @since 4.1.0
 */
public class Unique extends BaseConstraint {

  private final NamedReference[] columns;

  private Unique(
      String name,
      NamedReference[] columns,
      boolean enforced,
      ValidationStatus validationStatus,
      boolean rely) {
    super(name, enforced, validationStatus, rely);
    this.columns = columns;
  }

  /**
   * Returns the columns that comprise the unique key.
   */
  public NamedReference[] columns() {
    return columns;
  }

  @Override
  protected String definition() {
    return String.format("UNIQUE (%s)", toSQL(columns));
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    Unique that = (Unique) other;
    return Objects.equals(name(), that.name()) &&
        Arrays.equals(columns, that.columns()) &&
        enforced() == that.enforced() &&
        Objects.equals(validationStatus(), that.validationStatus()) &&
        rely() == that.rely();
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(name(), enforced(), validationStatus(), rely());
    result = 31 * result + Arrays.hashCode(columns);
    return result;
  }

  public static class Builder extends BaseConstraint.Builder<Builder, Unique> {

    private final NamedReference[] columns;

    Builder(String name, NamedReference[] columns) {
      super(name);
      this.columns = columns;
    }

    @Override
    protected Builder self() {
      return this;
    }

    public Unique build() {
      return new Unique(name(), columns, enforced(), validationStatus(), rely());
    }
  }
}
