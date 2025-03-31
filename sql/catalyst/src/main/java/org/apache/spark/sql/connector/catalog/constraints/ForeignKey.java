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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * A FOREIGN KEY constraint.
 * <p>
 * A FOREIGN KEY constraint specifies one or more columns (referencing columns) in a table that
 * refer to corresponding columns (referenced columns) in another table. The referenced columns
 * must form a UNIQUE or PRIMARY KEY constraint in the referenced table. For this constraint to be
 * satisfied, each row in the table must contain values in the referencing columns that exactly
 * match values of a row in the referenced table.
 * <p>
 * Spark doesn't enforce FOREIGN KEY constraints but leverages them for query optimization. Each
 * constraint is either valid (the existing data is guaranteed to satisfy the constraint), invalid
 * (some records violate the constraint), or unvalidated (the validity is unknown). If the validity
 * is unknown, Spark will check {@link #rely()} to see whether the constraint is believed to be
 * true and can be used for query optimization.
 *
 * @since 4.1.0
 */
@Evolving
public class ForeignKey extends BaseConstraint {

  private final NamedReference[] columns;
  private final Identifier refTable;
  private final NamedReference[] refColumns;

  ForeignKey(
      String name,
      NamedReference[] columns,
      Identifier refTable,
      NamedReference[] refColumns,
      boolean enforced,
      ValidationStatus validationStatus,
      boolean rely) {
    super(name, enforced, validationStatus, rely);
    this.columns = columns;
    this.refTable = refTable;
    this.refColumns = refColumns;
  }

  /**
   * Returns the referencing columns.
   */
  public NamedReference[] columns() {
    return columns;
  }

  /**
   * Returns the referenced table.
   */
  public Identifier referencedTable() {
    return refTable;
  }

  /**
   * Returns the referenced columns in the referenced table.
   */
  public NamedReference[] referencedColumns() {
    return refColumns;
  }

  @Override
  protected String definition() {
    return String.format(
        "FOREIGN KEY (%s) REFERENCES %s (%s)",
        toDDL(columns),
        refTable,
        toDDL(refColumns));
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    ForeignKey that = (ForeignKey) other;
    return Objects.equals(name(), that.name()) &&
        Arrays.equals(columns, that.columns) &&
        Objects.equals(refTable, that.refTable) &&
        Arrays.equals(refColumns, that.refColumns) &&
        enforced() == that.enforced() &&
        Objects.equals(validationStatus(), that.validationStatus()) &&
        rely() == that.rely();
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(name(), refTable, enforced(), validationStatus(), rely());
    result = 31 * result + Arrays.hashCode(columns);
    result = 31 * result + Arrays.hashCode(refColumns);
    return result;
  }

  public static class Builder extends BaseConstraint.Builder<Builder, ForeignKey> {

    private final NamedReference[] columns;
    private final Identifier refTable;
    private final NamedReference[] refColumns;

    public Builder(
        String name,
        NamedReference[] columns,
        Identifier refTable,
        NamedReference[] refColumns) {
      super(name);
      this.columns = columns;
      this.refTable = refTable;
      this.refColumns = refColumns;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    public ForeignKey build() {
      return new ForeignKey(
          name(),
          columns,
          refTable,
          refColumns,
          enforced(),
          validationStatus(),
          rely());
    }
  }
}
