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

import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.NamedReference;

import java.util.Arrays;
import java.util.Objects;

/**
 * A FOREIGN KEY constraint.
 * <p>
 * A FOREIGN KEY constraint specifies one or more columns (referencing columns) in a table that
 * refer to corresponding columns (referenced columns) in another table. The referenced columns
 * must form a UNIQUE or PRIMARY KEY constraint in the referenced table. For this constraint to be
 * satisfied, each row in the table must contain values in the referencing columns that exactly
 * match values of a row in the referenced table.
 *
 * @since 4.1.0
 */
public class ForeignKey extends BaseConstraint {

  private final NamedReference[] columns;
  private final Identifier refTable;
  private final NamedReference[] refColumns;

  ForeignKey(
      String name,
      NamedReference[] columns,
      Identifier refTable,
      NamedReference[] refColumns,
      ConstraintState state) {
    super(name, state);
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
        toSQL(columns),
        refTable.toString(),
        toSQL(refColumns));
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
        Objects.equals(state(), that.state());
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(name(), refTable, state());
    result = 31 * result + Arrays.hashCode(columns);
    result = 31 * result + Arrays.hashCode(refColumns);
    return result;
  }
}
