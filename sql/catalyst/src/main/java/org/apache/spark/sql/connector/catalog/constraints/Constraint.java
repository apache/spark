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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;

/**
 * A constraint that defines valid states of data in a table.
 *
 * @since 4.1.0
 */
@Evolving
public interface Constraint {
  /**
   * Returns the name of this constraint.
   */
  String name();

  /**
   * Returns the definition of this constraint in DDL format.
   */
  String toDDL();

  /**
   * Returns the state of this constraint.
   */
  ConstraintState state();

  /**
   * Creates a CHECK constraint with a search condition defined in SQL (Spark SQL dialect).
   *
   * @param name the constraint name
   * @param sql the SQL representation of the search condition (Spark SQL dialect)
   * @param state the constraint state
   * @return a CHECK constraint with the provided configuration
   */
  static Check check(String name, String sql, ConstraintState state) {
    return new Check(name, sql, null /* no predicate */, state);
  }

  /**
   * Creates a CHECK constraint with a search condition defined by a {@link Predicate predicate}.
   *
   * @param name the constraint name
   * @param predicate the search condition
   * @param state the constraint state
   * @return a CHECK constraint with the provided configuration
   */
  static Check check(String name, Predicate predicate, ConstraintState state) {
    return new Check(name, null /* no SQL */, predicate, state);
  }

  /**
   * Creates a CHECK constraint with a search condition defined in SQL (Spark SQL dialect) and
   * by {@link Predicate} (if the SQL representation can be converted into a supported expression).
   * The SQL string and predicate must be equivalent.
   *
   * @param name the constraint name
   * @param sql the SQL representation of the search condition (Spark SQL dialect)
   * @param predicate the search condition
   * @param state the constraint state
   * @return a CHECK constraint with the provided configuration
   */
  static Check check(String name, String sql, Predicate predicate, ConstraintState state) {
    return new Check(name, sql, predicate, state);
  }

  /**
   * Creates a UNIQUE constraint.
   *
   * @param name the constraint name
   * @param columns the columns that comprise the unique key
   * @param state the constraint state
   * @return a UNIQUE constraint with the provided configuration
   */
  static Unique unique(String name, NamedReference[] columns, ConstraintState state) {
    return new Unique(name, columns, state);
  }

  /**
   * Creates a PRIMARY KEY constraint.
   *
   * @param name the constraint name
   * @param columns the columns that comprise the primary key
   * @param state the constraint state
   * @return a PRIMARY KEY constraint with the provided configuration
   */
  static PrimaryKey primaryKey(String name, NamedReference[] columns, ConstraintState state) {
    return new PrimaryKey(name, columns, state);
  }

  /**
   * Creates a FOREIGN KEY constraint.
   *
   * @param name the constraint name
   * @param columns the referencing columns
   * @param refTable the referenced table identifier
   * @param refColumns the referenced columns in the referenced table
   * @param state the constraint state
   * @return a FOREIGN KEY constraint with the provided configuration
   */
  static ForeignKey foreignKey(
      String name,
      NamedReference[] columns,
      Identifier refTable,
      NamedReference[] refColumns,
      ConstraintState state) {
    return new ForeignKey(name, columns, refTable, refColumns, state);
  }
}
