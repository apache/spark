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

/**
 * A constraint that restricts states of data in a table.
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
   * Indicates whether this constraint is actively enforced. If enforced, data modifications
   * that violate the constraint fail with a constraint violation error.
   */
  boolean enforced();

  /**
   * Indicates whether the existing data in the table satisfies this constraint. The constraint
   * can be valid (the data is guaranteed to satisfy the constraint), invalid (some records violate
   * the constraint), or unvalidated (the validity is unknown). The validation status is usually
   * managed by the system and can't be modified by the user.
   */
  ValidationStatus validationStatus();

  /**
   * Indicates whether this constraint is assumed to hold true if the validity is unknown. Unlike
   * the validation status, this flag is usually provided by the user as a hint to the system.
   */
  boolean rely();

  /**
   * Returns the definition of this constraint in the DDL format.
   */
  String toDDL();

  /**
   * Instantiates a builder for a CHECK constraint.
   *
   * @param name the constraint name
   * @return a CHECK constraint builder
   */
  static Check.Builder check(String name) {
    return new Check.Builder(name);
  }

  /**
   * Instantiates a builder for a UNIQUE constraint.
   *
   * @param name the constraint name
   * @param columns columns that comprise the unique key
   * @return a UNIQUE constraint builder
   */
  static Unique.Builder unique(String name, NamedReference[] columns) {
    return new Unique.Builder(name, columns);
  }

  /**
   * Instantiates a builder for a PRIMARY KEY constraint.
   *
   * @param name the constraint name
   * @param columns columns that comprise the primary key
   * @return a PRIMARY KEY constraint builder
   */
  static PrimaryKey.Builder primaryKey(String name, NamedReference[] columns) {
    return new PrimaryKey.Builder(name, columns);
  }

  /**
   * Instantiates a builder for a FOREIGN KEY constraint.
   *
   * @param name the constraint name
   * @param columns the referencing columns
   * @param refTable the referenced table identifier
   * @param refColumns the referenced columns in the referenced table
   * @return a FOREIGN KEY constraint builder
   */
  static ForeignKey.Builder foreignKey(
      String name,
      NamedReference[] columns,
      Identifier refTable,
      NamedReference[] refColumns) {
    return new ForeignKey.Builder(name, columns, refTable, refColumns);
  }

  /**
   * An indicator of the validity of the constraint.
   * <p>
   * A constraint may be validated independently of enforcement, meaning it can be validated
   * without being actively enforced, or vice versa. A constraint can be valid (the data is
   * guaranteed to satisfy the constraint), invalid (some records violate the constraint),
   * or unvalidated (the validity is unknown).
   */
  enum ValidationStatus {
    VALID, INVALID, UNVALIDATED
  }
}
