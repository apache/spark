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

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.filter.Predicate;

/**
 * A mix-in interface for {@link Table} update support. Data sources can implement this
 * interface to provide the ability to update data in tables that matches filter expressions.
 * <p>
 * This interface enables push-down of UPDATE operations to the data source, similar to how
 * {@link SupportsDeleteV2} enables push-down of DELETE operations.
 *
 * @since 4.1.0
 */
@Evolving
public interface SupportsUpdateV2 extends Table {

  /**
   * Represents a column assignment for an UPDATE operation.
   * Contains the column name and the new value expression as a string.
   */
  class Assignment {
    private final String column;
    private final String value;

    public Assignment(String column, String value) {
      this.column = column;
      this.value = value;
    }

    public String getColumn() {
      return column;
    }

    public String getValue() {
      return value;
    }
  }

  /**
   * Checks whether it is possible to update data in a data source table that matches filter
   * expressions with the given assignments.
   * <p>
   * Rows should be updated in the data source iff all of the filter expressions match.
   * That is, the expressions must be interpreted as a set of filters that are ANDed together.
   * <p>
   * Spark will call this method at planning time to check whether {@link #updateWhere}
   * would reject the update operation because it requires significant effort. If this method
   * returns false, Spark will not call {@link #updateWhere} and will try to rewrite
   * the update operation using row-level operations if the data source table supports it.
   *
   * @param predicates V2 filter expressions, used to select rows to update when all expressions
   *                   match
   * @param assignments the column assignments to apply to matching rows
   * @return true if the update operation can be performed
   */
  default boolean canUpdateWhere(Predicate[] predicates, Assignment[] assignments) {
    return true;
  }

  /**
   * Update data in a data source table that matches filter expressions. Note that this method
   * will be invoked only if {@link #canUpdateWhere} returns true.
   * <p>
   * Rows are updated in the data source iff all of the filter expressions match. That is, the
   * expressions must be interpreted as a set of filters that are ANDed together.
   * <p>
   * Implementations may reject an update operation if the update isn't possible without significant
   * effort. To reject an update, implementations should throw {@link IllegalArgumentException}
   * with a clear error message that identifies which expression or assignment was rejected.
   *
   * @param predicates predicate expressions, used to select rows to update when all expressions
   *                   match
   * @param assignments the column assignments to apply to matching rows
   * @throws IllegalArgumentException If the update is rejected due to required effort
   */
  void updateWhere(Predicate[] predicates, Assignment[] assignments);
}
