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
import org.apache.spark.sql.sources.Filter;

/**
 * A mix-in interface for {@link Table} delete support. Data sources can implement this
 * interface to provide the ability to delete data from tables that matches filter expressions.
 *
 * @since 3.0.0
 */
@Evolving
public interface SupportsDelete {

  /**
   * Checks whether it is possible to delete data from a data source table that matches filter
   * expressions.
   * <p>
   * Rows should be deleted from the data source iff all of the filter expressions match.
   * That is, the expressions must be interpreted as a set of filters that are ANDed together.
   * <p>
   * Spark will call this method at planning time to check whether {@link #deleteWhere(Filter[])}
   * would reject the delete operation because it requires significant effort. If this method
   * returns false, Spark will not call {@link #deleteWhere(Filter[])} and will try to rewrite
   * the delete operation and produce row-level changes if the data source table supports deleting
   * individual records.
   *
   * @param filters filter expressions, used to select rows to delete when all expressions match
   * @return true if the delete operation can be performed
   *
   * @since 3.1.0
   */
  default boolean canDeleteWhere(Filter[] filters) {
    return true;
  }

  /**
   * Delete data from a data source table that matches filter expressions. Note that this method
   * will be invoked only if {@link #canDeleteWhere(Filter[])} returns true.
   * <p>
   * Rows are deleted from the data source iff all of the filter expressions match. That is, the
   * expressions must be interpreted as a set of filters that are ANDed together.
   * <p>
   * Implementations may reject a delete operation if the delete isn't possible without significant
   * effort. For example, partitioned data sources may reject deletes that do not filter by
   * partition columns because the filter may require rewriting files without deleted records.
   * To reject a delete implementations should throw {@link IllegalArgumentException} with a clear
   * error message that identifies which expression was rejected.
   *
   * @param filters filter expressions, used to select rows to delete when all expressions match
   * @throws IllegalArgumentException If the delete is rejected due to required effort
   */
  void deleteWhere(Filter[] filters);
}
