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

/**
 * Represents a dependency of a SQL object such as a view or metric view.
 * <p>
 * A dependency is one of: {@link TableDependency} or {@link FunctionDependency}. The
 * {@code sealed} declaration enforces this structurally.
 * <p>
 * Note: today the only producer in Spark itself is metric-view dependency extraction, which
 * emits {@link TableDependency} only. {@link FunctionDependency} and the
 * {@link #function(String[])} factory are exposed as groundwork for future producers
 * (e.g. SQL UDF dependency tracking); consumers iterating a {@link DependencyList} received
 * from Spark today should expect to see only {@link TableDependency} instances.
 *
 * @since 4.2.0
 */
@Evolving
public sealed interface Dependency permits TableDependency, FunctionDependency {

  /**
   * Construct a {@link TableDependency} from the structural multi-part name of the dependent
   * table. {@code nameParts} should contain at least one element; for catalog-managed tables
   * the first element is typically the catalog name and subsequent elements are namespace
   * components followed by the table name.
   */
  static TableDependency table(String[] nameParts) {
    return new TableDependency(nameParts);
  }

  /**
   * Construct a {@link FunctionDependency} from the structural multi-part name of the
   * dependent function. {@code nameParts} should contain at least one element; for
   * catalog-managed functions the first element is typically the catalog name and subsequent
   * elements are namespace components followed by the function name.
   */
  static FunctionDependency function(String[] nameParts) {
    return new FunctionDependency(nameParts);
  }
}
