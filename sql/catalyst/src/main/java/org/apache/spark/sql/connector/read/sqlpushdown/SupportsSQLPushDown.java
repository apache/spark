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
package org.apache.spark.sql.connector.read.sqlpushdown;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

/**
 * A mix-in interface for {@link ScanBuilder}. Data sources which support SQL can implement this
 *  interface to push down SQL to backend and reduce the size of the data to be read.
 *
 *  @since 3.x.x
 */

@Evolving
public interface SupportsSQLPushDown extends ScanBuilder,
        SupportsPushDownRequiredColumns,
        SupportsPushDownFilters {

  /**
   * Return true if executing a query on them would result in a query issued to multiple partitions.
   * Returns false if it would result in a query to a single partition and therefore provides global
   * results.
   */
  boolean isMultiplePartitionExecution();

  /**
   * Pushes down {@link SQLStatement} to datasource and returns filters that need to be evaluated
   * after scanning.
   * <p>
   * Rows should be returned from the data source if and only if all of the filters match. That is,
   * filters must be interpreted as ANDed together.
   */
  Filter[] pushStatement(SQLStatement statement, StructType outputSchema);

  /**
   * Returns the statement that are pushed to the data source via
   * {@link #pushStatement(SQLStatement statement, StructType outputSchema)}
   */
  SQLStatement pushedStatement();
}
