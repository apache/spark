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
import org.apache.spark.sql.connector.read.Statistics;

/**
 * A mix-in interface for {@link Table}. Data sources can implement this interface to report
 * table-level statistics to Spark from catalog metadata, without building a scan.
 * <p>
 * These statistics represent the full table and are not adjusted for any filter or projection
 * pushdown. They are used by the optimizer during planning phases that occur before scan-level
 * pushdown, such as join-type selection for LEFT SEMI and LEFT ANTI joins. Because no scan is
 * built, the cost of computing these statistics must be low (for example, read from a snapshot
 * summary map or table properties).
 * <p>
 * After scan pushdown is applied, SupportsReportStatistics on the resulting Scan is used for more
 * accurate, filter-aware statistics. Connectors that support both interfaces provide a two-level
 * view: catalog-level stats here, and refined scan-level stats after pushdown.
 * <p>
 * If any field is unavailable (e.g., no snapshot has been committed yet), the corresponding
 * {@link java.util.OptionalLong} should be empty rather than a guess.
 *
 * @since 5.0.0
 */
@Evolving
public interface SupportsReportCatalogStatistics {

  /**
   * Returns estimated statistics for this table based on catalog metadata.
   * <p>
   * Implementations must not build a scan or perform any expensive I/O. The returned statistics
   * represent the full table at the time of the call (e.g., the current snapshot), not a filtered
   * or projected subset.
   *
   * @return table-level statistics from catalog metadata
   */
  Statistics estimateCatalogStatistics();
}
