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
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Represents a table which can be atomically truncated.
 *
 * @since 3.2.0
 */
@Evolving
public interface TruncatableTable extends Table {
  /**
   * Truncate a table by removing all rows from the table atomically.
   *
   * @return true if a table was truncated successfully otherwise false
   *
   * @since 3.2.0
   */
  boolean truncateTable();

  /**
   * Truncate a table with per-statement options from a {@code DELETE ... WITH (key=value)} clause.
   * <p>
   * The default implementation ignores {@code options} and delegates to {@link #truncateTable()},
   * which preserves backward compatibility for connectors that do not need per-statement options.
   *
   * @param options per-statement options from the {@code WITH} clause; empty if none were given
   * @return true if the table was truncated successfully, false otherwise
   * @since 4.3.0
   */
  default boolean truncateTable(CaseInsensitiveStringMap options) {
    return truncateTable();
  }

  /**
   * Returns an array of supported custom metrics with name and description.
   * By default it returns empty array.
   *
   * @since 4.2.0
   */
  default CustomMetric[] supportedCustomMetrics() {
    return new CustomMetric[]{};
  }

  /**
   * Returns an array of custom metrics which are collected with values at the driver side only.
   * Note that these metrics must be included in the supported custom metrics reported by
   * `supportedCustomMetrics`.
   *
   * @since 4.2.0
   */
  default CustomTaskMetric[] reportDriverMetrics() {
    return new CustomTaskMetric[]{};
  }
}
