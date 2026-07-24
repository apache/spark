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

package org.apache.spark.sql.connector.read;

import java.util.OptionalLong;

import org.apache.spark.annotation.Evolving;

/**
 * A mix in interface for {@link Scan}. Data sources can implement this interface to
 * report statistics to Spark.
 * <p>
 * As of Spark 3.0, statistics are reported to the optimizer after operators are pushed to the
 * data source. Implementations may return more accurate statistics based on pushed operators
 * which may improve query performance by providing better information to the optimizer.
 *
 * @since 3.0.0
 */
@Evolving
public interface SupportsReportStatistics extends Scan {

  /**
   * Returns the estimated statistics of this data source scan.
   */
  Statistics estimateStatistics();

  /**
   * Returns the estimated size in bytes of this scan without computing full statistics.
   * <p>
   * When cost-based optimization or plan statistics are disabled, Spark primarily needs the scan's
   * size in bytes (for example, for broadcast-join thresholding). This method lets connectors serve
   * that size estimate cheaply and avoid computing the full statistics. The default implementation
   * delegates to {@link #estimateStatistics()} and returns its {@code sizeInBytes()}, so connectors
   * that already compute statistics cheaply do not need to override this method.
   *
   * @since 4.3.0
   */
  default OptionalLong estimateSizeInBytes() {
    Statistics statistics = estimateStatistics();
    return statistics != null ? statistics.sizeInBytes() : OptionalLong.empty();
  }

  /**
   * Returns whether the statistics reported by this scan already reflect all filters that were
   * fully pushed down to the data source.
   * <p>
   * When {@code true} (the default), the reported statistics describe exactly the data the scan
   * will produce. When {@code false}, they do <em>not</em> account for the fully pushed filters
   * (for example, they describe the whole table), so Spark may use those fully pushed filters to
   * adjust stats. Re-applying those fully pushed filters in Spark should be redundant for query
   * results because the data source already evaluates them.
   *
   * @since 4.3.0
   */
  default boolean reflectsFullyPushedDownFilters() {
    return true;
  }
}
