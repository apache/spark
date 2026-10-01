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
   * size in bytes (for example, for broadcast-join thresholding). This method lets connectors that
   * can produce a size estimate cheaply serve it directly and avoid computing the full statistics.
   * <p>
   * The default implementation returns {@code OptionalLong.empty()}, signalling that the connector
   * does not offer a cheap size estimate. In that case Spark falls back to
   * {@link #estimateStatistics()}, so a connector that only implements
   * {@link #estimateStatistics()} keeps the same size-estimation behavior it had before this method
   * existed. Connectors override this method only when they have a genuinely cheaper size estimate
   * than {@link #estimateStatistics()}.
   *
   * @since 4.3.0
   */
  default OptionalLong estimateSizeInBytes() {
    return OptionalLong.empty();
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
   * <p>
   * The adjustment Spark performs when this returns {@code false} is best-effort: Spark re-applies
   * a fully pushed filter for stats adjustment only when every column the filter references is
   * still present in {@link Scan#readSchema()}. If {@code pruneColumns} removes a pushed-filter
   * column, Spark drops that filter from the adjustment, so the reported statistics will not
   * reflect it.
   * <p>
   * A connector that wants a fully pushed filter to participate in this stats adjustment must, when
   * Spark requests schema pruning through {@link SupportsPushDownRequiredColumns}, retain the
   * columns referenced only by that fully pushed filter; otherwise those columns are pruned and the
   * filter is dropped from the adjustment.
   *
   * @since 4.3.0
   */
  default boolean reflectsFullyPushedDownFilters() {
    return true;
  }
}
