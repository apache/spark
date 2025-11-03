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

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.sources.Filter;

/**
 * A mix-in interface for {@link Scan}. Data sources can implement this interface if they can
 * filter initially planned {@link InputPartition}s using predicates Spark infers at runtime.
 * This interface is very similar to {@link SupportsRuntimeFiltering} except it uses
 * data source V2 {@link Predicate} instead of data source V1 {@link Filter}.
 * {@link SupportsRuntimeV2Filtering} is preferred over {@link SupportsRuntimeFiltering}
 * and only one of them should be implemented by the data sources.
 *
 * <p>
 * Note that Spark will push runtime filters only if they are beneficial.
 *
 * @since 3.4.0
 */
@Experimental
public interface SupportsRuntimeV2Filtering extends Scan {
  /**
   * Returns attributes this scan can be filtered by at runtime.
   * <p>
   * Spark will call {@link #filter(Predicate[])} if it can derive a runtime
   * predicate for any of the filter attributes.
   */
  NamedReference[] filterAttributes();

  /**
   * Filters this scan using runtime predicates.
   * <p>
   * The provided expressions must be interpreted as a set of predicates that are ANDed together.
   * Implementations may use the predicates to prune initially planned {@link InputPartition}s.
   * <p>
   * If the scan also implements {@link SupportsReportPartitioning}, it must preserve
   * the originally reported partitioning during runtime filtering. While applying runtime
   * predicates, the scan may detect that some {@link InputPartition}s have no matching data. It
   * can omit such partitions entirely only if it does not report a specific partitioning.
   * Otherwise, the scan can replace the initially planned {@link InputPartition}s that have no
   * matching data with empty {@link InputPartition}s but must preserve the overall number of
   * partitions.
   * <p>
   * Note that Spark will call {@link Scan#toBatch()} again after filtering the scan at runtime.
   *
   * @param predicates data source V2 predicates used to filter the scan at runtime
   */
  void filter(Predicate[] predicates);
}
