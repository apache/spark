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
package org.apache.spark.sql.internal.connector

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering

/**
 * A mix-in interface for {@link FileScan}. File sources can implement this interface to
 * push down runtime filters to the file source. The runtime filters will be used for partition
 * pruning.
 */
trait SupportsRuntimeCatalystFilters extends SupportsRuntimeV2Filtering {

  /**
   * Filters this scan using runtime expressions.
   * <p>
   * The provided expressions must be interpreted as a set of expressions that are ANDed together.
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
   * @param filters expressions used to filter the scan at runtime
   */
  def filter(filters: Seq[Expression]): Unit

  /**
   * This should never be called, catalyst filters take precedence.
   */
  def filter(predicates: Array[Predicate]): Unit = {}
}
