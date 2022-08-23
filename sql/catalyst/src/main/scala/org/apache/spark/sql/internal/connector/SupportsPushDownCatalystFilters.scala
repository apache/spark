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

/**
 * A mix-in interface for {@link FileScanBuilder}. File sources can implement this interface to
 * push down filters to the file source. The pushed down filters will be separated into partition
 * filters and data filters. Partition filters are used for partition pruning and data filters are
 * used to reduce the size of the data to be read.
 */
trait SupportsPushDownCatalystFilters {

  /**
   * Pushes down catalyst Expression filters (which will be separated into partition filters and
   * data filters), and returns data filters that need to be evaluated after scanning.
   */
  def pushFilters(filters: Seq[Expression]): Seq[Expression]

  /**
   * Returns the data filters that are pushed to the data source via
   * {@link #pushFilters(Seq[Expression])}.
   */
  def pushedFilters: Array[Predicate]
}
