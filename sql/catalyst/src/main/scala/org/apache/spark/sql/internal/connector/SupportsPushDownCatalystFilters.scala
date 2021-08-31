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

/**
 * A mix-in interface for {@link FileScanBuilder}. This can be used to push down partitionFilters
 * and dataFilters to FileIndex in the format of catalyst Expression.
 */
trait SupportsPushDownCatalystFilters {
  /**
   * Pushes down partitionFilters and dataFilters to FileIndex in the format of catalyst
   * Expression. These catalyst Expression filters are used for partition pruning. The dataFilters
   * are also translated into data source filters and used for selecting records.
   */
  def pushCatalystFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Unit
}
