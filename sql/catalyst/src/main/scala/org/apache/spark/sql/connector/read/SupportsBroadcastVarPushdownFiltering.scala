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

package org.apache.spark.sql.connector.read

import java.util
import java.util.Collections

import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.types.StructType



trait SupportsBroadcastVarPushdownFiltering {

  def filter(predicates: Array[Predicate]): Unit

  def hasPushedBroadCastFilter(): Boolean = false

  def allAttributes(): Array[NamedReference] = new Array[NamedReference](0)

  /**
   * This method should be implemented by the DataSourceV2 Scan which should check for equality
   * of Scan without taking into account pushed runtime filters (DPP)
   *
   * @param other scan to be compared to
   * @return boolean if the scans are same.
   */
  def equalToIgnoreRuntimeFilters(other: SupportsBroadcastVarPushdownFiltering): Boolean =
    this == other


  /**
   * This method should be implemented by the DataSourceV2 Scan to return the hashCode excluding
   * the runtime filters (DPP) pushed to scan.
   *
   * @return int
   */
  def hashCodeIgnoreRuntimeFilters(): Int = this.hashCode

  def getPushedBroadcastFilters(): util.List[PushedBroadcastFilterData] = Collections.emptyList

  def getPushedBroadcastVarIds(): util.Set[java.lang.Long] = Collections.emptySet

  def getPushedBroadcastFiltersCount(): Int = 0

  def postAllBroadcastVarsPushed(): Unit = {}

  def partitionAttributes(): Array[NamedReference] = new Array[NamedReference](0)

  def readSchema: StructType
}
