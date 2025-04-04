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
package org.apache.spark.sql.execution.datasources.parquet.bcvar

import java.util
import java.util.Comparator

import org.apache.parquet.filter2.predicate.Operators.Column
import org.apache.parquet.filter2.predicate.Statistics
import org.apache.parquet.filter2.predicate.UserDefinedPredicate

import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper

class RangeInFilter[T <: Comparable[T]](
    private val bcVar: BroadcastedJoinKeysWrapper,
    column: Column[T],
    catalystToParquetFormatConverter: Any => T = (x: Any) => x.asInstanceOf[T]) extends
  UserDefinedPredicate[T] with Serializable {

  private lazy val rangeSet: util.NavigableSet[T] = BroadcastVarCache.getNavigableSet(bcVar,
    column, catalystToParquetFormatConverter)

  def this() = this(null, null)

  override def keep(t: T): Boolean = this.rangeSet.contains(t)

  override def canDrop(statistics: Statistics[T]): Boolean = {
    val lowerBoundsSupplier = statistics.getMin
    val upperBoundsSupplier = statistics.getMax
    !RangeInPredUtil.isInRange[T](lowerBoundsSupplier, upperBoundsSupplier, rangeSet, true,
      this.rangeSet.comparator().asInstanceOf[Comparator[T]])
  }

  override def inverseCanDrop(statistics: Statistics[T]): Boolean =
    throw new UnsupportedOperationException("Not implemented")
}
