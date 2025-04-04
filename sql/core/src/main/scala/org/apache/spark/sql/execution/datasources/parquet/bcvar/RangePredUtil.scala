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

object RangeInPredUtil {
  private val KEEP = true
  private val DISCARD = false

  def isInRange[T](
      lowerBoundSupplier : => T,
      upperBoundSupplier: => T,
      rangeSet: util.NavigableSet[T],
      keepIfBoundsNull: Boolean,
      comparator: Comparator[T]): Boolean = {
    if (rangeSet.isEmpty) return DISCARD
    val lower = lowerBoundSupplier
    val upper = upperBoundSupplier
    if (lower != null && upper != null) {
      val leastElementGELower = rangeSet.ceiling(lower)
      if (leastElementGELower == null) DISCARD
      else if (comparator.compare(leastElementGELower, lower) == 0) KEEP
      else if (comparator.compare(leastElementGELower, upper) > 0) DISCARD
      else KEEP
    }
    else if (upper != null) if (rangeSet.floor(upper) != null) KEEP
    else DISCARD
    else if (lower != null) if (rangeSet.ceiling(lower) != null) KEEP
    else DISCARD
    else if (keepIfBoundsNull) KEEP
    else DISCARD
  }

  def isStrictlyInRange[T](
      lowerBoundSupplier: => T,
      upperBoundSupplier: => T,
      rangeSet: util.NavigableSet[T],
      keepIfBoundsNull: Boolean,
      comparator: Comparator[T]): Boolean = {
    if (rangeSet.isEmpty) return DISCARD
    val lower = lowerBoundSupplier
    val upper = upperBoundSupplier
    if (rangeSet.contains(lower) && rangeSet.contains(upper) &&
      comparator.compare(lower, upper) == 0) {
      KEEP
    }
    else {
      DISCARD
    }
  }
}

