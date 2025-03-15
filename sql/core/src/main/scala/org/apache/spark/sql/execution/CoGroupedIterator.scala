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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering

/**
 * Iterates over [[GroupedIterator]]s and returns the cogrouped data, i.e. each record is a
 * grouping key with its associated values from all [[GroupedIterator]]s.
 * Note: we assume the output of each [[GroupedIterator]] is ordered by the grouping key.
 */
class CoGroupedIterator(
    groupedIterators: Seq[Iterator[(InternalRow, Iterator[InternalRow])]],
    groupingSchema: Seq[Attribute])
    extends Iterator[(InternalRow, Seq[Iterator[InternalRow]])] {

  private val keyOrdering =
    GenerateOrdering.generate(groupingSchema.map(SortOrder(_, Ascending)), groupingSchema)

  private var currentData: Seq[(InternalRow, Iterator[InternalRow])] =
    Seq.fill(groupedIterators.length)(null)

  override def hasNext: Boolean = {
    currentData = currentData.zip(groupedIterators).map { case (currentDatum, groupedIterator) =>
      if (currentDatum == null && groupedIterator.hasNext) {
        groupedIterator.next()
      } else {
        currentDatum
      }
    }

    currentData.exists(_ != null)
  }

  override def next(): (InternalRow, Seq[Iterator[InternalRow]]) = {
    assert(hasNext)

    val minGroup = currentData.filter(_ != null).map(_._1).min(keyOrdering)
    val minGroupData = currentDataInMinGroup(minGroup)

    (minGroup, minGroupData)
  }

  private def currentDataInMinGroup(minGroup: InternalRow): Seq[Iterator[InternalRow]] = {
    val (currentDataInMinGroup, currentDataWithoutMinGroup) = currentData.map { currentDatum =>
      if (currentDatum != null && currentDatum._1 == minGroup) {
        (currentDatum._2, null)
      } else {
        (Iterator.empty, currentDatum)
      }
    }.unzip

    currentData = currentDataWithoutMinGroup

    currentDataInMinGroup
  }
}
