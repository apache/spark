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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering

/**
 * Iterates over [[GroupedIterator]]s and returns the cogrouped data, i.e. each record is a
 * grouping key with its associated values from all [[GroupedIterator]]s. Note: we assume the
 * output of each [[GroupedIterator]] is ordered by the grouping key.
 */
class CoGroupedIterator(
                              iterators: List[Iterator[(InternalRow, Iterator[InternalRow])]],
                              groupingSchema: Seq[Attribute])
  extends Iterator[(InternalRow, List[Iterator[InternalRow]])] {

  private val keyOrdering =
    GenerateOrdering.generate(groupingSchema.map(SortOrder(_, Ascending)), groupingSchema)

  private var currentData: mutable.ListBuffer[(InternalRow, Iterator[InternalRow])] = _
  override def hasNext: Boolean = {
    if (currentData == null) {
      currentData = mutable.ListBuffer()
      iterators.foreach(_ => currentData += null)
    }
    iterators.zipWithIndex.foreach({ case (data, index) =>
      if (currentData(index) == null && data.hasNext) {
        currentData(index) = data.next()
      }
    })
    currentData.exists(_ != null)
  }

  @throws(classOf[RuntimeException])
  override def next(): (InternalRow, List[Iterator[InternalRow]]) = {
    assert(hasNext)
    val smallestKey: InternalRow = currentData
      .filter(_ != null)
      .map(_._1)
      .reduceLeft((row1: InternalRow, row2: InternalRow) => {
        val compare = keyOrdering.compare(row1, row2)
        if (compare == 0) {
          row1
        } else if (compare < 0) {
          row1
        } else {
          row2
        }
      })

    val dataInterator: List[Iterator[InternalRow]] = currentData.zipWithIndex.map({
      case (data: (InternalRow, Iterator[InternalRow]), index: Int) =>
        if (keyOrdering.compare(smallestKey, data._1) == 0) {
          currentData(index) = null
          data._2
        } else {
          Iterator.empty
        }
      case (_, _: Int) =>
        Iterator.empty
    }).toList

    (smallestKey, dataInterator)
  }
}
