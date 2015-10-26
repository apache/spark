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
import org.apache.spark.sql.catalyst.expressions.{Ascending, SortOrder, Attribute}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering

private[sql] class CoGroupedIterator(
    groupedInputs: Seq[Iterator[(InternalRow, Iterator[InternalRow])]],
    groupingSchema: Seq[Attribute])
  extends Iterator[(InternalRow, Seq[Iterator[InternalRow]])] {

  private val keyOrdering =
    GenerateOrdering.generate(groupingSchema.map(SortOrder(_, Ascending)), groupingSchema)

  private val currentGroupedInputs =
    Array.fill(groupedInputs.length)(null: (InternalRow, Iterator[InternalRow]))

  private val result = Array.fill(groupedInputs.length)(null: Iterator[InternalRow])

  override def hasNext: Boolean = groupedInputs.exists(_.hasNext)

  override def next(): (InternalRow, Seq[Iterator[InternalRow]]) = {
    var index = 0
    while (index < currentGroupedInputs.length) {
      if ((currentGroupedInputs(index) eq null) && groupedInputs(index).hasNext) {
        currentGroupedInputs(index) = groupedInputs(index).next()
      }
      index += 1
    }

    assert(currentGroupedInputs.exists(_ ne null))

    val minKey = currentGroupedInputs.view.filter(_ ne null).map(_._1).min(keyOrdering)

    index = 0
    while (index < currentGroupedInputs.length) {
      val currentInput = currentGroupedInputs(index)
      if ((currentInput ne null) && currentInput._1 == minKey) {
        result(index) = currentInput._2
        currentGroupedInputs(index) = null
      } else {
        result(index) = Iterator.empty
      }
      index += 1
    }

    minKey -> result
  }
}
