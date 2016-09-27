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

package org.apache.spark.sql.execution.window

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Projection


/**
 * Function for comparing boundary values.
 */
private[window] abstract class BoundOrdering {
  def compare(inputRow: InternalRow, inputIndex: Int, outputRow: InternalRow, outputIndex: Int): Int
}

/**
 * Compare the input index to the bound of the output index.
 */
private[window] final case class RowBoundOrdering(offset: Int) extends BoundOrdering {
  override def compare(
    inputRow: InternalRow,
    inputIndex: Int,
    outputRow: InternalRow,
    outputIndex: Int): Int =
    inputIndex - (outputIndex + offset)
}

/**
 * Compare the value of the input index to the value bound of the output index.
 */
private[window] final case class RangeBoundOrdering(
    ordering: Ordering[InternalRow],
    current: Projection,
    bound: Projection)
  extends BoundOrdering {

  override def compare(
    inputRow: InternalRow,
    inputIndex: Int,
    outputRow: InternalRow,
    outputIndex: Int): Int =
    ordering.compare(current(inputRow), bound(outputRow))
}
