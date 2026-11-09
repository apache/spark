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

/**
 * Compare the value of the input index to the value bound of the output index, for a range frame
 * ordered by a single expression of an integral type, with an integral offset.
 *
 * Unlike [[RangeBoundOrdering]], the bound (the value of the output row plus the offset, or minus
 * the offset in descending order) is not computed in the type of the order expression, where it
 * could overflow. The comparison is exact for all values: a bound beyond the range of the type is
 * greater or smaller than every value of the type, and the offset does not need to fit in the
 * type. NULL values compare as in [[RangeBoundOrdering]], where the bound of a NULL value is NULL.
 *
 * @param current projection that returns the value of the order expression as a LONG.
 * @param offset the offset of the bound.
 * @param ascending whether the order expression is sorted in ascending order.
 * @param nullsFirst whether NULL values of the order expression are sorted first.
 */
private[window] final case class IntegralRangeBoundOrdering(
    current: Projection,
    offset: Long,
    ascending: Boolean,
    nullsFirst: Boolean)
  extends BoundOrdering {

  override def compare(
      inputRow: InternalRow,
      inputIndex: Int,
      outputRow: InternalRow,
      outputIndex: Int): Int = {
    // `current` reuses its result row, so read the input value before projecting the output row.
    val input = current(inputRow)
    val inputIsNull = input.isNullAt(0)
    val inputValue = if (inputIsNull) 0L else input.getLong(0)
    val output = current(outputRow)
    val outputIsNull = output.isNullAt(0)
    if (inputIsNull && outputIsNull) {
      0
    } else if (inputIsNull) {
      if (nullsFirst) -1 else 1
    } else if (outputIsNull) {
      if (nullsFirst) 1 else -1
    } else if (ascending) {
      IntegralRangeBoundOrdering.compareToSum(inputValue, output.getLong(0), offset)
    } else {
      // In descending order the bound is the value minus the offset, and greater values come
      // first.
      -IntegralRangeBoundOrdering.compareToDifference(inputValue, output.getLong(0), offset)
    }
  }
}

private[window] object IntegralRangeBoundOrdering {

  /**
   * Returns the sign of `x - (v + d)`, computed without overflow.
   */
  def compareToSum(x: Long, v: Long, d: Long): Int = {
    val sum = v + d
    // The addition overflows if and only if `v` and `d` have the same sign and `sum` does not.
    if (((v ^ sum) & (d ^ sum)) < 0) {
      if (d > 0) -1 else 1
    } else {
      java.lang.Long.compare(x, sum)
    }
  }

  /**
   * Returns the sign of `x - (v - d)`, computed without overflow.
   */
  def compareToDifference(x: Long, v: Long, d: Long): Int = {
    val difference = v - d
    // The subtraction overflows if and only if `v` and `d` have different signs and `difference`
    // does not have the sign of `v`.
    if (((v ^ d) & (v ^ difference)) < 0) {
      if (d < 0) -1 else 1
    } else {
      java.lang.Long.compare(x, difference)
    }
  }
}
