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

package org.apache.spark.sql.catalyst.expressions.aggregate

import java.nio.ByteBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, GenericInternalRow}
import org.apache.spark.sql.types.{BinaryType, DataType}


// scalastyle:off
/**
 * HyperLogLog++ (HLL++) is a state of the art cardinality estimation algorithm. This class
 * implements the dense version of the HLL++ algorithm as an Aggregate Function, and outputs
 * the underlying HLL sketch for downstream re-aggregation and evaluation.
 *
 * This implementation has been based on the following papers:
 * HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm
 * http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 *
 * HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation
 * Algorithm
 * http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en/us/pubs/archive/40671.pdf
 *
 * Appendix to HyperLogLog in Practice: Algorithmic Engineering of a State of the Art Cardinality
 * Estimation Algorithm
 * https://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen#
 *
 * @param child to estimate the cardinality of.
 * @param relativeSD the maximum relative standard deviation allowed.
 */
// scalastyle:on
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, relativeSD]) - Returns the aggregateable HyperLogLog++ sketch as a binary column.
      `relativeSD` defines the maximum relative standard deviation allowed.""",
  examples = """
    Examples:
      > SELECT approx_count_distinct(_FUNC_(col1)) FROM VALUES (1), (1), (2), (2), (3) tab(col1);
       3
  """,
  group = "agg_funcs",
  since = "3.3.1")
case class HyperLogLogPlusPlusSketch(
    child: Expression,
    relativeSD: Double = 0.05,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends HyperLogLogPlusPlusTrait {

  import HyperLogLogPlusPlusSketch._

  def this(child: Expression, relativeSD: Expression) = {
    this(
      child = child,
      relativeSD = HyperLogLogPlusPlus.validateDoubleLiteral(relativeSD)
    )
  }

  override def prettyName: String = "approx_count_distinct_sketch"

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = true

  /**
   * Generate the HyperLogLog sketch.
   */
  override def eval(buffer: InternalRow): Any = {
    serializeSketch(relativeSD, hllppHelper.numWords, buffer, mutableAggBufferOffset)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): HyperLogLogPlusPlusSketch =
    copy(child = newChild)

}

object HyperLogLogPlusPlusSketch {
  def serializeSketch(relativeSD: Double,
                      numWords: Integer,
                      buffer: InternalRow,
                      mutableAggBufferOffset: Integer): Array[Byte] = {
    // Trino/Presto utilize the Airlift implementation of HLL; Airlift's
    // supported HLL storage formats are available in this doc:
    // https://github.com/airlift/airlift/blob/master/stats/docs/hll.md
    //
    // For the Spark implementation, we'll store the HLL sketch in a format that
    // is specific to the fields accessible to us via the HyperLogLogPlusPlusTrait,
    // and use the first int to store a format value of 1 such that we can revisit
    // and add support for interoperable formats in the future
    val byteBuffer = ByteBuffer.allocate(
      Integer.BYTES + java.lang.Double.BYTES + (numWords * java.lang.Double.BYTES))
    byteBuffer.putInt(1)
    byteBuffer.putDouble(relativeSD)
    Seq.tabulate(numWords) { i =>
      byteBuffer.putLong(buffer.getLong(mutableAggBufferOffset + i))
    }

    byteBuffer.array()
  }

  def deserializeSketch(sketch: Array[Byte]):
    (Double, InternalRow) = {

    val byteBuffer = ByteBuffer.wrap(sketch)
    byteBuffer.getInt() match {
      case 1 =>
        val relativeSD = byteBuffer.getDouble()
        val row = new GenericInternalRow() {
          override def getLong(ordinal: Int): Long = {
            byteBuffer.getLong(
              Integer.BYTES + java.lang.Double.BYTES + (ordinal * java.lang.Double.BYTES))
          }
        }

        (relativeSD, row)
      case _ => throw new UnsupportedOperationException()
    }


  }
}