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
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.types.{BinaryType, DataType}


// scalastyle:off
/**
 * HyperLogLog++ (HLL++) is a state of the art cardinality estimation algorithm. This class
 * implements the dense version of the HLL++ algorithm as an Aggregate Function.
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
 * @param child a sketch generated to estimate the cardinality of.
 * @param relativeSD the maximum relative standard deviation allowed.
 */
// scalastyle:on
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, relativeSD]) - Returns the re-aggregateable HyperLogLog++ sketch.
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
    inputAggBufferOffset: Int = 0)
  extends HyperLogLogPlusPlusBase(child, relativeSD, mutableAggBufferOffset, inputAggBufferOffset) {

  override def prettyName: String = "approx_count_distinct_sketch"

  override def dataType: DataType = BinaryType

  override def defaultResult: Option[Literal] = Option(Literal.create(0L, dataType))

  /**
   * Generate the HyperLogLog sketch.
   */
  override def eval(buffer: InternalRow): Any = {
    // there's various formats proposed in this design, which we could attempt to mimic:
    // https://github.com/airlift/airlift/blob/master/stats/docs/hll.md
    // for now let's just store what's necessary to re-recreate this HLLPP instance
    val byteBuffer = ByteBuffer.allocate(8 + (hllppHelper.numWords * 8))
    byteBuffer.putDouble(relativeSD)
    Seq.tabulate(hllppHelper.numWords) { i =>
      byteBuffer.putLong(buffer.getLong(mutableAggBufferOffset + i))
    }

    byteBuffer.array()
  }

  // copy() methods are equivalent between implementations of HyperLogLogPlusPlusBase

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): HyperLogLogPlusPlusSketch =
    copy(child = newChild)

}