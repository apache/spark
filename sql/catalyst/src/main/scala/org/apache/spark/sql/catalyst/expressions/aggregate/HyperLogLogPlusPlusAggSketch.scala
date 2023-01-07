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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.util.HyperLogLogPlusPlusHelper


// scalastyle:off
/**
 * HyperLogLog++ (HLL++) is a state of the art cardinality estimation algorithm. This class
 * implements the dense version of the HLL++ algorithm as an Aggregate Function. It utilizes
 * HLL++ sketches generated from the {@link HyperLogLogPlusPlusSketch} function and outputs
 * the estimated distinct count of those sketches post aggregation.
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
 * @param child the HLL++ sketch to be aggregated and evaluated.
 */
// scalastyle:on
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the estimated cardinality of the HyperLogLog++ sketch.""",
  examples = """
    Examples:
      > SELECT _FUNC_(approx_count_distinct_sketch(col1)) FROM VALUES (1), (1), (2), (2), (3) tab(col1);
       3
  """,
  group = "agg_funcs",
  since = "3.3.1")
case class HyperLogLogPlusPlusAggSketch(
    child: Expression,
    relativeSD: Double = 0.05,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends HyperLogLogPlusPlusTrait {

  def this(child: Expression, relativeSD: Expression) = {
    this(
      child = child,
      relativeSD = HyperLogLogPlusPlus.validateDoubleLiteral(relativeSD)
    )
  }

  override def prettyName: String = "approx_count_distinct_agg_sketch"

  /**
   * This aggregate merges the HLL++ sketches that were written by a previous
   * HyperLogLogPlusPlusSketch instance, which utilized a HLL++ helper that may have been
   * configured differently than the one provided by our super class. Because we can't
   * re-instantiate the super class's HLL++ helper, let's maintain the first instance that
   * we deserialize, and ensure that our instance is able to fit within the words
   * allocated in the aggregation buffer by the super class's HLL++ helper instance.
   */
  private[this] var derivedHllppHelper: Option[HyperLogLogPlusPlusHelper] = None

  /**
   * Update the HLL++ buffer.
   */
  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val v = child.eval(input)
    if (v != null) {
      val (newHllppHelper, newInternalRow) = HyperLogLogPlusPlusSketch
        .generateHyperLogLogPlusPlusHelper(v.asInstanceOf[Array[Byte]])

      derivedHllppHelper match {
        case Some(existingHllppHelper) =>
          if (existingHllppHelper.numWords != newHllppHelper.numWords) {
            throw new IllegalStateException("One or more of the HyperLogLogPlusPlusSketch " +
              "sketches were configured with different relative precision values, " +
              "and cannont be aggregated")
          }

          existingHllppHelper.merge(buffer1 = buffer, buffer2 = newInternalRow,
              offset1 = mutableAggBufferOffset, offset2 = 0)

        case None =>
          if (this.hllppHelper.numWords < newHllppHelper.numWords) {
            throw new IllegalStateException("The HyperLogLogPlusPlusAggSketch function " +
              "must be configured with more relative precision than the sketches written " +
              "by a previous instance of the HyperLogLogPlusPlusSketch function")
          }

          (0 until newHllppHelper.numWords).foreach(i => {
            buffer.setLong(mutableAggBufferOffset + i, newInternalRow.getLong(i))
          })
          derivedHllppHelper = Some(hllppHelper)
      }
    }
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): HyperLogLogPlusPlusAggSketch =
    copy(child = newChild)

}
