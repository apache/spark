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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.util.HyperLogLogPlusPlusHelper
import org.apache.spark.sql.types.LongType


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
 * @param child the serialized HLL++ sketch, in a binary column with format:
 *
 *            | int    | double     | long * numWords          |
 *            | format | relativeSD | HLL[0] ... HLL[numWords] |
 *
 * @param relativeSD the maximum relative standard deviation allowed; should be less than or equal
 *                   to the relativeSD used when writing the HLL++ sketch.
 */
// scalastyle:on
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the estimated cardinality of the HyperLogLog++ sketch.""",
  examples = """
    Examples:
      > SELECT _FUNC_(approx_count_distinct_sketch(col1))
        FROM VALUES (1), (1), (2), (2), (3) tab(col1);
       3
  """,
  group = "agg_funcs",
  since = "3.3.1")
case class HyperLogLogPlusPlusAggSketch(
    child: Expression,
    var relativeSD: Double = HyperLogLogPlusPlus.defaultRelativeSD,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends HyperLogLogPlusPlusTrait {

  def this(child: Expression) = {
    this(
      child = child,
      relativeSD = HyperLogLogPlusPlus.defaultRelativeSD,
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0
    )
  }

  def this(child: Expression, relativeSD: Expression) = {
    this(
      child = child,
      relativeSD = HyperLogLogPlusPlus.validateDoubleLiteral(relativeSD),
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0
    )
  }

  override def prettyName: String = "approx_count_distinct_agg_sketch"

  /**
   * Allocate enough words to store all registers, plus one extra long
   * allowing us to check buffer compatibility during merge. This is mildly
   * different from the HyperLogLogPlusPlus trait which doesn't store numWords
   * at the head of the aggregation buffer.
   */
  override val aggBufferAttributes: Seq[AttributeReference] = {
    Seq.tabulate(hllppHelper.numWords + 1) { i =>
      AttributeReference(s"MS[$i]", LongType)()
    }
  }

  // Note: although this simply copies aggBufferAttributes, this common code can not be placed
  // in the superclass because that will lead to initialization ordering issues.
  override val inputAggBufferAttributes: Seq[AttributeReference] =
  aggBufferAttributes.map(_.newInstance())

  /**
   * This aggregate merges the HLL++ sketches that were written by a previous
   * HyperLogLogPlusPlusSketch instance, which utilized a HLL++ helper that may have been
   * configured differently than the one provided by our super class. We'll maintain the
   * relativeSD and create a new helper for the first sketch that we deserialize, ensuring
   * that every following sketch is aggregateable and can fit within the original agg buffer.
   */
  private var relativeSDUpdated: Boolean = false

  /**
   * Update the HLL++ buffer; input is expected to be the serialized
   * HLL++ sketch, which we'll merge into the shared agg buffer.
   */
  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val v = child.eval(input)
    if (v != null) {

      val (newRelativeSD, newInternalRow) =
        HyperLogLogPlusPlusSketch.deserializeSketch(v.asInstanceOf[Array[Byte]])

      relativeSDUpdated match {
        case true =>
          val newNumWords = new HyperLogLogPlusPlusHelper(newRelativeSD).numWords
          if (newNumWords != hllppHelper.numWords) {
            throw new IllegalStateException(s"""One or more of the HyperLogLogPlusPlusSketch
              sketches were configured with different relativeSD values, and cannot be
              aggregated: new SD ($newRelativeSD) previous SD($relativeSD)""")
          }

          // When merging into our agg buffer, skip the first long as that's the
          // numWords value which shouldn't be overwritten; the deserialized row
          // is formatted without the leading long so no skipping is necessary.
          hllppHelper.merge(buffer1 = buffer, buffer2 = newInternalRow,
              offset1 = mutableAggBufferOffset + 1, offset2 = 0)

        case false =>
          val newNumWords = new HyperLogLogPlusPlusHelper(newRelativeSD).numWords
          if (newNumWords > hllppHelper.numWords) {
            throw new IllegalStateException(s"""The HyperLogLogPlusPlusAggSketch function
              must be configured with a relativeSD value that is less than or equal to the
              sketches written by a previous instance of the HyperLogLogPlusPlusSketch
              function: new SD ($newRelativeSD) configured SD ($relativeSD)""")
          }

          // Update our hllppHelper to reflect the first sketch's configuration.
          relativeSD = newRelativeSD
          hllppHelper = new HyperLogLogPlusPlusHelper(relativeSD)
          relativeSDUpdated = true

          // First store the numWords value into the agg buffer.
          buffer.setLong(mutableAggBufferOffset, hllppHelper.numWords)

          // Then fill the rest of the agg buffer with the words from the sketch.
          (0 until hllppHelper.numWords).foreach(i => {
            buffer.setLong(mutableAggBufferOffset + 1 + i, newInternalRow.getLong(i))
          })
      }
    }
  }

  /**
   * Merges two aggregation buffers; the format of the aggregation buffers is the same as the
   * HyperLogLogPlusPlus implementation with the minor difference that the numWords associated
   * with the HLL instance is stored at the head of the buffer to support compatibility checks.
   */
  override def merge(buffer1: InternalRow, buffer2: InternalRow): Unit = {
    val numWords1 = buffer1.getLong(mutableAggBufferOffset)
    val numWords2 = buffer2.getLong(inputAggBufferOffset)
    if (numWords1 != 0 && numWords2 != 0 && numWords1 != numWords2) {
      throw new IllegalStateException(s"""One or more of the HyperLogLogPlusPlusSketch
        sketches were configured with different relativeSD values and cannot
        be aggregated: new numWords ($numWords2) current numWords ($numWords1)""")
    }

    // We need to explicitly set numWords so that the buffer retains it between merges.
    buffer1.setLong(mutableAggBufferOffset, if (numWords1 == 0) numWords2 else numWords1)

    // When merging agg buffers, skip the first long of both as they're the same numWords value.
    hllppHelper.merge(buffer1 = buffer1, buffer2 = buffer2,
      offset1 = mutableAggBufferOffset + 1, offset2 = inputAggBufferOffset + 1)
  }

  override def eval(buffer: InternalRow): Any = {
    // When evaling agg buffer, skip the first long containing the numWords value.
    hllppHelper.query(buffer, mutableAggBufferOffset + 1)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): HyperLogLogPlusPlusAggSketch =
    copy(child = newChild)

}
