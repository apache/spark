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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.HyperLogLogPlusPlusHelper
import org.apache.spark.sql.types.{DataType, LongType, StructType}

// scalastyle:off
/**
 * This trait implements the core HLL++ functionality, such that extending classes
 * need only implement constructors, copy constructors, and override the methods
 * from {@link ImperativeAggregate} or {@link Expression} based on their individual needs.
 */
// scalastyle:on
trait HyperLogLogPlusPlusTrait extends ImperativeAggregate with UnaryLike[Expression] {

  def child: Expression

  def relativeSD: Double

  // Protected var such that HyperLogLogPlusPlusAggSketch
  // can override at runtime based on input sketches
  protected[aggregate] var hllppHelper: HyperLogLogPlusPlusHelper =
    new HyperLogLogPlusPlusHelper(relativeSD)

  override def prettyName: String = "approx_count_distinct"

  override def dataType: DataType = LongType

  override def nullable: Boolean = false

  override def defaultResult: Option[Literal] = Option(Literal.create(0L, dataType))

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  /** Allocate enough words to store all registers. */
  override def aggBufferAttributes: Seq[AttributeReference] = {
    Seq.tabulate(hllppHelper.numWords) { i =>
      AttributeReference(s"MS[$i]", LongType)()
    }
  }

  // Note: although this simply copies aggBufferAttributes, this common code can not be placed
  // in the superclass because that will lead to initialization ordering issues.
  override def inputAggBufferAttributes: Seq[AttributeReference] =
  aggBufferAttributes.map(_.newInstance())

  /** Fill all words with zeros. */
  override def initialize(buffer: InternalRow): Unit = {
    var word = 0
    while (word < hllppHelper.numWords) {
      buffer.setLong(mutableAggBufferOffset + word, 0)
      word += 1
    }
  }

  /**
   * Update the HLL++ buffer.
   */
  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val v = child.eval(input)
    if (v != null) {
      hllppHelper.update(buffer, mutableAggBufferOffset, v, child.dataType)
    }
  }

  /**
   * Merge the HLL++ buffers.
   */
  override def merge(buffer1: InternalRow, buffer2: InternalRow): Unit = {
    hllppHelper.merge(buffer1 = buffer1, buffer2 = buffer2,
      offset1 = mutableAggBufferOffset, offset2 = inputAggBufferOffset)
  }

  /**
   * Compute the HyperLogLog estimate.
   */
  override def eval(buffer: InternalRow): Any = {
    hllppHelper.query(buffer, mutableAggBufferOffset)
  }
}
