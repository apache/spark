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

import org.apache.datasketches.hll.{HllSketch, TgtHllType, Union}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, DoubleType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String


case class DatasketchesHllSketch(
    child: Expression,
    lgConfigK: Int = HllSketch.DEFAULT_LG_K,
    tgtHllType: TgtHllType = HllSketch.DEFAULT_HLL_TYPE,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[HllSketch] with UnaryLike[Expression] {

  override def prettyName: String = "hllsketch"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override protected def withNewChildInternal(newChild: Expression):
    DatasketchesHllSketch = copy(child = newChild)

  /**
   * Creates an empty aggregation buffer object. This is called before processing each key group
   * (group by key).
   *
   * @return an aggregation buffer object
   */
  override def createAggregationBuffer(): HllSketch = {
    new HllSketch(lgConfigK, tgtHllType)
  }

  /**
   * Updates the aggregation buffer object with an input row and returns a new buffer object. For
   * performance, the function may do in-place update and return it instead of constructing new
   * buffer object.
   *
   * This is typically called when doing Partial or Complete mode aggregation.
   *
   * @param buffer The aggregation buffer object.
   * @param input  an input row
   */
  override def update(buffer: HllSketch, input: InternalRow): HllSketch = {
    val v = child.eval(input)
    if (v != null) {
      child.dataType match {
        // Implement update for all types supported by HllSketch
        // Spark doesn't have equivalent types for ByteBuffer or char[] so leave those out
        // TODO: Test all supported types, figure out if we should support all other SQL types
        // via direct calls to HllSketch.couponUpdate() ?
        case IntegerType => buffer.update(v.asInstanceOf[Int])
        case LongType => buffer.update(v.asInstanceOf[Long])
        case DoubleType => buffer.update(v.asInstanceOf[Double])
        case StringType => buffer.update(v.asInstanceOf[UTF8String].toString)
        case ArrayType(ByteType, _) => buffer.update(v.asInstanceOf[Array[Byte]])
        case ArrayType(IntegerType, _) => buffer.update(v.asInstanceOf[Array[Int]])
        case ArrayType(LongType, _) => buffer.update(v.asInstanceOf[Array[Long]])
        case _ => throw new UnsupportedOperationException()
      }
    }
    buffer
  }

  /**
   * Merges an input aggregation object into aggregation buffer object and returns a new buffer
   * object. For performance, the function may do in-place merge and return it instead of
   * constructing new buffer object.
   *
   * This is typically called when doing PartialMerge or Final mode aggregation.
   *
   * @param buffer the aggregation buffer object used to store the aggregation result.
   * @param input  an input aggregation object. Input aggregation object can be produced by
   *               de-serializing the partial aggregate's output from Mapper side.
   */
  override def merge(buffer: HllSketch, input: HllSketch): HllSketch = {
    // TODO: Error handling for sketches with different tgtHllType?
    val union = new Union(Math.max(buffer.getLgConfigK, input.getLgConfigK))
    union.update(buffer)
    union.update(input)
    union.getResult
  }

  /**
   * Generates the final aggregation result value for current key group with the aggregation buffer
   * object.
   *
   * Developer note: the only return types accepted by Spark are:
   *   - primitive types
   *   - InternalRow and subclasses
   *   - ArrayData
   *   - MapData
   *
   * @param buffer aggregation buffer object.
   * @return The aggregation result of current key group
   */
  override def eval(buffer: HllSketch): Any = {
    buffer.getEstimate.toLong
  }

  /** Serializes the aggregation buffer object T to Array[Byte] */
  override def serialize(buffer: HllSketch): Array[Byte] = {
    buffer.toCompactByteArray
  }

  /** De-serializes the serialized format Array[Byte], and produces aggregation buffer object T */
  override def deserialize(storageFormat: Array[Byte]): HllSketch = {
    HllSketch.heapify(storageFormat)
  }
}
