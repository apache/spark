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
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap

/**
 * Mode in Pandas' fashion.
 * This expression is dedicated only for PySpark and Spark-ML.
 */
case class GroupedCount(
    child: Expression,
    ignoreNulls: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedAggregateWithHashMapAsBuffer with BinaryLike[Expression] {

  def this(child: Expression) = this(child, Literal.create(true), 0, 0)
  def this(child: Expression, ignoreNulls: Expression) = this(child, ignoreNulls, 0, 0)

  // Returns empty map for empty inputs
  override def nullable: Boolean = false

  override def dataType: DataType =
    MapType(child.dataType, LongType, valueContainsNull = false)

  override def prettyName: String = "grouped_count"

  private lazy val ignore = ignoreNulls.eval().asInstanceOf[Boolean]

  override def update(
      buffer: OpenHashMap[AnyRef, Long],
      input: InternalRow): OpenHashMap[AnyRef, Long] = {
    val key = child.eval(input)
    if (key != null) {
      buffer.changeValue(InternalRow.copyValue(key).asInstanceOf[AnyRef], 1L, _ + 1L)
    } else if (!ignore) {
      buffer.changeValue(null, 1L, _ + 1L)
    }
    buffer
  }

  override def merge(
      buffer: OpenHashMap[AnyRef, Long],
      other: OpenHashMap[AnyRef, Long]): OpenHashMap[AnyRef, Long] = {
    other.foreach { case (key, count) =>
      buffer.changeValue(key, count, _ + count)
    }
    buffer
  }

  override def eval(buffer: OpenHashMap[AnyRef, Long]): Any = {
    val size = buffer.size
    if (size == 0) {
      return ArrayBasedMapData(Array.empty, Array.empty)
    }
    val keys = Array.ofDim[AnyRef](size)
    val counts = Array.ofDim[Long](size)
    val iter = buffer.iterator
    var index = 0
    while (iter.hasNext) {
      val (key, count) = iter.next()
      keys(index) = key
      counts(index) = count
      index += 1
    }
    ArrayBasedMapData(keys, counts)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def left: Expression = child

  override def right: Expression = ignoreNulls

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression =
    copy(child = newLeft, ignoreNulls = newRight)
}
