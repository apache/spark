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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.collection.OpenHashMap

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the concatenated input values," +
    " separated by the delimiter string.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES ('a'), ('b'), ('c') AS tab(col);
       a,b,c
      > SELECT _FUNC_(col) FROM VALUES (NULL), ('a'), ('b') AS tab(col);
       a,b
      > SELECT _FUNC_(col, '|') FROM VALUES ('a'), ('b') AS tab(col);
       a|b
      > SELECT _FUNC_(col) FROM VALUES (NULL), (NULL) AS tab(col);
       NULL
  """,
  group = "agg_funcs",
  since = "4.0.0")
case class ListAgg(
    child: Expression,
    delimiter: Expression = Literal.create(",", StringType),
    reverse: Boolean = false,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends TypedAggregateWithHashMapAsBuffer
  with UnaryLike[Expression] {

  def this(child: Expression) = this(child, Literal.create(",", StringType), false, 0, 0)
  def this(child: Expression, delimiter: Expression) = this(child, delimiter, false, 0, 0)

  override def update(
      buffer: OpenHashMap[AnyRef, Long],
      input: InternalRow): OpenHashMap[AnyRef, Long] = {
    val value = child.eval(input)
    if (value != null) {
      val key = InternalRow.copyValue(value)
      buffer.changeValue(key.asInstanceOf[AnyRef], 1L, _ + 1L)
    }
    buffer
  }

  override def merge(
      buffer: OpenHashMap[AnyRef, Long],
      input: OpenHashMap[AnyRef, Long]): OpenHashMap[AnyRef, Long] = {
    input.foreach { case (key, count) =>
      buffer.changeValue(key, count, _ + count)
    }
    buffer
  }

  override def eval(buffer: OpenHashMap[AnyRef, Long]): Any = {
    if (buffer.nonEmpty) {
      val ordering = PhysicalDataType.ordering(child.dataType)
      val sortedCounts = if (reverse) {
        buffer.toSeq.sortBy(_._1)(ordering.asInstanceOf[Ordering[AnyRef]].reverse)
      } else {
        buffer.toSeq.sortBy(_._1)(ordering.asInstanceOf[Ordering[AnyRef]])
      }
      UTF8String.fromString(sortedCounts.map(kc => {
        List.fill(kc._2.toInt)(kc._1.toString).mkString(delimiter.eval()
          .asInstanceOf[UTF8String].toString)
      }).mkString(delimiter.eval().asInstanceOf[UTF8String].toString))
    } else {
      null
    }
  }

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int) : ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = true

  override def dataType: DataType = StringType

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
