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

package org.apache.spark.sql.execution.aggregate

import scala.language.existentials

import org.apache.spark.Logging
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{OuterScopes, encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

object TypedAggregateExpression {
  def apply[A, B : Encoder, C : Encoder](
      aggregator: Aggregator[A, B, C]): TypedAggregateExpression = {
    new TypedAggregateExpression(
      aggregator.asInstanceOf[Aggregator[Any, Any, Any]],
      None,
      encoderFor[B].asInstanceOf[ExpressionEncoder[Any]],
      encoderFor[C].asInstanceOf[ExpressionEncoder[Any]],
      Nil,
      0,
      0)
  }
}

/**
 * This class is a rough sketch of how to hook `Aggregator` into the Aggregation system.  It has
 * the following limitations:
 *  - It assumes the aggregator has a zero, `0`.
 */
case class TypedAggregateExpression(
    aggregator: Aggregator[Any, Any, Any],
    aEncoder: Option[ExpressionEncoder[Any]], // Should be bound.
    unresolvedBEncoder: ExpressionEncoder[Any],
    cEncoder: ExpressionEncoder[Any],
    children: Seq[Attribute],
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int)
  extends ImperativeAggregate with Logging {

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = true

  override def dataType: DataType = if (cEncoder.flat) {
    cEncoder.schema.head.dataType
  } else {
    cEncoder.schema
  }

  override def deterministic: Boolean = true

  override lazy val resolved: Boolean = aEncoder.isDefined

  override lazy val inputTypes: Seq[DataType] = Nil

  override val aggBufferSchema: StructType = unresolvedBEncoder.schema

  override val aggBufferAttributes: Seq[AttributeReference] = aggBufferSchema.toAttributes

  val bEncoder = unresolvedBEncoder
    .resolve(aggBufferAttributes, OuterScopes.outerScopes)
    .bind(aggBufferAttributes)

  // Note: although this simply copies aggBufferAttributes, this common code can not be placed
  // in the superclass because that will lead to initialization ordering issues.
  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  // We let the dataset do the binding for us.
  lazy val boundA = aEncoder.get

  private def updateBuffer(buffer: MutableRow, value: InternalRow): Unit = {
    var i = 0
    while (i < aggBufferAttributes.length) {
      val offset = mutableAggBufferOffset + i
      aggBufferSchema(i).dataType match {
        case BooleanType => buffer.setBoolean(offset, value.getBoolean(i))
        case ByteType => buffer.setByte(offset, value.getByte(i))
        case ShortType => buffer.setShort(offset, value.getShort(i))
        case IntegerType => buffer.setInt(offset, value.getInt(i))
        case LongType => buffer.setLong(offset, value.getLong(i))
        case FloatType => buffer.setFloat(offset, value.getFloat(i))
        case DoubleType => buffer.setDouble(offset, value.getDouble(i))
        case other => buffer.update(offset, value.get(i, other))
      }
      i += 1
    }
  }

  override def initialize(buffer: MutableRow): Unit = {
    val zero = bEncoder.toRow(aggregator.zero)
    updateBuffer(buffer, zero)
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    val inputA = boundA.fromRow(input)
    val currentB = bEncoder.shift(mutableAggBufferOffset).fromRow(buffer)
    val merged = aggregator.reduce(currentB, inputA)
    val returned = bEncoder.toRow(merged)

    updateBuffer(buffer, returned)
  }

  override def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    val b1 = bEncoder.shift(mutableAggBufferOffset).fromRow(buffer1)
    val b2 = bEncoder.shift(inputAggBufferOffset).fromRow(buffer2)
    val merged = aggregator.merge(b1, b2)
    val returned = bEncoder.toRow(merged)

    updateBuffer(buffer1, returned)
  }

  override def eval(buffer: InternalRow): Any = {
    val b = bEncoder.shift(mutableAggBufferOffset).fromRow(buffer)
    val result = cEncoder.toRow(aggregator.finish(b))
    dataType match {
      case _: StructType => result
      case _ => result.get(0, dataType)
    }
  }

  override def toString: String = {
    s"""${aggregator.getClass.getSimpleName}(${children.mkString(",")})"""
  }

  override def nodeName: String = aggregator.getClass.getSimpleName
}
