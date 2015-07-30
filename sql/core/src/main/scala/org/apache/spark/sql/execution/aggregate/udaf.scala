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

import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{InternalRow, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.catalyst.expressions.{MutableRow, InterpretedMutableProjection, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction2
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{Metadata, StructField, StructType, DataType}

/**
 * A Mutable [[Row]] representing an mutable aggregation buffer.
 */
private[sql] class MutableAggregationBufferImpl (
    schema: StructType,
    toCatalystConverters: Array[Any => Any],
    toScalaConverters: Array[Any => Any],
    bufferOffset: Int,
    var underlyingBuffer: MutableRow)
  extends MutableAggregationBuffer {

  private[this] val offsets: Array[Int] = {
    val newOffsets = new Array[Int](length)
    var i = 0
    while (i < newOffsets.length) {
      newOffsets(i) = bufferOffset + i
      i += 1
    }
    newOffsets
  }

  override def length: Int = toCatalystConverters.length

  override def get(i: Int): Any = {
    if (i >= length || i < 0) {
      throw new IllegalArgumentException(
        s"Could not access ${i}th value in this buffer because it only has $length values.")
    }
    toScalaConverters(i)(underlyingBuffer.get(offsets(i), schema(i).dataType))
  }

  def update(i: Int, value: Any): Unit = {
    if (i >= length || i < 0) {
      throw new IllegalArgumentException(
        s"Could not update ${i}th value in this buffer because it only has $length values.")
    }
    underlyingBuffer.update(offsets(i), toCatalystConverters(i)(value))
  }

  override def copy(): MutableAggregationBufferImpl = {
    new MutableAggregationBufferImpl(
      schema,
      toCatalystConverters,
      toScalaConverters,
      bufferOffset,
      underlyingBuffer)
  }
}

/**
 * A [[Row]] representing an immutable aggregation buffer.
 */
private[sql] class InputAggregationBuffer private[sql] (
    schema: StructType,
    toCatalystConverters: Array[Any => Any],
    toScalaConverters: Array[Any => Any],
    bufferOffset: Int,
    var underlyingInputBuffer: InternalRow)
  extends Row {

  private[this] val offsets: Array[Int] = {
    val newOffsets = new Array[Int](length)
    var i = 0
    while (i < newOffsets.length) {
      newOffsets(i) = bufferOffset + i
      i += 1
    }
    newOffsets
  }

  override def length: Int = toCatalystConverters.length

  override def get(i: Int): Any = {
    if (i >= length || i < 0) {
      throw new IllegalArgumentException(
        s"Could not access ${i}th value in this buffer because it only has $length values.")
    }
    // TODO: Use buffer schema to avoid using generic getter.
    toScalaConverters(i)(underlyingInputBuffer.get(offsets(i), schema(i).dataType))
  }

  override def copy(): InputAggregationBuffer = {
    new InputAggregationBuffer(
      schema,
      toCatalystConverters,
      toScalaConverters,
      bufferOffset,
      underlyingInputBuffer)
  }
}

/**
 * The internal wrapper used to hook a [[UserDefinedAggregateFunction]] `udaf` in the
 * internal aggregation code path.
 * @param children
 * @param udaf
 */
private[sql] case class ScalaUDAF(
    children: Seq[Expression],
    udaf: UserDefinedAggregateFunction)
  extends AggregateFunction2 with Logging {

  require(
    children.length == udaf.inputSchema.length,
    s"$udaf only accepts ${udaf.inputSchema.length} arguments, " +
      s"but ${children.length} are provided.")

  override def nullable: Boolean = true

  override def dataType: DataType = udaf.returnDataType

  override def deterministic: Boolean = udaf.deterministic

  override val inputTypes: Seq[DataType] = udaf.inputSchema.map(_.dataType)

  override val bufferSchema: StructType = udaf.bufferSchema

  override val bufferAttributes: Seq[AttributeReference] = bufferSchema.toAttributes

  override lazy val cloneBufferAttributes = bufferAttributes.map(_.newInstance())

  val childrenSchema: StructType = {
    val inputFields = children.zipWithIndex.map {
      case (child, index) =>
        StructField(s"input$index", child.dataType, child.nullable, Metadata.empty)
    }
    StructType(inputFields)
  }

  lazy val inputProjection = {
    val inputAttributes = childrenSchema.toAttributes
    log.debug(
      s"Creating MutableProj: $children, inputSchema: $inputAttributes.")
    try {
      GenerateMutableProjection.generate(children, inputAttributes)()
    } catch {
      case e: Exception =>
        log.error("Failed to generate mutable projection, fallback to interpreted", e)
        new InterpretedMutableProjection(children, inputAttributes)
    }
  }

  val inputToScalaConverters: Any => Any =
    CatalystTypeConverters.createToScalaConverter(childrenSchema)

  val bufferValuesToCatalystConverters: Array[Any => Any] = bufferSchema.fields.map { field =>
    CatalystTypeConverters.createToCatalystConverter(field.dataType)
  }

  val bufferValuesToScalaConverters: Array[Any => Any] = bufferSchema.fields.map { field =>
    CatalystTypeConverters.createToScalaConverter(field.dataType)
  }

  lazy val inputAggregateBuffer: InputAggregationBuffer =
    new InputAggregationBuffer(
      bufferSchema,
      bufferValuesToCatalystConverters,
      bufferValuesToScalaConverters,
      inputBufferOffset,
      null)

  lazy val mutableAggregateBuffer: MutableAggregationBufferImpl =
    new MutableAggregationBufferImpl(
      bufferSchema,
      bufferValuesToCatalystConverters,
      bufferValuesToScalaConverters,
      mutableBufferOffset,
      null)

  lazy val evalAggregateBuffer: InputAggregationBuffer =
    new InputAggregationBuffer(
      bufferSchema,
      bufferValuesToCatalystConverters,
      bufferValuesToScalaConverters,
      mutableBufferOffset,
      null)

  override def initialize(buffer: MutableRow): Unit = {
    mutableAggregateBuffer.underlyingBuffer = buffer

    udaf.initialize(mutableAggregateBuffer)
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    mutableAggregateBuffer.underlyingBuffer = buffer

    udaf.update(
      mutableAggregateBuffer,
      inputToScalaConverters(inputProjection(input)).asInstanceOf[Row])
  }

  override def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    mutableAggregateBuffer.underlyingBuffer = buffer1
    inputAggregateBuffer.underlyingInputBuffer = buffer2

    udaf.merge(mutableAggregateBuffer, inputAggregateBuffer)
  }

  override def eval(buffer: InternalRow): Any = {
    evalAggregateBuffer.underlyingInputBuffer = buffer

    udaf.evaluate(evalAggregateBuffer)
  }

  override def toString: String = {
    s"""${udaf.getClass.getSimpleName}(${children.mkString(",")})"""
  }

  override def nodeName: String = udaf.getClass.getSimpleName
}
