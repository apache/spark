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

package org.apache.spark.sql.execution.expressions.aggregate2

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.catalyst.{InternalRow, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate2.{InputAggregationBuffer, AggregateFunction2, MutableAggregationBuffer}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

abstract class UserDefinedAggregateFunction extends Serializable {

  def inputDataType: StructType

  def bufferSchema: StructType

  def returnDataType: DataType

  def deterministic: Boolean

  def initialize(buffer: MutableAggregationBuffer): Unit

  def update(buffer: MutableAggregationBuffer, input: Row): Unit

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit

  def evaluate(buffer: Row): Any

}

case class ScalaUDAF(
    children: Seq[Expression],
    udaf: UserDefinedAggregateFunction)
  extends AggregateFunction2 with ImplicitCastInputTypes with Logging {

  require(
    children.length == udaf.inputDataType.length,
    s"$udaf only accepts ${udaf.inputDataType.length} arguments, " +
      s"but ${children.length} are provided.")

  override def nullable: Boolean = true

  override def dataType: DataType = udaf.returnDataType

  override def deterministic: Boolean = udaf.deterministic

  override val inputTypes: Seq[DataType] = udaf.inputDataType.map(_.dataType)

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
      bufferValuesToCatalystConverters,
      bufferValuesToScalaConverters,
      bufferOffset,
      null)

  lazy val mutableAggregateBuffer: MutableAggregationBuffer =
    new MutableAggregationBuffer(
      bufferValuesToCatalystConverters,
      bufferValuesToScalaConverters,
      bufferOffset,
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

    udaf.update(mutableAggregateBuffer, inputAggregateBuffer)
  }

  override def eval(buffer: InternalRow = null): Any = {
    inputAggregateBuffer.underlyingInputBuffer = buffer

    udaf.evaluate(inputAggregateBuffer)
  }

  override def toString: String = {
    s"""${udaf.getClass.getSimpleName}(${children.mkString(",")})"""
  }
}
