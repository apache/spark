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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
 * A helper trait used to create specialized setter and getter for types supported by
 * [[org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap]]'s buffer.
 * (see UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema).
 */
sealed trait BufferSetterGetterUtils {

  def createGetters(schema: StructType): Array[(InternalRow, Int) => Any] = {
    val dataTypes = schema.fields.map(_.dataType)
    val getters = new Array[(InternalRow, Int) => Any](dataTypes.length)

    var i = 0
    while (i < getters.length) {
      getters(i) = dataTypes(i) match {
        case NullType =>
          (row: InternalRow, ordinal: Int) => null

        case BooleanType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getBoolean(ordinal)

        case ByteType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getByte(ordinal)

        case ShortType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getShort(ordinal)

        case IntegerType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getInt(ordinal)

        case LongType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getLong(ordinal)

        case FloatType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getFloat(ordinal)

        case DoubleType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getDouble(ordinal)

        case dt: DecimalType =>
          val precision = dt.precision
          val scale = dt.scale
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getDecimal(ordinal, precision, scale)

        case DateType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getInt(ordinal)

        case TimestampType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getLong(ordinal)

        case other =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.get(ordinal, other)
      }

      i += 1
    }

    getters
  }

  def createSetters(schema: StructType): Array[((InternalRow, Int, Any) => Unit)] = {
    val dataTypes = schema.fields.map(_.dataType)
    val setters = new Array[(InternalRow, Int, Any) => Unit](dataTypes.length)

    var i = 0
    while (i < setters.length) {
      setters(i) = dataTypes(i) match {
        case NullType =>
          (row: InternalRow, ordinal: Int, value: Any) => row.setNullAt(ordinal)

        case b: BooleanType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setBoolean(ordinal, value.asInstanceOf[Boolean])
            } else {
              row.setNullAt(ordinal)
            }

        case ByteType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setByte(ordinal, value.asInstanceOf[Byte])
            } else {
              row.setNullAt(ordinal)
            }

        case ShortType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setShort(ordinal, value.asInstanceOf[Short])
            } else {
              row.setNullAt(ordinal)
            }

        case IntegerType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setInt(ordinal, value.asInstanceOf[Int])
            } else {
              row.setNullAt(ordinal)
            }

        case LongType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setLong(ordinal, value.asInstanceOf[Long])
            } else {
              row.setNullAt(ordinal)
            }

        case FloatType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setFloat(ordinal, value.asInstanceOf[Float])
            } else {
              row.setNullAt(ordinal)
            }

        case DoubleType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setDouble(ordinal, value.asInstanceOf[Double])
            } else {
              row.setNullAt(ordinal)
            }

        case dt: DecimalType =>
          val precision = dt.precision
          (row: InternalRow, ordinal: Int, value: Any) =>
            // To make it work with UnsafeRow, we cannot use setNullAt.
            // Please see the comment of UnsafeRow's setDecimal.
            row.setDecimal(ordinal, value.asInstanceOf[Decimal], precision)

        case DateType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setInt(ordinal, value.asInstanceOf[Int])
            } else {
              row.setNullAt(ordinal)
            }

        case TimestampType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setLong(ordinal, value.asInstanceOf[Long])
            } else {
              row.setNullAt(ordinal)
            }

        case other =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.update(ordinal, value)
            } else {
              row.setNullAt(ordinal)
            }
      }

      i += 1
    }

    setters
  }
}

/**
 * A Mutable [[Row]] representing a mutable aggregation buffer.
 */
private[aggregate] class MutableAggregationBufferImpl(
    schema: StructType,
    toCatalystConverters: Array[Any => Any],
    toScalaConverters: Array[Any => Any],
    bufferOffset: Int,
    var underlyingBuffer: InternalRow)
  extends MutableAggregationBuffer with BufferSetterGetterUtils {

  private[this] val offsets: Array[Int] = {
    val newOffsets = new Array[Int](length)
    var i = 0
    while (i < newOffsets.length) {
      newOffsets(i) = bufferOffset + i
      i += 1
    }
    newOffsets
  }

  private[this] val bufferValueGetters = createGetters(schema)

  private[this] val bufferValueSetters = createSetters(schema)

  override def length: Int = toCatalystConverters.length

  override def get(i: Int): Any = {
    if (i >= length || i < 0) {
      throw new IllegalArgumentException(
        s"Could not access ${i}th value in this buffer because it only has $length values.")
    }

    toScalaConverters(i)(bufferValueGetters(i)(underlyingBuffer, offsets(i)))
  }

  def update(i: Int, value: Any): Unit = {
    if (i >= length || i < 0) {
      throw new IllegalArgumentException(
        s"Could not update ${i}th value in this buffer because it only has $length values.")
    }

    bufferValueSetters(i)(underlyingBuffer, offsets(i), toCatalystConverters(i)(value))
  }

  // Because get method call specialized getter based on the schema, we cannot use the
  // default implementation of the isNullAt (which is get(i) == null).
  // We have to override it to call isNullAt of the underlyingBuffer.
  override def isNullAt(i: Int): Boolean = {
    underlyingBuffer.isNullAt(offsets(i))
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
private[aggregate] class InputAggregationBuffer(
    schema: StructType,
    toCatalystConverters: Array[Any => Any],
    toScalaConverters: Array[Any => Any],
    bufferOffset: Int,
    var underlyingInputBuffer: InternalRow)
  extends Row with BufferSetterGetterUtils {

  private[this] val offsets: Array[Int] = {
    val newOffsets = new Array[Int](length)
    var i = 0
    while (i < newOffsets.length) {
      newOffsets(i) = bufferOffset + i
      i += 1
    }
    newOffsets
  }

  private[this] val bufferValueGetters = createGetters(schema)

  def getBufferOffset: Int = bufferOffset

  override def length: Int = toCatalystConverters.length

  override def get(i: Int): Any = {
    if (i >= length || i < 0) {
      throw new IllegalArgumentException(
        s"Could not access ${i}th value in this buffer because it only has $length values.")
    }
    toScalaConverters(i)(bufferValueGetters(i)(underlyingInputBuffer, offsets(i)))
  }

  // Because get method call specialized getter based on the schema, we cannot use the
  // default implementation of the isNullAt (which is get(i) == null).
  // We have to override it to call isNullAt of the underlyingInputBuffer.
  override def isNullAt(i: Int): Boolean = {
    underlyingInputBuffer.isNullAt(offsets(i))
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
 */
case class ScalaUDAF(
    children: Seq[Expression],
    udaf: UserDefinedAggregateFunction,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends ImperativeAggregate
  with NonSQLExpression
  with Logging
  with ImplicitCastInputTypes
  with UserDefinedExpression {

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = true

  override def dataType: DataType = udaf.dataType

  override lazy val deterministic: Boolean = udaf.deterministic

  override val inputTypes: Seq[DataType] = udaf.inputSchema.map(_.dataType)

  override val aggBufferSchema: StructType = udaf.bufferSchema

  override val aggBufferAttributes: Seq[AttributeReference] = aggBufferSchema.toAttributes

  // Note: although this simply copies aggBufferAttributes, this common code can not be placed
  // in the superclass because that will lead to initialization ordering issues.
  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  private[this] lazy val childrenSchema: StructType = {
    val inputFields = children.zipWithIndex.map {
      case (child, index) =>
        StructField(s"input$index", child.dataType, child.nullable, Metadata.empty)
    }
    StructType(inputFields)
  }

  private lazy val inputProjection = {
    val inputAttributes = childrenSchema.toAttributes
    log.debug(
      s"Creating MutableProj: $children, inputSchema: $inputAttributes.")
    GenerateMutableProjection.generate(children, inputAttributes)
  }

  private[this] lazy val inputToScalaConverters: Any => Any =
    CatalystTypeConverters.createToScalaConverter(childrenSchema)

  private[this] lazy val bufferValuesToCatalystConverters: Array[Any => Any] = {
    aggBufferSchema.fields.map { field =>
      CatalystTypeConverters.createToCatalystConverter(field.dataType)
    }
  }

  private[this] lazy val bufferValuesToScalaConverters: Array[Any => Any] = {
    aggBufferSchema.fields.map { field =>
      CatalystTypeConverters.createToScalaConverter(field.dataType)
    }
  }

  private[this] lazy val outputToCatalystConverter: Any => Any = {
    CatalystTypeConverters.createToCatalystConverter(dataType)
  }

  // This buffer is only used at executor side.
  private[this] lazy val inputAggregateBuffer: InputAggregationBuffer = {
    new InputAggregationBuffer(
      aggBufferSchema,
      bufferValuesToCatalystConverters,
      bufferValuesToScalaConverters,
      inputAggBufferOffset,
      null)
  }

  // This buffer is only used at executor side.
  private[this] lazy val mutableAggregateBuffer: MutableAggregationBufferImpl = {
    new MutableAggregationBufferImpl(
      aggBufferSchema,
      bufferValuesToCatalystConverters,
      bufferValuesToScalaConverters,
      mutableAggBufferOffset,
      null)
  }

  // This buffer is only used at executor side.
  private[this] lazy val evalAggregateBuffer: InputAggregationBuffer = {
    new InputAggregationBuffer(
      aggBufferSchema,
      bufferValuesToCatalystConverters,
      bufferValuesToScalaConverters,
      mutableAggBufferOffset,
      null)
  }

  override def initialize(buffer: InternalRow): Unit = {
    mutableAggregateBuffer.underlyingBuffer = buffer

    udaf.initialize(mutableAggregateBuffer)
  }

  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    mutableAggregateBuffer.underlyingBuffer = buffer

    udaf.update(
      mutableAggregateBuffer,
      inputToScalaConverters(inputProjection(input)).asInstanceOf[Row])
  }

  override def merge(buffer1: InternalRow, buffer2: InternalRow): Unit = {
    mutableAggregateBuffer.underlyingBuffer = buffer1
    inputAggregateBuffer.underlyingInputBuffer = buffer2

    udaf.merge(mutableAggregateBuffer, inputAggregateBuffer)
  }

  override def eval(buffer: InternalRow): Any = {
    evalAggregateBuffer.underlyingInputBuffer = buffer

    outputToCatalystConverter(udaf.evaluate(evalAggregateBuffer))
  }

  override def toString: String = {
    s"""${udaf.getClass.getSimpleName}(${children.mkString(",")})"""
  }

  override def nodeName: String = udaf.getClass.getSimpleName
}
