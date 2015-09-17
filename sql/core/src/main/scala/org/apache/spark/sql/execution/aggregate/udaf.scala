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

        case other =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.get(ordinal, other)
      }

      i += 1
    }

    getters
  }

  def createSetters(schema: StructType): Array[((MutableRow, Int, Any) => Unit)] = {
    val dataTypes = schema.fields.map(_.dataType)
    val setters = new Array[(MutableRow, Int, Any) => Unit](dataTypes.length)

    var i = 0
    while (i < setters.length) {
      setters(i) = dataTypes(i) match {
        case b: BooleanType =>
          (row: MutableRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setBoolean(ordinal, value.asInstanceOf[Boolean])
            } else {
              row.setNullAt(ordinal)
            }

        case ByteType =>
          (row: MutableRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setByte(ordinal, value.asInstanceOf[Byte])
            } else {
              row.setNullAt(ordinal)
            }

        case ShortType =>
          (row: MutableRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setShort(ordinal, value.asInstanceOf[Short])
            } else {
              row.setNullAt(ordinal)
            }

        case IntegerType =>
          (row: MutableRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setInt(ordinal, value.asInstanceOf[Int])
            } else {
              row.setNullAt(ordinal)
            }

        case LongType =>
          (row: MutableRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setLong(ordinal, value.asInstanceOf[Long])
            } else {
              row.setNullAt(ordinal)
            }

        case FloatType =>
          (row: MutableRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setFloat(ordinal, value.asInstanceOf[Float])
            } else {
              row.setNullAt(ordinal)
            }

        case DoubleType =>
          (row: MutableRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setDouble(ordinal, value.asInstanceOf[Double])
            } else {
              row.setNullAt(ordinal)
            }

        case dt: DecimalType =>
          val precision = dt.precision
          (row: MutableRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setDecimal(ordinal, value.asInstanceOf[Decimal], precision)
            } else {
              row.setNullAt(ordinal)
            }

        case other =>
          (row: MutableRow, ordinal: Int, value: Any) =>
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
 * A Mutable [[Row]] representing an mutable aggregation buffer.
 */
private[sql] class MutableAggregationBufferImpl (
    schema: StructType,
    toCatalystConverters: Array[Any => Any],
    toScalaConverters: Array[Any => Any],
    bufferOffset: Int,
    var underlyingBuffer: MutableRow)
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
private[sql] class InputAggregationBuffer private[sql] (
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

  override def dataType: DataType = udaf.dataType

  override def deterministic: Boolean = udaf.deterministic

  override val inputTypes: Seq[DataType] = udaf.inputSchema.map(_.dataType)

  override val bufferSchema: StructType = udaf.bufferSchema

  override val bufferAttributes: Seq[AttributeReference] = bufferSchema.toAttributes

  override lazy val cloneBufferAttributes = bufferAttributes.map(_.newInstance())

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
    try {
      GenerateMutableProjection.generate(children, inputAttributes)()
    } catch {
      case e: Exception =>
        log.error("Failed to generate mutable projection, fallback to interpreted", e)
        new InterpretedMutableProjection(children, inputAttributes)
    }
  }

  private[this] lazy val inputToScalaConverters: Any => Any =
    CatalystTypeConverters.createToScalaConverter(childrenSchema)

  private[this] lazy val bufferValuesToCatalystConverters: Array[Any => Any] = {
    bufferSchema.fields.map { field =>
      CatalystTypeConverters.createToCatalystConverter(field.dataType)
    }
  }

  private[this] lazy val bufferValuesToScalaConverters: Array[Any => Any] = {
    bufferSchema.fields.map { field =>
      CatalystTypeConverters.createToScalaConverter(field.dataType)
    }
  }

  // This buffer is only used at executor side.
  private[this] var inputAggregateBuffer: InputAggregationBuffer = null

  // This buffer is only used at executor side.
  private[this] var mutableAggregateBuffer: MutableAggregationBufferImpl = null

  // This buffer is only used at executor side.
  private[this] var evalAggregateBuffer: InputAggregationBuffer = null

  /**
   * Sets the inputBufferOffset to newInputBufferOffset and then create a new instance of
   * `inputAggregateBuffer` based on this new inputBufferOffset.
   */
  override def withNewInputBufferOffset(newInputBufferOffset: Int): Unit = {
    super.withNewInputBufferOffset(newInputBufferOffset)
    // inputBufferOffset has been updated.
    inputAggregateBuffer =
      new InputAggregationBuffer(
        bufferSchema,
        bufferValuesToCatalystConverters,
        bufferValuesToScalaConverters,
        inputBufferOffset,
        null)
  }

  /**
   * Sets the mutableBufferOffset to newMutableBufferOffset and then create a new instance of
   * `mutableAggregateBuffer` and `evalAggregateBuffer` based on this new mutableBufferOffset.
   */
  override def withNewMutableBufferOffset(newMutableBufferOffset: Int): Unit = {
    super.withNewMutableBufferOffset(newMutableBufferOffset)
    // mutableBufferOffset has been updated.
    mutableAggregateBuffer =
      new MutableAggregationBufferImpl(
        bufferSchema,
        bufferValuesToCatalystConverters,
        bufferValuesToScalaConverters,
        mutableBufferOffset,
        null)
    evalAggregateBuffer =
      new InputAggregationBuffer(
        bufferSchema,
        bufferValuesToCatalystConverters,
        bufferValuesToScalaConverters,
        mutableBufferOffset,
        null)
  }

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
