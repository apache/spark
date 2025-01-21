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
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedAggregator}
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

        case TimestampType | TimestampNTZType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getLong(ordinal)

        case _: YearMonthIntervalType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getInt(ordinal)

        case _: DayTimeIntervalType =>
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

        case TimestampType | TimestampNTZType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setLong(ordinal, value.asInstanceOf[Long])
            } else {
              row.setNullAt(ordinal)
            }

        case _: YearMonthIntervalType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setInt(ordinal, value.asInstanceOf[Int])
            } else {
              row.setNullAt(ordinal)
            }

        case _: DayTimeIntervalType =>
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
    inputAggBufferOffset: Int = 0,
    udafName: Option[String] = None)
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

  override val aggBufferAttributes: Seq[AttributeReference] = toAttributes(aggBufferSchema)

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
    val inputAttributes = toAttributes(childrenSchema)
    log.debug(
      s"Creating MutableProj: $children, inputSchema: $inputAttributes.")
    MutableProjection.create(children, inputAttributes)
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
    s"""$nodeName(${children.mkString(",")})"""
  }

  override def nodeName: String = name

  override def name: String = udafName.getOrElse(udaf.getClass.getSimpleName)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): ScalaUDAF =
    copy(children = newChildren)
}

case class ScalaAggregator[IN, BUF, OUT](
    children: Seq[Expression],
    agg: Aggregator[IN, BUF, OUT],
    inputEncoder: ExpressionEncoder[IN],
    bufferEncoder: ExpressionEncoder[BUF],
    nullable: Boolean = true,
    isDeterministic: Boolean = true,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0,
    aggregatorName: Option[String] = None)
  extends TypedImperativeAggregate[BUF]
  with NonSQLExpression
  with UserDefinedExpression
  with ImplicitCastInputTypes
  with Logging {

  // input and buffer encoders are resolved by ResolveEncodersInScalaAgg
  @transient private[this] lazy val inputDeserializer = inputEncoder.createDeserializer()
  @transient private[this] lazy val bufferSerializer = bufferEncoder.createSerializer()
  @transient private[this] lazy val bufferDeserializer = bufferEncoder.createDeserializer()
  @transient private[this] lazy val outputEncoder = encoderFor(agg.outputEncoder)
  @transient private[this] lazy val outputSerializer = outputEncoder.createSerializer()

  def dataType: DataType = outputEncoder.objSerializer.dataType

  def inputTypes: Seq[DataType] = inputEncoder.schema.map(_.dataType)

  @transient override lazy val deterministic: Boolean = isDeterministic

  def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ScalaAggregator[IN, BUF, OUT] =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ScalaAggregator[IN, BUF, OUT] =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  @transient private[this] lazy val inputProjection = UnsafeProjection.create(children)

  def createAggregationBuffer(): BUF = agg.zero

  def update(buffer: BUF, input: InternalRow): BUF =
    agg.reduce(buffer, inputDeserializer(inputProjection(input)))

  def merge(buffer: BUF, input: BUF): BUF = agg.merge(buffer, input)

  def eval(buffer: BUF): Any = {
    val row = outputSerializer(agg.finish(buffer))
    if (outputEncoder.isSerializedAsStruct) row else row.get(0, dataType)
  }

  @transient private[this] lazy val bufferRow = new UnsafeRow(bufferEncoder.namedExpressions.length)

  def serialize(agg: BUF): Array[Byte] =
    bufferSerializer(agg).asInstanceOf[UnsafeRow].getBytes()

  def deserialize(storageFormat: Array[Byte]): BUF = {
    bufferRow.pointTo(storageFormat, storageFormat.length)
    bufferDeserializer(bufferRow)
  }

  override def toString: String = s"""${nodeName}(${children.mkString(",")})"""

  override def nodeName: String = name

  override def name: String = aggregatorName.getOrElse(agg.getClass.getSimpleName)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ScalaAggregator[IN, BUF, OUT] =
    copy(children = newChildren)
}

object ScalaAggregator {
  def apply[IN, BUF, OUT](
      uda: UserDefinedAggregator[IN, BUF, OUT],
      children: Seq[Expression]): ScalaAggregator[IN, BUF, OUT] = {
    new ScalaAggregator(
      children = children,
      agg = uda.aggregator,
      inputEncoder = encoderFor(uda.inputEncoder),
      bufferEncoder = encoderFor(uda.aggregator.bufferEncoder),
      nullable = uda.nullable,
      isDeterministic = uda.deterministic,
      aggregatorName = uda.givenName)
  }
}

/**
 * An extension rule to resolve encoder expressions from a [[ScalaAggregator]]
 */
object ResolveEncodersInScalaAgg extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case p if !p.resolved => p
    case p => p.transformExpressionsUp {
      case agg: ScalaAggregator[_, _, _] =>
        agg.copy(
          inputEncoder = agg.inputEncoder.resolveAndBind(),
          bufferEncoder = agg.bufferEncoder.resolveAndBind())
    }
  }
}
