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

import org.apache.datasketches.kll._
import org.apache.datasketches.memory.Memory

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._

/** This represents the aggregation buffer for the below aggregate functions. */
case class KllBufferBigint(sketch: KllLongsSketch = KllLongsSketch.newHeapInstance()) {
  def serialize(): Array[Byte] = sketch.toByteArray
  def eval(): Array[Byte] = sketch.toByteArray
}
case class KllBufferFloat(sketch: KllFloatsSketch = KllFloatsSketch.newHeapInstance()) {
  def serialize(): Array[Byte] = sketch.toByteArray
  def eval(): Array[Byte] = sketch.toByteArray
}
case class KllBufferDouble(sketch: KllDoublesSketch = KllDoublesSketch.newHeapInstance()) {
  def serialize(): Array[Byte] = sketch.toByteArray
  def eval(): Array[Byte] = sketch.toByteArray
}

/**
 * The KllSketchAggBigint function utilizes an Apache DataSketches KllLongsSketch instance to
 * compute quantiles of the values of an input expression (such as an input column in a table).
 * It outputs the binary representation of the KllLongsSketch.
 *
 * See [[https://datasketches.apache.org/docs/KLL/KLLSketch.html]] for more information.
 *
 * @param child
 *   child expression against which quantile computation will occur
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the KllLongsSketch compact binary representation.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(kll_sketch_to_string_bigint(_FUNC_(col))) > 0 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class KllSketchAggBigint(
    override val child: Expression,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[KllBufferBigint]
    with UnaryLike[Expression]
    with ExpectsInputTypes {
  def this(child: Expression) = this(child, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): KllSketchAggBigint =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): KllSketchAggBigint =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  override protected def withNewChildInternal(newInput: Expression): KllSketchAggBigint =
    copy(child = newInput)

  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      TypeCollection(
        ByteType,
        IntegerType,
        LongType,
        ShortType))
  override def nullable: Boolean = false
  override def prettyName: String = "kll_sketch_agg_bigint"
  override def createAggregationBuffer(): KllBufferBigint = KllBufferBigint()

  /**
   * Evaluate the input row and update the KllLongsSketch instance with the row's value. The update
   * function only supports a subset of Spark SQL types, and an exception will be thrown for
   * unsupported types.
   * Note, null values are ignored.
   */
  override def update(updateBuffer: KllBufferBigint, input: InternalRow): KllBufferBigint = {
    // Return early for null values.
    val v = child.eval(input)
    if (v == null) {
      return updateBuffer
    }
    // Handle the different data types for sketch updates.
    val sketch: KllLongsSketch = updateBuffer.sketch
    child.dataType match {
      case ByteType =>
        sketch.update(v.asInstanceOf[Byte].toLong)
      case IntegerType =>
        sketch.update(v.asInstanceOf[Int].toLong)
      case LongType =>
        sketch.update(v.asInstanceOf[Long])
      case ShortType =>
        sketch.update(v.asInstanceOf[Short].toLong)
      case _ =>
        throw KllSketchAgg.unexpectedInputDataTypeError(child)
    }

    KllBufferBigint(sketch)
  }

  /** Merges an input sketch into the current aggregation buffer. */
  override def merge(updateBuffer: KllBufferBigint, input: KllBufferBigint): KllBufferBigint = {
    try {
      updateBuffer.sketch.merge(input.sketch)
      KllBufferBigint(updateBuffer.sketch)
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchIncompatibleMergeError(prettyName, e.getMessage)
    }
  }

  /** Returns a sketch derived from the input column or expression. */
  override def eval(sketchState: KllBufferBigint): Any = sketchState.eval()

  /** Converts the underlying sketch state into a byte array. */
  override def serialize(sketchState: KllBufferBigint): Array[Byte] = sketchState.serialize()

  /** Wraps the byte array into a sketch instance. */
  override def deserialize(buffer: Array[Byte]): KllBufferBigint = if (buffer.nonEmpty) {
    try {
      KllBufferBigint(KllLongsSketch.wrap(Memory.wrap(buffer)))
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  } else {
    this.createAggregationBuffer()
  }
}

/**
 * The KllSketchAggFloat function utilizes an Apache DataSketches KllFloatsSketch instance to
 * compute quantiles of the values of an input expression (such as an input column in a table).
 * It outputs the binary representation of the KllFloatsSketch.
 *
 * See [[https://datasketches.apache.org/docs/KLL/KLLSketch.html]] for more information.
 *
 * @param child
 *   child expression against which quantile computation will occur
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the KllFloatsSketch compact binary representation.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(kll_sketch_to_string_float(_FUNC_(col))) > 0 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class KllSketchAggFloat(
    override val child: Expression,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[KllBufferFloat]
        with UnaryLike[Expression]
        with ExpectsInputTypes {
  def this(child: Expression) = this(child, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): KllSketchAggFloat =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): KllSketchAggFloat =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  override protected def withNewChildInternal(newInput: Expression): KllSketchAggFloat =
    copy(child = newInput)

  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      TypeCollection(
        ByteType,
        FloatType,
        IntegerType,
        LongType,
        ShortType
      ))
  override def nullable: Boolean = false
  override def prettyName: String = "kll_sketch_agg_float"
  override def createAggregationBuffer(): KllBufferFloat = KllBufferFloat()

  /**
   * Evaluate the input row and update the KllFloatsSketch instance with the row's value. The update
   * function only supports a subset of Spark SQL types, and an exception will be thrown for
   * unsupported types.
   * Note, Null values are ignored.
   */
  override def update(updateBuffer: KllBufferFloat, input: InternalRow): KllBufferFloat = {
    // Return early for null values.
    val v = child.eval(input)
    if (v == null) {
      return updateBuffer
    }
    // Handle the different data types for sketch updates.
    val sketch: KllFloatsSketch = updateBuffer.sketch
    child.dataType match {
      case ByteType =>
        sketch.update(v.asInstanceOf[Byte].toFloat)
      case IntegerType =>
        sketch.update(v.asInstanceOf[Int].toFloat)
      case FloatType =>
        sketch.update(v.asInstanceOf[Float])
      case LongType =>
        sketch.update(v.asInstanceOf[Long].toFloat)
      case ShortType =>
        sketch.update(v.asInstanceOf[Short].toFloat)
      case _ =>
        throw KllSketchAgg.unexpectedInputDataTypeError(child)
    }

    KllBufferFloat(sketch)
  }

  /** Merges an input sketch into the current aggregation buffer. */
  override def merge(updateBuffer: KllBufferFloat, input: KllBufferFloat): KllBufferFloat = {
    try {
      updateBuffer.sketch.merge(input.sketch)
      KllBufferFloat(updateBuffer.sketch)
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchIncompatibleMergeError(prettyName, e.getMessage)
    }
  }

  /** Returns a sketch derived from the input column or expression. */
  override def eval(sketchState: KllBufferFloat): Any = sketchState.eval()

  /** Converts the underlying sketch state into a byte array. */
  override def serialize(sketchState: KllBufferFloat): Array[Byte] = sketchState.serialize()

  /** Wraps the byte array into a sketch instance. */
  override def deserialize(buffer: Array[Byte]): KllBufferFloat = if (buffer.nonEmpty) {
    try {
      KllBufferFloat(KllFloatsSketch.wrap(Memory.wrap(buffer)))
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  } else {
    this.createAggregationBuffer()
  }
}

/**
 * The KllSketchAggDouble function utilizes an Apache DataSketches KllDoublesSketch instance to
 * compute quantiles of the values of an input expression (such as an input column in a table).
 * It outputs the binary representation of the KllDoublesSketch.
 *
 * See [[https://datasketches.apache.org/docs/KLL/KLLSketch.html]] for more information.
 *
 * @param child
 *   child expression against which quantile computation will occur
 * @param mutableAggBufferOffset
 *   offset for mutable aggregation buffer
 * @param inputAggBufferOffset
 *   offset for input aggregation buffer
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the KllDoublesSketch compact binary representation.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(kll_sketch_to_string_double(_FUNC_(col))) > 0 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "agg_funcs",
  since = "4.1.0")
// scalastyle:on line.size.limit
case class KllSketchAggDouble(
    override val child: Expression,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[KllBufferDouble]
        with UnaryLike[Expression]
        with ExpectsInputTypes {
  def this(child: Expression) = this(child, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): KllSketchAggDouble =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): KllSketchAggDouble =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  override protected def withNewChildInternal(newInput: Expression): KllSketchAggDouble =
    copy(child = newInput)

  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      TypeCollection(
        ByteType,
        FloatType,
        DoubleType,
        IntegerType,
        LongType,
        ShortType
      ))
  override def nullable: Boolean = false
  override def prettyName: String = "kll_sketch_agg_double"
  override def createAggregationBuffer(): KllBufferDouble = KllBufferDouble()

  /**
   * Evaluate the input row and update the KllDoublesSketch instance with the row's value.
   * The update function only supports a subset of Spark SQL types, and an exception will be
   * thrown for unsupported types.
   * Note, Null values are ignored.
   */
  override def update(updateBuffer: KllBufferDouble, input: InternalRow): KllBufferDouble = {
    // Return early for null values.
    val v = child.eval(input)
    if (v == null) {
      return updateBuffer
    }
    // Handle the different data types for sketch updates.
    val sketch: KllDoublesSketch = updateBuffer.sketch
    child.dataType match {
      case ByteType =>
        sketch.update(v.asInstanceOf[Byte].toDouble)
      case DoubleType =>
        sketch.update(v.asInstanceOf[Double])
      case FloatType =>
        sketch.update(v.asInstanceOf[Float].toDouble)
      case IntegerType =>
        sketch.update(v.asInstanceOf[Int].toDouble)
      case LongType =>
        sketch.update(v.asInstanceOf[Long].toDouble)
      case ShortType =>
        sketch.update(v.asInstanceOf[Short].toDouble)
      case _ =>
        throw KllSketchAgg.unexpectedInputDataTypeError(child)
    }

    KllBufferDouble(sketch)
  }

  /** Merges an input sketch into the current aggregation buffer. */
  override def merge(updateBuffer: KllBufferDouble, input: KllBufferDouble): KllBufferDouble = {
    try {
      updateBuffer.sketch.merge(input.sketch)
      KllBufferDouble(updateBuffer.sketch)
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchIncompatibleMergeError(prettyName, e.getMessage)
    }
  }

  /** Returns a sketch derived from the input column or expression. */
  override def eval(sketchState: KllBufferDouble): Any = sketchState.eval()

  /** Converts the underlying sketch state into a byte array. */
  override def serialize(sketchState: KllBufferDouble): Array[Byte] = sketchState.serialize()

  /** Wraps the byte array into a sketch instance. */
  override def deserialize(buffer: Array[Byte]): KllBufferDouble = if (buffer.nonEmpty) {
    try {
      KllBufferDouble(KllDoublesSketch.wrap(Memory.wrap(buffer)))
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  } else {
    this.createAggregationBuffer()
  }
}

object KllSketchAgg {
  def unexpectedInputDataTypeError(child: Expression): SparkUnsupportedOperationException =
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_3121",
      messageParameters = Map("dataType" -> child.dataType.toString))
}
