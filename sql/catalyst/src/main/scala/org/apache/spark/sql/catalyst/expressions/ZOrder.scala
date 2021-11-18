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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, ByteType, DataType, IntegerType, IntegralType, LongType, ShortType}

/**
 * An expression that calculates Z-order value (https://en.wikipedia.org/wiki/Z-order_curve)
 * from `children` input data.
 *
 * At the high level, Z-order value is calculated by interleaving the binary representations of
 * `children` input. So the Z-order value preserves locality of input data, while mapping the
 * multi-dimensional input into one dimension output.
 *
 */
@ExpressionDescription(
  usage = """
    _FUNC_(input1, input2, ...) - Returns Z-order value in binary type for inputs.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(1, 2);
       [-64, 6]
  """,
  since = "3.3.0",
  group = "misc_funcs")
case class ZOrder(children: Seq[Expression])
  extends Expression with ExpectsInputTypes with CodegenFallback {

  override def nullable: Boolean = false

  /**
   * Supported input data types. Currently support [[IntegralType]].
   *
   * TODO(SPARK-37362): Support [[FractionalType]] for Z-order.
   * TODO(SPARK-37363): Support [[StringType]] for Z-order.
   */
  override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.size)(IntegralType)

  override def dataType: DataType = BinaryType

  override def prettyName: String = "zorder"

  /**
   * Number of bits per input in binary representation.
   */
  @transient private lazy val inputBitSizes = children.map(_.dataType.defaultSize * 8)

  /**
   * Maximal number of bits per input in binary representation.
   */
  @transient private lazy val inputBitMaxSize = inputBitSizes.max

  /**
   * Total number of bits per input in binary representation.
   */
  @transient private lazy val inputBitTotalSize = inputBitSizes.sum

  /**
   * The output for Z-order, represented in byte array format.
   */
  @transient private lazy val outputBytes = new Array[Byte](inputBitTotalSize / 8)

  /**
   * Handle input with null value. Convert null into minimal value per each data type.
   * This mimics [[NullsFirst]] sort ordering.
   */
  @transient private lazy val handleNullFromInputs: Seq[Any => Any] = children.map {
    child => child.dataType match {
      case ByteType =>
        value: Any => if (value == null) Byte.MinValue else value
      case ShortType =>
        value: Any => if (value == null) Short.MinValue else value
      case IntegerType =>
        value: Any => if (value == null) Int.MinValue else value
      case LongType =>
        value: Any => if (value == null) Long.MinValue else value
      case x =>
        throw new IllegalArgumentException(
          s"ZOrder expression should not take $x as the data type")
    }
  }

  /**
   * Get the bit in `ordinal` position (0-based) of input `value`.
   * The return value of corresponding bit is in [[Byte]] type.
   */
  @transient private lazy val getBitFromInputs: Seq[(Any, Int) => Byte] = children.map { child =>
    child.dataType match {
      case ByteType =>
        (value: Any, ordinal: Int) =>
          ((value.asInstanceOf[Byte] >> ordinal) & 1).toByte
      case ShortType =>
        (value: Any, ordinal: Int) =>
          ((value.asInstanceOf[Short] >> ordinal) & 1).toByte
      case IntegerType =>
        (value: Any, ordinal: Int) =>
          ((value.asInstanceOf[Int] >> ordinal) & 1).toByte
      case LongType =>
        (value: Any, ordinal: Int) =>
          ((value.asInstanceOf[Long] >> ordinal) & 1).toByte
      case x =>
        throw new IllegalArgumentException(
          s"ZOrder expression should not take $x as the data type")
    }
  }

  /**
   * Get the Z-order sign bit of input `value`.
   *
   * For [[IntegralType]], Z-order flips the original sign bit of input. Sign bit is 0 for negative
   * value, and 1 for positive value. This is to maintain the same ordering between input and
   * output in [[BinaryType]].
   */
  @transient private lazy val getZOrderSignBitFromInputs: Seq[Any => Byte] = children.map {
    child => child.dataType match {
      case ByteType =>
        value: Any => if (value.asInstanceOf[Byte] < 0) 0.toByte else 1.toByte
      case ShortType =>
        value: Any => if (value.asInstanceOf[Short] < 0) 0.toByte else 1.toByte
      case IntegerType =>
        value: Any => if (value.asInstanceOf[Int] < 0) 0.toByte else 1.toByte
      case LongType =>
        value: Any => if (value.asInstanceOf[Long] < 0) 0.toByte else 1.toByte
      case x =>
        throw new IllegalArgumentException(
          s"ZOrder expression should not take $x as the data type")
    }
  }

  /**
   * Update the bit at `ordinal` position (0-based) in `outputBytes`, with new value `bitAsByte`.
   */
  private def updateBitInOutput(bitAsByte: Byte, ordinal: Int): Unit = {
    val currentByteIndex = ordinal >> 3
    val currentByte = outputBytes(currentByteIndex)
    val newByte = (currentByte | (bitAsByte << ((ordinal & 7) ^ 7))).toByte
    outputBytes.update(currentByteIndex, newByte)
  }

  /**
   * Calculate Z-order from `input` and store the result in `outputBytes`.
   *
   * The function has the following steps:
   *  - Step 1: Handle null value from `input`.
   *  - Step 2: Initialize `outputBytes` with all zeros.
   *  - Step 3: Interleave sign bits from `input` and write to `outputBytes`.
   *  - Step 4: Interleave data bits from `input` and write to `outputBytes`.
   *            Start from most significant bit to least. Skip the corresponding
   *            input if it finishes early.
   *
   * Example (all value in binary representation for illustration purpose):
   * input1: 01111010
   * input2: 10011011 11100001
   *
   * output: 10101011 11001101 11100001
   *         ||\                     /
   *         || ---------------------
   * (Z-order sign bits)  |
   *             (Z-order data bits)
   *
   * TODO(SPARK-37364): Support code-gen evaluation for Z-order.
   */
  override def eval(input: InternalRow): Any = {
    val evaluatedInput = children.map(_.eval(input))

    // Handle null value from `input`.
    val inputValues = handleNullFromInputs.zip(evaluatedInput).map {
      case (nullHandler, value) => nullHandler(value)
    }
    var outputBitIndex = 0

    // Initialize `outputBytes` with all zeros.
    outputBytes.indices.foreach(outputBytes.update(_, 0))

    // Interleave sign bits from `input` and write to `outputBytes`.
    getZOrderSignBitFromInputs.zip(inputValues).foreach { case (getSignBit, value) =>
      val signBit = getSignBit(value)
      updateBitInOutput(signBit, outputBitIndex)
      outputBitIndex += 1
    }

    // Interleave data bits from `input` and write to `outputBytes`.
    var dataBitIndex = 0
    while (dataBitIndex < inputBitMaxSize - 1) {
      var inputIndex = 0
      while (inputIndex < inputValues.size) {
        val dataBitSize = inputBitSizes(inputIndex) - 1
        if (dataBitIndex < dataBitSize) {
          val bitPosition = dataBitSize - dataBitIndex - 1
          val dataBit = getBitFromInputs(inputIndex)(inputValues(inputIndex), bitPosition)
          updateBitInOutput(dataBit, outputBitIndex)
          outputBitIndex += 1
        }
        inputIndex += 1
      }
      dataBitIndex += 1
    }

    outputBytes
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): ZOrder =
    copy(children = newChildren)
}
