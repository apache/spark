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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

class MutableProjectionSuite extends SparkFunSuite with ExpressionEvalHelper {

  val fixedLengthTypes = Array[DataType](
    BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
    DateType, TimestampType)

  val variableLengthTypes = Array(
    StringType, DecimalType.defaultConcreteType, CalendarIntervalType, BinaryType,
    ArrayType(StringType), MapType(IntegerType, StringType),
    StructType.fromDDL("a INT, b STRING"), ObjectType(classOf[java.lang.Integer]))

  def createMutableProjection(dataTypes: Array[DataType]): MutableProjection = {
    MutableProjection.create(dataTypes.zipWithIndex.map(x => BoundReference(x._2, x._1, true)))
  }

  testBothCodegenAndInterpreted("fixed-length types") {
    val inputRow = InternalRow.fromSeq(Seq(true, 3.toByte, 15.toShort, -83, 129L, 1.0f, 5.0, 1, 2L))
    val proj = createMutableProjection(fixedLengthTypes)
    assert(proj(inputRow) === inputRow)
  }

  testBothCodegenAndInterpreted("unsafe buffer") {
    val inputRow = InternalRow.fromSeq(Seq(false, 1.toByte, 9.toShort, -18, 53L, 3.2f, 7.8, 4, 9L))
    val numFields = fixedLengthTypes.length
    val numBytes = Platform.BYTE_ARRAY_OFFSET + UnsafeRow.calculateBitSetWidthInBytes(numFields) +
      UnsafeRow.WORD_SIZE * numFields
    val unsafeBuffer = UnsafeRow.createFromByteArray(numBytes, numFields)
    val proj = createMutableProjection(fixedLengthTypes)
    val projUnsafeRow = proj.target(unsafeBuffer)(inputRow)
    assert(SafeProjection.create(fixedLengthTypes)(projUnsafeRow) === inputRow)
  }

  testBothCodegenAndInterpreted("variable-length types") {
    val proj = createMutableProjection(variableLengthTypes)
    val scalaValues = Seq("abc", BigDecimal(10),
      IntervalUtils.stringToInterval(UTF8String.fromString("interval 1 day")),
      Array[Byte](1, 2), Array("123", "456"), Map(1 -> "a", 2 -> "b"), Row(1, "a"),
      Integer.valueOf(5))
    val inputRow = InternalRow.fromSeq(scalaValues.zip(variableLengthTypes).map {
      case (v, dataType) => CatalystTypeConverters.createToCatalystConverter(dataType)(v)
    })
    val projRow = proj(inputRow)
    variableLengthTypes.zipWithIndex.foreach { case (dataType, index) =>
      val toScala = CatalystTypeConverters.createToScalaConverter(dataType)
      assert(toScala(projRow.get(index, dataType)) === toScala(inputRow.get(index, dataType)))
    }
  }

  test("unsupported types for unsafe buffer") {
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString) {
      val proj = createMutableProjection(Array(StringType))
      val errMsg = intercept[IllegalArgumentException] {
        proj.target(new UnsafeRow(1))
      }.getMessage
      assert(errMsg.contains("MutableProjection cannot use UnsafeRow for output data types:"))
    }
  }

  test("SPARK-33473: subexpression elimination for interpreted MutableProjection") {
    Seq("true", "false").foreach { enabled =>
      withSQLConf(
        SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key -> enabled,
        SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString) {
        val one = BoundReference(0, DoubleType, true)
        val two = BoundReference(1, DoubleType, true)

        val mul = Multiply(one, two)
        val mul2 = Multiply(mul, mul)
        val sqrt = Sqrt(mul2)
        val sum = Add(mul2, sqrt)

        val proj = MutableProjection.create(Seq(sum))
        val result = (d1: Double, d2: Double) =>
          ((d1 * d2) * (d1 * d2)) + Math.sqrt((d1 * d2) * (d1 * d2))

        val inputRows = Seq(
          InternalRow.fromSeq(Seq(1.0, 2.0)),
          InternalRow.fromSeq(Seq(2.0, 3.0)),
          InternalRow.fromSeq(Seq(1.0, null)),
          InternalRow.fromSeq(Seq(null, 2.0)),
          InternalRow.fromSeq(Seq(3.0, 4.0)),
          InternalRow.fromSeq(Seq(null, null))
        )
        val expectedResults = Seq(
          result(1.0, 2.0),
          result(2.0, 3.0),
          null,
          null,
          result(3.0, 4.0),
          null
        )

        inputRows.zip(expectedResults).foreach { case (inputRow, expected) =>
          val projRow = proj.apply(inputRow)
          if (expected != null) {
            assert(projRow.getDouble(0) == expected)
          } else {
            assert(projRow.isNullAt(0))
          }
        }
      }
    }
  }
}
