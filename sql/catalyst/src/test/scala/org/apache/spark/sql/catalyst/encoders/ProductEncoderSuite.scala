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

package org.apache.spark.sql.catalyst.encoders

import java.util

import scala.reflect.runtime.universe._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst._

case class RepeatedStruct(s: Seq[PrimitiveData])

case class NestedArray(a: Array[Array[Int]])

case class BoxedData(
    intField: java.lang.Integer,
    longField: java.lang.Long,
    doubleField: java.lang.Double,
    floatField: java.lang.Float,
    shortField: java.lang.Short,
    byteField: java.lang.Byte,
    booleanField: java.lang.Boolean)

class ProductEncoderSuite extends SparkFunSuite {

  encodeDecodeTest(PrimitiveData(1, 1, 1, 1, 1, 1, true))

  encodeDecodeTest(
    OptionalData(
      Some(2), Some(2), Some(2), Some(2), Some(2), Some(2), Some(true),
      Some(PrimitiveData(1, 1, 1, 1, 1, 1, true))))

  encodeDecodeTest(OptionalData(None, None, None, None, None, None, None, None))

  encodeDecodeTest(
    BoxedData(1, 1L, 1.0, 1.0f, 1.toShort, 1.toByte, true))

  encodeDecodeTest(
    BoxedData(null, null, null, null, null, null, null))

  encodeDecodeTest(("Array[Byte] null", null: Array[Byte]))
  encodeDecodeTestCustom(("Array[Byte]", Array[Byte](1, 2, 3)))
    { (l, r) => util.Arrays.equals(l._2, r._2) }

  encodeDecodeTest(("Array[Int] null", null: Array[Int]))
  encodeDecodeTestCustom(("Array[Int]", Array[Int](1, 2, 3)))
    { (l, r) => util.Arrays.equals(l._2, r._2) }

  encodeDecodeTest(("Array[Long] null", null: Array[Long]))
  encodeDecodeTestCustom(("Array[Long]", Array[Long](1, 2, 3)))
    { (l, r) => util.Arrays.equals(l._2, r._2) }

  encodeDecodeTestCustom(("java.sql.Timestamp", new java.sql.Timestamp(1)))
    { (l, r) => l._2.toString == r._2.toString }

  encodeDecodeTestCustom(("java.sql.Date", new java.sql.Date(1)))
    { (l, r) => l._2.toString == r._2.toString }

  /** Simplified encodeDecodeTestCustom, where the comparison function can be `Object.equals`.*/
  protected def encodeDecodeTest[T <: Product : TypeTag](inputData: T) =
    encodeDecodeTestCustom[T](inputData)((l, r) => l == r)

  /**
   * Constructs a test that round-trips `t` through an encoder, checking the results to ensure it
   * matches the original.
   */
  protected def encodeDecodeTestCustom[T <: Product : TypeTag](
      inputData: T)(
      c: (T, T) => Boolean) = {
    test(s"encode/decode: $inputData") {
      val encoder = try ProductEncoder[T] catch {
        case e: Exception =>
          fail(s"Exception thrown generating encoder", e)
      }
      val convertedData = encoder.toRow(inputData)
      val schema = encoder.schema.toAttributes
      val boundEncoder = encoder.bind(schema)
      val convertedBack = try boundEncoder.fromRow(convertedData) catch {
        case e: Exception =>
          fail(
           s"""Exception thrown while decoding
              |Converted: $convertedData
              |Schema: ${schema.mkString(",")}
              |${encoder.schema.treeString}
              |Construct Expressions:
              |${boundEncoder.constructExpression.treeString}
              |
            """.stripMargin, e)
      }

      if (!c(inputData,convertedBack)) {
        fail(
          s"""Encoded/Decoded data does not match input data
             |
             |in:  $inputData
             |out: $convertedBack
             |
             |Converted: ${convertedData.toSeq(encoder.schema).mkString("[", ",", "]")}
             |Schema: ${schema.mkString(",")}
             |${encoder.schema.treeString}
             |Construct Expressions:
             |${boundEncoder.constructExpression.treeString}
             |
           """.stripMargin)
      }
    }
  }

//  test("convert PrimitiveData to InternalRow") {
//    val inputData = PrimitiveData(1, 1, 1, 1, 1, 1, true)
//    val encoder = ProductEncoder[PrimitiveData]
//    val convertedData = encoder.toRow(inputData)
//
//    assert(convertedData.getInt(0) == 1)
//    assert(convertedData.getLong(1) == 1.toLong)
//    assert(convertedData.getDouble(2) == 1.toDouble)
//    assert(convertedData.getFloat(3) == 1.toFloat)
//    assert(convertedData.getShort(4) == 1.toShort)
//    assert(convertedData.getByte(5) == 1.toByte)
//    assert(convertedData.getBoolean(6) == true)
//
//    val schema = encoder.schema.toAttributes
//    val boundEncoder = encoder.bind(schema)
//    val convertedBack = boundEncoder.fromRow(convertedData)
//    assert(inputData == convertedBack)
//  }
//
//  test("convert Some[_] to InternalRow") {
//    val primitiveData = PrimitiveData(1, 1, 1, 1, 1, 1, true)
//    val inputData = OptionalData(Some(2), Some(2), Some(2), Some(2), Some(2), Some(2), Some(true),
//      Some(primitiveData))
//
//    val encoder = ProductEncoder[OptionalData]
//    val convertedData = encoder.toRow(inputData)
//
//    assert(convertedData.getInt(0) == 2)
//    assert(convertedData.getLong(1) == 2.toLong)
//    assert(convertedData.getDouble(2) == 2.toDouble)
//    assert(convertedData.getFloat(3) == 2.toFloat)
//    assert(convertedData.getShort(4) == 2.toShort)
//    assert(convertedData.getByte(5) == 2.toByte)
//    assert(convertedData.getBoolean(6) == true)
//
//    val nestedRow = convertedData.getStruct(7, 7)
//    assert(nestedRow.getInt(0) == 1)
//    assert(nestedRow.getLong(1) == 1.toLong)
//    assert(nestedRow.getDouble(2) == 1.toDouble)
//    assert(nestedRow.getFloat(3) == 1.toFloat)
//    assert(nestedRow.getShort(4) == 1.toShort)
//    assert(nestedRow.getByte(5) == 1.toByte)
//    assert(nestedRow.getBoolean(6) == true)
//  }
//
//  test("convert None to InternalRow") {
//    val inputData = OptionalData(None, None, None, None, None, None, None, None)
//    val encoder = ProductEncoder[OptionalData]
//    val convertedData = encoder.toRow(inputData)
//
//    assert(convertedData.isNullAt(0))
//    assert(convertedData.isNullAt(1))
//    assert(convertedData.isNullAt(2))
//    assert(convertedData.isNullAt(3))
//    assert(convertedData.isNullAt(4))
//    assert(convertedData.isNullAt(5))
//    assert(convertedData.isNullAt(6))
//    assert(convertedData.isNullAt(7))
//  }
//
//  test("convert nullable but present data to InternalRow") {
//    val inputData = NullableData(
//      1, 1L, 1.0, 1.0f, 1.toShort, 1.toByte, true, "test", new java.math.BigDecimal(1), new Date(0),
//      new Timestamp(0), Array[Byte](1, 2, 3))
//
//    val encoder = ProductEncoder[NullableData]
//    val convertedData = encoder.toRow(inputData)
//
//    assert(convertedData.getInt(0) == 1)
//    assert(convertedData.getLong(1) == 1.toLong)
//    assert(convertedData.getDouble(2) == 1.toDouble)
//    assert(convertedData.getFloat(3) == 1.toFloat)
//    assert(convertedData.getShort(4) == 1.toShort)
//    assert(convertedData.getByte(5) == 1.toByte)
//    assert(convertedData.getBoolean(6) == true)
//
//    assert(!convertedData.isNullAt(9))
//  }
//
//  test("convert nullable data to InternalRow") {
//    val inputData =
//      NullableData(null, null, null, null, null, null, null, null, null, null, null, null)
//
//    val encoder = ProductEncoder[NullableData]
//    val convertedData = encoder.toRow(inputData)
//
//    assert(convertedData.isNullAt(0))
//    assert(convertedData.isNullAt(1))
//    assert(convertedData.isNullAt(2))
//    assert(convertedData.isNullAt(3))
//    assert(convertedData.isNullAt(4))
//    assert(convertedData.isNullAt(5))
//    assert(convertedData.isNullAt(6))
//    assert(convertedData.isNullAt(7))
//    assert(convertedData.isNullAt(8))
//    assert(convertedData.isNullAt(9))
//    assert(convertedData.isNullAt(10))
//    assert(convertedData.isNullAt(11))
//  }
//
//  test("convert repeated struct") {
//    val inputData = RepeatedStruct(PrimitiveData(1, 1, 1, 1, 1, 1, true) :: Nil)
//    val encoder = ProductEncoder[RepeatedStruct]
//
//    val converted = encoder.toRow(inputData)
//    val convertedStruct = converted.getArray(0).getStruct(0, 7)
//    assert(convertedStruct.getInt(0) == 1)
//    assert(convertedStruct.getLong(1) == 1.toLong)
//    assert(convertedStruct.getDouble(2) == 1.toDouble)
//    assert(convertedStruct.getFloat(3) == 1.toFloat)
//    assert(convertedStruct.getShort(4) == 1.toShort)
//    assert(convertedStruct.getByte(5) == 1.toByte)
//    assert(convertedStruct.getBoolean(6) == true)
//  }
//
//  test("convert nested seq") {
//    val convertedData = ProductEncoder[Tuple1[Seq[Seq[Int]]]].toRow(Tuple1(Seq(Seq(1))))
//    assert(convertedData.getArray(0).getArray(0).getInt(0) == 1)
//
//    val convertedData2 = ProductEncoder[Tuple1[Seq[Seq[Seq[Int]]]]].toRow(Tuple1(Seq(Seq(Seq(1)))))
//    assert(convertedData2.getArray(0).getArray(0).getArray(0).getInt(0) == 1)
//  }
//
//  test("convert nested array") {
//    val convertedData = ProductEncoder[Tuple1[Array[Array[Int]]]].toRow(Tuple1(Array(Array(1))))
//  }
//
//  test("convert complex") {
//    val inputData = ComplexData(
//      Seq(1, 2),
//      Array(1, 2),
//      1 :: 2 :: Nil,
//      Seq(new Integer(1), null, new Integer(2)),
//      Map(1 -> 2L),
//      Map(1 -> new java.lang.Long(2)),
//      PrimitiveData(1, 1, 1, 1, 1, 1, true),
//      Array(Array(1)))
//
//    val encoder = ProductEncoder[ComplexData]
//    val convertedData = encoder.toRow(inputData)
//
//    assert(!convertedData.isNullAt(0))
//    val seq = convertedData.getArray(0)
//    assert(seq.numElements() == 2)
//    assert(seq.getInt(0) == 1)
//    assert(seq.getInt(1) == 2)
//  }
}
