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

import org.apache.spark.sql.types.{StructField, ArrayType, ArrayData}

import scala.collection.mutable.ArrayBuffer
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

case class RepeatedData(
    arrayField: Seq[Int],
    arrayFieldContainsNull: Seq[java.lang.Integer],
    mapField: scala.collection.Map[Int, Long],
    mapFieldNull: scala.collection.Map[Int, java.lang.Long],
    structField: PrimitiveData)

case class SpecificCollection(l: List[Int])

class ProductEncoderSuite extends SparkFunSuite {

  encodeDecodeTest(PrimitiveData(1, 1, 1, 1, 1, 1, true))

  // TODO: Support creating specific subclasses of Seq.
  ignore("Specific collection types") { encodeDecodeTest(SpecificCollection(1 :: Nil)) }

  encodeDecodeTest(
    OptionalData(
      Some(2), Some(2), Some(2), Some(2), Some(2), Some(2), Some(true),
      Some(PrimitiveData(1, 1, 1, 1, 1, 1, true))))

  encodeDecodeTest(OptionalData(None, None, None, None, None, None, None, None))

  encodeDecodeTest(
    BoxedData(1, 1L, 1.0, 1.0f, 1.toShort, 1.toByte, true))

  encodeDecodeTest(
    BoxedData(null, null, null, null, null, null, null))

  encodeDecodeTest(
    RepeatedStruct(PrimitiveData(1, 1, 1, 1, 1, 1, true) :: Nil))

  encodeDecodeTest(
    RepeatedData(
      Seq(1, 2),
      Seq(new Integer(1), null, new Integer(2)),
      Map(1 -> 2L),
      Map(1 -> null),
      PrimitiveData(1, 1, 1, 1, 1, 1, true)))

  encodeDecodeTest(("nullable Seq[Integer]", Seq[Integer](1, null)))

  encodeDecodeTest(("Seq[(String, String)]",
    Seq(("a", "b"))))
  encodeDecodeTest(("Seq[(Int, Int)]",
    Seq((1, 2))))
  encodeDecodeTest(("Seq[(Long, Long)]",
    Seq((1L, 2L))))
  encodeDecodeTest(("Seq[(Float, Float)]",
    Seq((1.toFloat, 2.toFloat))))
  encodeDecodeTest(("Seq[(Double, Double)]",
    Seq((1.toDouble, 2.toDouble))))
  encodeDecodeTest(("Seq[(Short, Short)]",
    Seq((1.toShort, 2.toShort))))
  encodeDecodeTest(("Seq[(Byte, Byte)]",
    Seq((1.toByte, 2.toByte))))
  encodeDecodeTest(("Seq[(Boolean, Boolean)]",
    Seq((true, false))))

  // TODO: Decoding/encoding of complex maps.
  ignore("complex maps") {
    encodeDecodeTest(("Map[Int, (String, String)]",
      Map(1 ->("a", "b"))))
  }

  encodeDecodeTest(("ArrayBuffer[(String, String)]",
    ArrayBuffer(("a", "b"))))
  encodeDecodeTest(("ArrayBuffer[(Int, Int)]",
    ArrayBuffer((1, 2))))
  encodeDecodeTest(("ArrayBuffer[(Long, Long)]",
    ArrayBuffer((1L, 2L))))
  encodeDecodeTest(("ArrayBuffer[(Float, Float)]",
    ArrayBuffer((1.toFloat, 2.toFloat))))
  encodeDecodeTest(("ArrayBuffer[(Double, Double)]",
    ArrayBuffer((1.toDouble, 2.toDouble))))
  encodeDecodeTest(("ArrayBuffer[(Short, Short)]",
    ArrayBuffer((1.toShort, 2.toShort))))
  encodeDecodeTest(("ArrayBuffer[(Byte, Byte)]",
    ArrayBuffer((1.toByte, 2.toByte))))
  encodeDecodeTest(("ArrayBuffer[(Boolean, Boolean)]",
    ArrayBuffer((true, false))))

  encodeDecodeTest(("Seq[Seq[(Int, Int)]]",
    Seq(Seq((1, 2)))))

  encodeDecodeTestCustom(("Array[Array[(Int, Int)]]",
    Array(Array((1, 2)))))
  { (l, r) => l._2(0)(0) == r._2(0)(0) }

  encodeDecodeTestCustom(("Array[Array[(Int, Int)]]",
    Array(Array(Array((1, 2))))))
  { (l, r) => l._2(0)(0)(0) == r._2(0)(0)(0) }

  encodeDecodeTestCustom(("Array[Array[Array[(Int, Int)]]]",
    Array(Array(Array(Array((1, 2)))))))
  { (l, r) => l._2(0)(0)(0)(0) == r._2(0)(0)(0)(0) }

  encodeDecodeTestCustom(("Array[Array[Array[Array[(Int, Int)]]]]",
    Array(Array(Array(Array(Array((1, 2))))))))
  { (l, r) => l._2(0)(0)(0)(0)(0) == r._2(0)(0)(0)(0)(0) }


  encodeDecodeTestCustom(("Array[Array[Integer]]",
    Array(Array[Integer](1))))
  { (l, r) => l._2(0)(0) == r._2(0)(0) }

  encodeDecodeTestCustom(("Array[Array[Int]]",
    Array(Array(1))))
  { (l, r) => l._2(0)(0) == r._2(0)(0) }

  encodeDecodeTestCustom(("Array[Array[Int]]",
    Array(Array(Array(1)))))
  { (l, r) => l._2(0)(0)(0) == r._2(0)(0)(0) }

  encodeDecodeTestCustom(("Array[Array[Array[Int]]]",
    Array(Array(Array(Array(1))))))
  { (l, r) => l._2(0)(0)(0)(0) == r._2(0)(0)(0)(0) }

  encodeDecodeTestCustom(("Array[Array[Array[Array[Int]]]]",
    Array(Array(Array(Array(Array(1)))))))
  { (l, r) => l._2(0)(0)(0)(0)(0) == r._2(0)(0)(0)(0)(0) }

  encodeDecodeTest(("Array[Byte] null",
    null: Array[Byte]))
  encodeDecodeTestCustom(("Array[Byte]",
    Array[Byte](1, 2, 3)))
    { (l, r) => util.Arrays.equals(l._2, r._2) }

  encodeDecodeTest(("Array[Int] null",
    null: Array[Int]))
  encodeDecodeTestCustom(("Array[Int]",
    Array[Int](1, 2, 3)))
    { (l, r) => util.Arrays.equals(l._2, r._2) }

  encodeDecodeTest(("Array[Long] null",
    null: Array[Long]))
  encodeDecodeTestCustom(("Array[Long]",
    Array[Long](1, 2, 3)))
    { (l, r) => util.Arrays.equals(l._2, r._2) }

  encodeDecodeTest(("Array[Double] null",
    null: Array[Double]))
  encodeDecodeTestCustom(("Array[Double]",
    Array[Double](1, 2, 3)))
    { (l, r) => util.Arrays.equals(l._2, r._2) }

  encodeDecodeTest(("Array[Float] null",
    null: Array[Float]))
  encodeDecodeTestCustom(("Array[Float]",
    Array[Float](1, 2, 3)))
    { (l, r) => util.Arrays.equals(l._2, r._2) }

  encodeDecodeTest(("Array[Boolean] null",
    null: Array[Boolean]))
  encodeDecodeTestCustom(("Array[Boolean]",
    Array[Boolean](true, false)))
    { (l, r) => util.Arrays.equals(l._2, r._2) }

  encodeDecodeTest(("Array[Short] null",
    null: Array[Short]))
  encodeDecodeTestCustom(("Array[Short]",
    Array[Short](1, 2, 3)))
    { (l, r) => util.Arrays.equals(l._2, r._2) }

  encodeDecodeTestCustom(("java.sql.Timestamp",
    new java.sql.Timestamp(1)))
    { (l, r) => l._2.toString == r._2.toString }

  encodeDecodeTestCustom(("java.sql.Date", new java.sql.Date(1)))
    { (l, r) => l._2.toString == r._2.toString }

  /** Simplified encodeDecodeTestCustom, where the comparison function can be `Object.equals`. */
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
              |
              |Construct Expressions:
              |${boundEncoder.constructExpression.treeString}
              |
            """.stripMargin, e)
      }

      if (!c(inputData, convertedBack)) {
        val types =
          convertedBack.productIterator.filter(_ != null).map(_.getClass.getName).mkString(",")

        val encodedData = convertedData.toSeq(encoder.schema).zip(encoder.schema).map {
          case (a: ArrayData, StructField(_, at: ArrayType, _, _)) =>
            a.toArray[Any](at.elementType).toSeq
          case (other, _) =>
            other
        }.mkString("[", ",", "]")

        fail(
          s"""Encoded/Decoded data does not match input data
             |
             |in:  $inputData
             |out: $convertedBack
             |types: $types
             |
             |Encoded Data: $encodedData
             |Schema: ${schema.mkString(",")}
             |${encoder.schema.treeString}
             |
             |Extract Expressions:
             |${boundEncoder.extractExpressions.map(_.treeString).mkString("\n")}
             |
             |Construct Expressions:
             |${boundEncoder.constructExpression.treeString}
             |
           """.stripMargin)
      }
    }
  }
}
