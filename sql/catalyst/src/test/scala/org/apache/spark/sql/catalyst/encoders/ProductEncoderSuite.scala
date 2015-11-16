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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.{OptionalData, PrimitiveData}

case class RepeatedStruct(s: Seq[PrimitiveData])

case class NestedArray(a: Array[Array[Int]]) {
  override def equals(other: Any): Boolean = other match {
    case NestedArray(otherArray) =>
      java.util.Arrays.deepEquals(
        a.asInstanceOf[Array[AnyRef]],
        otherArray.asInstanceOf[Array[AnyRef]])
    case _ => false
  }
}

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

class ProductEncoderSuite extends ExpressionEncoderSuite {

  productTest(PrimitiveData(1, 1, 1, 1, 1, 1, true))

  productTest(
    OptionalData(Some(2), Some(2), Some(2), Some(2), Some(2), Some(2), Some(true),
      Some(PrimitiveData(1, 1, 1, 1, 1, 1, true))))

  productTest(OptionalData(None, None, None, None, None, None, None, None))

  productTest(BoxedData(1, 1L, 1.0, 1.0f, 1.toShort, 1.toByte, true))

  productTest(BoxedData(null, null, null, null, null, null, null))

  productTest(RepeatedStruct(PrimitiveData(1, 1, 1, 1, 1, 1, true) :: Nil))

  productTest((1, "test", PrimitiveData(1, 1, 1, 1, 1, 1, true)))

  productTest(
    RepeatedData(
      Seq(1, 2),
      Seq(new Integer(1), null, new Integer(2)),
      Map(1 -> 2L),
      Map(1 -> null),
      PrimitiveData(1, 1, 1, 1, 1, 1, true)))

  productTest(NestedArray(Array(Array(1, -2, 3), null, Array(4, 5, -6))))

  productTest(("Seq[(String, String)]",
    Seq(("a", "b"))))
  productTest(("Seq[(Int, Int)]",
    Seq((1, 2))))
  productTest(("Seq[(Long, Long)]",
    Seq((1L, 2L))))
  productTest(("Seq[(Float, Float)]",
    Seq((1.toFloat, 2.toFloat))))
  productTest(("Seq[(Double, Double)]",
    Seq((1.toDouble, 2.toDouble))))
  productTest(("Seq[(Short, Short)]",
    Seq((1.toShort, 2.toShort))))
  productTest(("Seq[(Byte, Byte)]",
    Seq((1.toByte, 2.toByte))))
  productTest(("Seq[(Boolean, Boolean)]",
    Seq((true, false))))

  productTest(("ArrayBuffer[(String, String)]",
    ArrayBuffer(("a", "b"))))
  productTest(("ArrayBuffer[(Int, Int)]",
    ArrayBuffer((1, 2))))
  productTest(("ArrayBuffer[(Long, Long)]",
    ArrayBuffer((1L, 2L))))
  productTest(("ArrayBuffer[(Float, Float)]",
    ArrayBuffer((1.toFloat, 2.toFloat))))
  productTest(("ArrayBuffer[(Double, Double)]",
    ArrayBuffer((1.toDouble, 2.toDouble))))
  productTest(("ArrayBuffer[(Short, Short)]",
    ArrayBuffer((1.toShort, 2.toShort))))
  productTest(("ArrayBuffer[(Byte, Byte)]",
    ArrayBuffer((1.toByte, 2.toByte))))
  productTest(("ArrayBuffer[(Boolean, Boolean)]",
    ArrayBuffer((true, false))))

  productTest(("Seq[Seq[(Int, Int)]]",
    Seq(Seq((1, 2)))))

  encodeDecodeTest(
    1 -> 10L,
    ExpressionEncoder.tuple(FlatEncoder[Int], FlatEncoder[Long]),
    "tuple with 2 flat encoders")

  encodeDecodeTest(
    (PrimitiveData(1, 1, 1, 1, 1, 1, true), (3, 30L)),
    ExpressionEncoder.tuple(ProductEncoder[PrimitiveData], ProductEncoder[(Int, Long)]),
    "tuple with 2 product encoders")

  encodeDecodeTest(
    (PrimitiveData(1, 1, 1, 1, 1, 1, true), 3),
    ExpressionEncoder.tuple(ProductEncoder[PrimitiveData], FlatEncoder[Int]),
    "tuple with flat encoder and product encoder")

  encodeDecodeTest(
    (3, PrimitiveData(1, 1, 1, 1, 1, 1, true)),
    ExpressionEncoder.tuple(FlatEncoder[Int], ProductEncoder[PrimitiveData]),
    "tuple with product encoder and flat encoder")

  encodeDecodeTest(
    (1, (10, 100L)),
    {
      val intEnc = FlatEncoder[Int]
      val longEnc = FlatEncoder[Long]
      ExpressionEncoder.tuple(intEnc, ExpressionEncoder.tuple(intEnc, longEnc))
    },
    "nested tuple encoder")

  private def productTest[T <: Product : TypeTag](input: T): Unit = {
    encodeDecodeTest(input, ProductEncoder[T], input.getClass.getSimpleName)
  }
}
